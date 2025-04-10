from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, admin
import json
import threading
import time
import logging
from typing import Dict, List, Optional, Any, Callable
from kafka_config import KAFKA_CONFIG, task_cache, notification_cache, task_cache_lock, notification_cache_lock

logger = logging.getLogger('consumer_service')

# Consumer class for better management
class KafkaConsumerService:
    def __init__(self, topic: str, group_id: str, cache: Dict, cache_lock: threading.Lock):
        self.topic = topic
        self.group_id = group_id
        self.cache = cache
        self.cache_lock = cache_lock
        self.running = False
        self.consumer = None
        self.consumer_thread = None
        self.stats_thread = None
        
        # Statistics tracking
        self.stats_lock = threading.Lock()
        self.message_count = 0
        self.unread_count = 0
        self.last_stats_update = 0
        self.stats_update_interval = 60  # Update stats every 60 seconds
        
        self.initialize_consumer()
        
    def initialize_consumer(self):
        """Initialize the Kafka consumer with proper configuration"""
        consumer_config = KAFKA_CONFIG.copy()
        consumer_config.update({
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,  # Commit every 5 seconds
            'session.timeout.ms': 30000,      # 30 seconds session timeout
            'max.poll.interval.ms': 300000,   # 5 minutes poll interval
        })
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([self.topic])
        
    def start_consuming(self):
        """Start consuming messages in a separate thread"""
        if self.running:
            return
            
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_loop)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
        # Start stats thread
        self.stats_thread = threading.Thread(target=self._update_stats_loop)
        self.stats_thread.daemon = True
        self.stats_thread.start()
        
        logger.info(f"Started consuming from topic {self.topic}")
        
    def _consume_loop(self):
        """Main consumption loop that runs in a separate thread"""
        try:
            while self.running:
                msg = self.consumer.poll(1.0)  # Poll with a 1-second timeout
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Check for tombstone message (null value)
                if msg.value() is None:
                    # This is a tombstone message, remove the corresponding message from cache
                    if msg.key() is not None:
                        key_str = msg.key().decode('utf-8')
                        self.remove_message(key_str)
                        logger.debug(f"Processed tombstone message for key {key_str}")
                    continue
                
                try:
                    # Parse the message
                    value = json.loads(msg.value().decode('utf-8'))
                    
                    # Get the message ID (task_id or notification_id)
                    msg_id = value.get('task_id') if self.topic == 'tasks' else value.get('id')
                    
                    if msg_id:
                        # Update the cache with the new message
                        with self.cache_lock:
                            # Check if this is a new message or an update
                            is_new = str(msg_id) not in self.cache
                            
                            # If it's a notification, check read status
                            if self.topic == 'notifications':
                                old_read_status = self.cache.get(str(msg_id), {}).get('read', False)
                                new_read_status = value.get('read', False)
                                
                                # Update unread count
                                with self.stats_lock:
                                    if is_new and not new_read_status:
                                        self.unread_count += 1
                                    elif not is_new and old_read_status != new_read_status:
                                        if new_read_status:  # Changed from unread to read
                                            self.unread_count = max(0, self.unread_count - 1)
                                        else:  # Changed from read to unread
                                            self.unread_count += 1
                            
                            # Update the cache
                            self.cache[str(msg_id)] = value
                            
                            # Update message count for new messages
                            if is_new:
                                with self.stats_lock:
                                    self.message_count += 1
                            
                        logger.debug(f"Cached message with ID {msg_id} from topic {self.topic}")
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode message: {msg.value()}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
        finally:
            self.consumer.close()
            logger.info(f"Stopped consuming from topic {self.topic}")
    
    def _update_stats_loop(self):
        """Periodically update statistics using Kafka admin API"""
        try:
            # Create admin client for getting topic metadata
            admin_client = admin.AdminClient(KAFKA_CONFIG)
            
            while self.running:
                current_time = time.time()
                
                # Only update stats periodically
                if current_time - self.last_stats_update >= self.stats_update_interval:
                    try:
                        # Get topic metadata
                        topic_metadata = admin_client.list_topics(topic=self.topic)
                        
                        if self.topic in topic_metadata.topics:
                            # Get partition information
                            partitions = topic_metadata.topics[self.topic].partitions
                            
                            # Get consumer group offsets
                            consumer_offsets = {}
                            try:
                                # Create a temporary consumer to get committed offsets
                                temp_config = KAFKA_CONFIG.copy()
                                temp_config['group.id'] = self.group_id
                                temp_consumer = Consumer(temp_config)
                                
                                for partition_id in partitions:
                                    tp = TopicPartition(self.topic, partition_id)
                                    committed = temp_consumer.committed([tp])[0]
                                    if committed is not None:
                                        consumer_offsets[partition_id] = committed.offset
                                    else:
                                        consumer_offsets[partition_id] = 0
                                
                                temp_consumer.close()
                            except Exception as e:
                                logger.error(f"Error getting consumer offsets: {e}")
                            
                            # Calculate total message count
                            total_messages = 0
                            for partition_id, partition_info in partitions.items():
                                # Get the end offset (high watermark)
                                high_watermark = partition_info.high_watermark
                                
                                # Get the consumer offset
                                consumer_offset = consumer_offsets.get(partition_id, 0)
                                
                                # Calculate messages in this partition
                                partition_messages = max(0, high_watermark - consumer_offset)
                                total_messages += partition_messages
                            
                            # Update message count
                            with self.stats_lock:
                                # For notifications, we don't need to track read/unread separately
                                # since all notifications in the queue are unread
                                
                                # Update message count based on Kafka metadata
                                self.message_count = total_messages
                            
                            logger.info(f"Updated stats for {self.topic}: {total_messages} messages")
                    except Exception as e:
                        logger.error(f"Error updating stats: {e}")
                    
                    self.last_stats_update = current_time
                
                # Sleep for a while before checking again
                time.sleep(10)
                
        except Exception as e:
            logger.error(f"Error in stats loop: {e}")
    
    def stop_consuming(self):
        """Stop the consumer thread"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=10)
        if self.stats_thread:
            self.stats_thread.join(timeout=10)
        logger.info(f"Consumer for topic {self.topic} stopped")
        
    def get_message_by_id(self, msg_id: str) -> Optional[Dict]:
        """Get a message from the cache by ID"""
        with self.cache_lock:
            return self.cache.get(str(msg_id))
            
    def get_all_messages(self) -> List[Dict]:
        """Get all messages from the cache"""
        with self.cache_lock:
            return list(self.cache.values())
    
    def get_message_count(self) -> int:
        """Get the current message count without reading all messages"""
        with self.stats_lock:
            return self.message_count
    
    def get_unread_count(self) -> int:
        """Get the current unread count for notifications"""
        with self.stats_lock:
            return self.unread_count
            
    def update_message(self, msg_id: str, updated_data: Dict) -> None:
        """Update a message in the cache"""
        with self.cache_lock:
            if str(msg_id) in self.cache:
                # For notifications, track read status changes
                if self.topic == 'notifications' and 'read' in updated_data:
                    old_read_status = self.cache[str(msg_id)].get('read', False)
                    new_read_status = updated_data['read']
                    
                    if old_read_status != new_read_status:
                        with self.stats_lock:
                            if new_read_status:  # Changed from unread to read
                                self.unread_count = max(0, self.unread_count - 1)
                            else:  # Changed from read to unread
                                self.unread_count += 1
                
                # Update the cache
                self.cache[str(msg_id)].update(updated_data)

    def remove_message(self, msg_id: str) -> bool:
        """
        Remove a message from the cache
        """
        with self.cache_lock:
            if str(msg_id) in self.cache:
                # Remove from cache
                del self.cache[str(msg_id)]
                
                # Update message count
                with self.stats_lock:
                    self.message_count = max(0, self.message_count - 1)
                    
                logger.debug(f"Removed message with ID {msg_id} from {self.topic} cache")
                return True
            return False

# Create consumer services
task_consumer_service = KafkaConsumerService('tasks', 'task_consumer_group', task_cache, task_cache_lock)
notification_consumer_service = KafkaConsumerService('notifications', 'notification_consumer_group', notification_cache, notification_cache_lock)

# Start consumers
def start_consumers():
    task_consumer_service.start_consuming()
    notification_consumer_service.start_consuming()

# Stop consumers
def stop_consumers():
    task_consumer_service.stop_consuming()
    notification_consumer_service.stop_consuming() 