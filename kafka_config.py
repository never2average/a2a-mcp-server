# Kafka configuration for AWS MSK
from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition
import json
import time
from typing import Dict, List, Callable, Optional, Any
import threading
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('kafka_service')

# Kafka configuration for AWS MSK
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092', #Replace with your real MSK bootstrap servers
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'test',  # Replace with your real MSK username
    'sasl.password': 'test',  # Replace with your real MSK password
}

# Initialize Kafka producer with better configuration
producer_config = KAFKA_CONFIG.copy()
producer_config.update({
    'acks': 'all',                  # Wait for all replicas to acknowledge
    'retries': 5,                   # Retry a few times before giving up
    'retry.backoff.ms': 500,        # Retry after 500ms
    'delivery.timeout.ms': 10000,   # 10 seconds delivery timeout
})
producer = Producer(producer_config)

# Initialize Kafka consumers for tasks and notifications
def create_consumer(topic: str, group_id: str) -> Consumer:
    consumer_config = KAFKA_CONFIG.copy()
    consumer_config.update({
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    return consumer

# Create consumers
task_consumer = create_consumer('tasks', 'task_consumer_group')
notification_consumer = create_consumer('notifications', 'notification_consumer_group')

# In-memory cache for messages
task_cache = {}
notification_cache = {}

# Lock for thread safety
task_cache_lock = threading.Lock()
notification_cache_lock = threading.Lock()

# Callback for message delivery reports
def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Helper function to produce messages to Kafka with better error handling
def produce_message(topic: str, key: str, message: Dict) -> bool:
    try:
        # Using a key for partitioning (ensures related messages go to same partition)
        producer.produce(
            topic=topic,
            key=key.encode('utf-8') if key else None,
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )
        # Using poll in a non-blocking way to handle callbacks
        producer.poll(0)
        return True
    except KafkaException as e:
        logger.error(f"Failed to send message to {topic}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending message to {topic}: {e}")
        return False

# Flush producer to ensure all messages are sent
def flush_producer(timeout=10):
    return producer.flush(timeout)

# Helper function to consume messages from Kafka
def consume_messages(consumer: Consumer, count: int = 10, timeout: float = 1.0) -> List[Dict]:
    from confluent_kafka import KafkaError
    
    messages = []
    start_time = time.time()
    
    while len(messages) < count and (time.time() - start_time) < timeout:
        msg = consumer.poll(timeout=0.1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break
        try:
            messages.append(json.loads(msg.value().decode('utf-8')))
        except json.JSONDecodeError:
            print(f"Failed to decode message: {msg.value()}")
    
    return messages 

def delete_message(topic: str, key: str) -> bool:
    """
    Delete a message from Kafka by sending a tombstone message (null value)
    """
    try:
        # A null value with the same key acts as a tombstone message in Kafka
        producer.produce(
            topic=topic,
            key=key.encode('utf-8') if key else None,
            value=None,  # Null value = tombstone
            callback=delivery_report
        )
        # Using poll in a non-blocking way to handle callbacks
        producer.poll(0)
        logger.info(f"Sent tombstone message for key {key} to topic {topic}")
        return True
    except KafkaException as e:
        logger.error(f"Failed to send tombstone message to {topic}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending tombstone message to {topic}: {e}")
        return False 


# Add a function to update AWS MSK credentials
def update_kafka_credentials(username, password):
    """Update the Kafka credentials for AWS MSK"""
    global KAFKA_CONFIG, producer_config, producer
    
    # Update credentials in the config
    KAFKA_CONFIG['sasl.username'] = username
    KAFKA_CONFIG['sasl.password'] = password
    
    # Update producer config
    producer_config = KAFKA_CONFIG.copy()
    producer_config.update({
        'acks': 'all',
        'retries': 5,
        'retry.backoff.ms': 500,
        'delivery.timeout.ms': 30000,  # Increased timeout for AWS connectivity
    })
    
    # Recreate the producer with updated config
    producer = Producer(producer_config)
    
    # Recreate consumers
    global task_consumer, notification_consumer
    task_consumer = create_consumer('tasks', 'task_consumer_group')
    notification_consumer = create_consumer('notifications', 'notification_consumer_group')
    
    logger.info("Updated AWS MSK Kafka credentials")
    return True 