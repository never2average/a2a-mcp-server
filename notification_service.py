from typing import Dict, List, Optional, Any
import logging
from kafka_config import produce_message, flush_producer, delete_message
from consumer_service import notification_consumer_service
import uuid
import time

logger = logging.getLogger('notification_service')

def send_task_notification(task_id: int, priority: int, notes: str, signal_id: str, data: Dict[str, Any]) -> bool:
    """
    Send a notification related to a task
    """
    notification_id = str(uuid.uuid4())
    notification = {
        'id': notification_id,
        'sender': 'system',
        'sender_identity': 'AI agent',
        'priority': priority,
        'data': data,
        'notes_to_LLM': notes,
        'related_to': str(task_id),
        'signal_id': signal_id,
        'timestamp': int(time.time())
    }
    
    # Send notification to Kafka
    success = produce_message('notifications', notification_id, notification)
    
    # Ensure message is delivered
    flush_producer()
    
    return success

def check_notification_count() -> int:
    """
    Count the number of unread notifications efficiently
    """
    # Since all notifications in the queue are unread, just return the total count
    return notification_consumer_service.get_message_count()

def get_notification_list(priority: Optional[int] = None, sender: Optional[str] = None, 
                          page_no: int = 1, page_size: int = 10, 
                          related_to: Optional[str] = None, 
                          task_id: Optional[int] = None) -> List[Dict]:
    """
    Get a list of notifications with optional filtering
    """
    # Get all notifications from the cache (all are unread)
    notifications = notification_consumer_service.get_all_messages()
    
    # Apply filters
    filtered_notifications = notifications
    
    if priority is not None:
        filtered_notifications = [n for n in filtered_notifications if n.get('priority') == priority]
    
    if sender is not None:
        filtered_notifications = [n for n in filtered_notifications if n.get('sender') == sender]
    
    if related_to is not None:
        filtered_notifications = [n for n in filtered_notifications if n.get('related_to') == related_to]
    
    if task_id is not None:
        filtered_notifications = [n for n in filtered_notifications if n.get('related_to') == str(task_id)]
    
    # Sort by timestamp (newest first)
    sorted_notifications = sorted(filtered_notifications, key=lambda x: x.get('timestamp', 0), reverse=True)
    
    # Apply pagination
    start_idx = (page_no - 1) * page_size
    end_idx = start_idx + page_size
    paginated_notifications = sorted_notifications[start_idx:end_idx] if start_idx < len(sorted_notifications) else []
    
    return paginated_notifications

def mark_notification_as_read(notification_id: int) -> bool:
    """
    Mark a notification as read by removing it from the queue
    """
    # Get the notification from the cache
    notification = notification_consumer_service.get_message_by_id(str(notification_id))
    
    if not notification:
        logger.warning(f"Notification {notification_id} not found")
        return False
    
    # Remove the notification from the cache
    notification_consumer_service.remove_message(str(notification_id))
    
    # Delete the message from Kafka (tombstone message)
    success = delete_message('notifications', str(notification_id))
    
    # Ensure message is delivered
    flush_producer()
    
    logger.info(f"Notification {notification_id} marked as read and removed from queue")
    return success 