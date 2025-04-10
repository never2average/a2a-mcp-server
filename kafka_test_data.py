import json
import random
import time
from datetime import datetime
from kafka_config import produce_message, flush_producer, update_kafka_credentials

def generate_sample_task(task_id):
    """Generate a sample task with the given ID"""
    priorities = [1, 2, 3]  # Low, Medium, High
    task_types = ["data_processing", "report_generation", "system_maintenance", "user_request", "alert_response"]
    
    return {
        "task_id": task_id,
        "title": f"Sample Task {task_id}",
        "description": f"This is a sample task #{task_id} for testing purposes",
        "priority": random.choice(priorities),
        "type": random.choice(task_types),
        "created_at": datetime.now().isoformat(),
        "status": "pending",
        "assigned_to": None
    }

def generate_sample_notification(notification_id, related_task_id=None):
    """Generate a sample notification with the given ID"""
    priorities = [1, 2, 3]  # Low, Medium, High
    senders = ["system", "admin", "scheduler", "monitor"]
    
    notification = {
        "notification_id": notification_id,
        "title": f"Notification {notification_id}",
        "message": f"This is sample notification #{notification_id} for testing",
        "priority": random.choice(priorities),
        "sender": random.choice(senders),
        "timestamp": datetime.now().isoformat(),
        "read": False
    }
    
    if related_task_id:
        notification["related_to"] = "task"
        notification["task_id"] = related_task_id
    
    return notification

def push_sample_data_to_kafka(username="your_actual_username", password="your_actual_password"):
    """Push sample tasks and notifications to Kafka"""
    # Update AWS MSK credentials
    update_kafka_credentials(username, password)
    
    print("Pushing sample tasks to Kafka...")
    for i in range(1, 6):
        task = generate_sample_task(i)
        # Use the key as the task_id string
        produce_message('tasks', str(task['task_id']), task)
        print(f"Pushed task {i}: {task['title']} (Priority: {task['priority']})")
        time.sleep(0.5)  # Small delay between messages
    
    print("\nPushing sample notifications to Kafka...")
    # Create some notifications related to tasks and some standalone
    for i in range(1, 8):
        # For some notifications, relate them to tasks
        related_task_id = i if i <= 5 and random.choice([True, False]) else None
        notification = generate_sample_notification(i, related_task_id)
        # Use the notification_id as the key
        produce_message('notifications', str(notification['notification_id']), notification)
        
        relation_info = f"(Related to Task {related_task_id})" if related_task_id else "(Standalone)"
        print(f"Pushed notification {i}: {notification['title']} {relation_info}")
        time.sleep(0.5)  # Small delay between messages
    
    # Flush to ensure all messages are sent
    flush_producer()
    
    print("\nAll sample data has been pushed to Kafka successfully!")

if __name__ == "__main__":
    try:
        push_sample_data_to_kafka()
    except Exception as e:
        print(f"Error pushing data to Kafka: {e}") 