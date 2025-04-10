from typing import Dict, List, Optional, Any
import logging
from kafka_config import produce_message, flush_producer, delete_message
from consumer_service import task_consumer_service
import time

logger = logging.getLogger('task_service')

def fetch_queue(priority: Optional[int] = None, count: int = 10, page_no: int = 1) -> List[Dict]:
    """
    Fetch tasks from the cache with optional filtering by priority
    """
    # Get all tasks from the cache
    all_tasks = task_consumer_service.get_all_messages()
    
    # Filter by priority if specified
    if priority is not None:
        filtered_tasks = [task for task in all_tasks if task.get('priority') == priority]
    else:
        filtered_tasks = all_tasks
    
    # Sort tasks by priority (higher priority first)
    sorted_tasks = sorted(filtered_tasks, key=lambda x: x.get('priority', 0), reverse=True)
    
    # Apply pagination
    start_idx = (page_no - 1) * count
    end_idx = start_idx + count
    paginated_tasks = sorted_tasks[start_idx:end_idx] if start_idx < len(sorted_tasks) else []
    
    return paginated_tasks

def get_task_details(task_id: int) -> Dict:
    """
    Get details of a specific task from the cache
    """
    task = task_consumer_service.get_message_by_id(str(task_id))
    return task if task else {}

def check_task_status(task_id: int) -> str:
    """
    Check the status of a specific task
    """
    task = get_task_details(task_id)
    return task.get('status', 'not_found')

def change_task_priority(task_id: int, new_priority: int) -> bool:
    """
    Change the priority of a task
    """
    # First, get the task details
    task = get_task_details(task_id)
    if not task:
        logger.warning(f"Task {task_id} not found for priority change")
        return False
    
    # Update the task priority
    task['priority'] = new_priority
    
    # Update the cache
    task_consumer_service.update_message(str(task_id), {'priority': new_priority})
    
    # Send the updated task back to the tasks queue
    task_success = produce_message('tasks', str(task_id), task)
    
    # Ensure messages are delivered
    flush_producer()
    
    return task_success

def pickup_task(task_id: int) -> bool:
    """
    Mark a task as being worked on
    """
    # Get the task details
    task = get_task_details(task_id)
    if not task:
        logger.warning(f"Task {task_id} not found for pickup")
        return False
    
    # Update the task status
    task['status'] = 'in_progress'
    
    # Update the cache
    task_consumer_service.update_message(str(task_id), {'status': 'in_progress'})
    
    # Send the updated task back to the tasks queue
    task_success = produce_message('tasks', str(task_id), task)    
    flush_producer()
    
    return task_success

def complete_task(task_id: int) -> bool:
    """
    Mark a task as completed and remove it from the queue
    """
    # Get the task details
    task = get_task_details(task_id)
    if not task:
        logger.warning(f"Task {task_id} not found for completion")
        return False
    
    # Update completion status
    task['status'] = 'completed'
    task['completion_time'] = int(time.time())
    
    # Archive the completed task before removing it
    archive_success = archive_completed_task(task_id, task.copy())
    
    # Remove the task from the cache
    task_consumer_service.remove_message(str(task_id))
    
    # Delete the task from the Kafka queue (tombstone message)
    delete_success = delete_message('tasks', str(task_id))
    
    # Ensure messages are delivered
    flush_producer()
    
    logger.info(f"Task {task_id} marked as completed, archived, and removed from queue")
    return archive_success and delete_success

def archive_completed_task(task_id: int, task_data: Dict) -> bool:
    """
    Archive a completed task to a separate topic for record-keeping
    """
    # Make sure the task is marked as completed
    task_data['status'] = 'completed'
    if 'completion_time' not in task_data:
        task_data['completion_time'] = int(time.time())
    
    # Send to a separate archive topic
    success = produce_message('completed_tasks', str(task_id), task_data)
    flush_producer()
    
    logger.info(f"Task {task_id} archived to completed_tasks topic")
    return success 