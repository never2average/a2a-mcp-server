import httpx
from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Optional
import atexit
import logging

# Import services
from task_service import (
    fetch_queue, 
    change_task_priority, 
    pickup_task, 
    complete_task, 
    get_task_details, 
    check_task_status
)
from notification_service import (
    check_notification_count, 
    get_notification_list, 
    mark_notification_as_read
)
from consumer_service import start_consumers, stop_consumers
from kafka_config import create_consumer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('mcp_app')

# Initialize MCP
mcp = FastMCP("Production Task Manager")

# Start Kafka consumers
logger.info("Starting Kafka consumers...")
start_consumers()

# Register shutdown handler
atexit.register(stop_consumers)

# Register task-related tools
@mcp.tool("fetch_queue")
def mcp_fetch_queue(priority: Optional[int] = None, count: int = 10, page_no: int = 1) -> List[Dict]:
    return fetch_queue(priority, count, page_no)

@mcp.tool("change_task_priority")
def mcp_change_task_priority(task_id: int, new_priority: int) -> bool:
    return change_task_priority(task_id, new_priority)

@mcp.tool("pickup_task")
def mcp_pickup_task(task_id: int) -> bool:
    return pickup_task(task_id)

@mcp.tool("complete_task")
def mcp_complete_task(task_id: int) -> bool:
    return complete_task(task_id)

@mcp.tool("get_task_details")
def mcp_get_task_details(task_id: int) -> Dict:
    return get_task_details(task_id)

@mcp.tool("check_task_status")
def mcp_check_task_status(task_id: int) -> str:
    return check_task_status(task_id)

# Register notification-related tools
@mcp.tool("check_notification_count")
def mcp_check_notification_count() -> int:
    return check_notification_count()

@mcp.tool("get_notification_list")
def mcp_get_notification_list(priority: Optional[int] = None, sender: Optional[str] = None, 
                             page_no: int = 1, page_size: int = 10, 
                             related_to: Optional[str] = None, 
                             task_id: Optional[int] = None) -> List[Dict]:
    return get_notification_list(priority, sender, page_no, page_size, related_to, task_id)

@mcp.tool("mark_notification_as_read")
def mcp_mark_notification_as_read(notification_id: int) -> bool:
    return mark_notification_as_read(notification_id)

# Create a consumer for the completed tasks topic if needed
completed_tasks_consumer = create_consumer('completed_tasks', 'completed_tasks_consumer_group')

logger.info("MCP application initialized and ready")

