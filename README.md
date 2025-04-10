# MCP Server Demo
A production-ready task management system built with MCP (Model Control Protocol) and Kafka.

## Overview

This project demonstrates a robust task management system using MCP to enable AI agents to interact with a Kafka-based task queue. The system allows for:

- Task management (creating, updating, completing tasks)
- Notification handling
- Real-time event processing via Kafka

## Features

- **Task Management**: Create, update, prioritize, and complete production tasks
- **Notification System**: Real-time notifications with priority levels
- **Kafka Integration**: Reliable message queuing and event streaming
- **MCP Tools**: AI-friendly interfaces for task and notification operations
- **Consumer Services**: Background processing of Kafka messages

## Requirements

- Python 3.13+
- Kafka cluster (local or AWS MSK)
- Confluent Kafka Python client

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/mcp-server-demo.git
cd mcp-server-demo

# Install dependencies
pip install -e .
```

## Configuration

Update the Kafka configuration in `kafka_config.py` with your actual Kafka cluster details:

```python
KAFKA_CONFIG = {
    'bootstrap.servers': 'your-kafka-bootstrap-servers',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'your-username',
    'sasl.password': 'your-password',
}
```

## Usage

### Starting the Server

```bash
python main.py
```

### Loading Test Data

To populate the system with sample tasks and notifications:

```bash
python kafka_test_data.py
```

### MCP Tools

The system exposes the following MCP tools for AI agents:

#### Task Management
- `fetch_queue`: Get a list of pending tasks
- `change_task_priority`: Update task priority
- `pickup_task`: Mark a task as in progress
- `complete_task`: Mark a task as completed
- `get_task_details`: Get detailed information about a task
- `check_task_status`: Check the current status of a task

#### Notification Management
- `check_notification_count`: Get count of unread notifications
- `get_notification_list`: Get a filtered list of notifications
- `mark_notification_as_read`: Mark a notification as read

## Architecture

The system consists of several components:

- **MCP Server**: Exposes tools for AI agents to interact with the system
- **Kafka Producers**: Send messages to Kafka topics
- **Kafka Consumers**: Process messages from Kafka topics
- **Task Service**: Business logic for task management
- **Notification Service**: Business logic for notification handling

## Development

### Project Structure

```
mcp-server-demo/
├── main.py                # MCP server initialization
├── kafka_config.py        # Kafka configuration
├── consumer_service.py    # Kafka consumer services
├── task_service.py        # Task management logic
├── notification_service.py # Notification handling logic
└── kafka_test_data.py     # Test data generator
```

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
