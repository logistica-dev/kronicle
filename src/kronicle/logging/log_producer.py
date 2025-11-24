# kronicle/logging/log_producer.py
import asyncio

from .log_event import LogEvent

# Single global queue (can later be replaced by Kafka, etc.)
event_queue: asyncio.Queue[LogEvent] = asyncio.Queue()


async def publish_event(event: LogEvent):
    await event_queue.put(event)
