from kronicle.logging.log_event import LogEvent
from kronicle.logging.log_producer import event_queue


async def console_consumer():
    """Simple sink: print to stdout."""
    while True:
        event: LogEvent = await event_queue.get()
        print(f"[{event.timestamp}] {event.level} {event.source}.{event.action}: {event.message} | {event.details}")
        event_queue.task_done()
