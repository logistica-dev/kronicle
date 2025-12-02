import asyncio

from kronicle.log_bus.event_bus import api_queue, data_queue, setup_queue
from kronicle.utils.dev_logs import log_d


async def consume_setup_logs():
    while True:
        event = await setup_queue.get()
        log_d("Event", "ADMIN", event)


async def consume_data_logs():
    while True:
        event = await data_queue.get()
        log_d("Event", "WRITER", event)


async def consume_api_logs():
    while True:
        event = await api_queue.get()
        log_d("Event", "READER", event)


def start_consumers():
    return [
        asyncio.create_task(consume_setup_logs()),
        asyncio.create_task(consume_data_logs()),
        asyncio.create_task(consume_api_logs()),
    ]
