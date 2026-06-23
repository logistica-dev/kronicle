# kronicle/logging/log_bus/event_bus.py
import asyncio

setup_queue = asyncio.Queue()
data_queue = asyncio.Queue()
api_queue = asyncio.Queue()

answers_queue = asyncio.Queue()
