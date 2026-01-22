# kronicle/deps/channel_deps.py
from fastapi import Request

from kronicle.services.channel_service import ChannelService


def channel_service(request: Request) -> ChannelService:
    """FastAPI dependency to retrieve the channel service from app.state"""
    return request.app.state.channel_service
