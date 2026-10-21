# tests/unit/repo/core/test_core_channel_repo.py
from unittest.mock import MagicMock
from uuid import uuid4

from sqlalchemy.orm import Session

from kronicle.repo.core.core_channel_repo import CoreChannelRepository


def test_get_by_zone_returns_matching_channels():
    repo = CoreChannelRepository()
    mock_db = MagicMock(spec=Session)
    zone_id = uuid4()
    channels = [object(), object()]
    mock_db.execute.return_value.scalars.return_value.all.return_value = channels

    result = repo.get_by_zone(mock_db, zone_id=zone_id)

    assert result is channels
    mock_db.execute.assert_called_once()
