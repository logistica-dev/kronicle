# kronicle/db/data/models/__init__.py
from kronicle.db.data.models.channel_metadata import ChannelMetadata

DATA_NAMESPACE = ChannelMetadata.namespace()

ALL_DATA_TABLES = [ChannelMetadata]
