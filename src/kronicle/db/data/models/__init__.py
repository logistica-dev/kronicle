# kronicle/db/data/models/__init.py__
from kronicle.db.data.models.channel_metadata import ChannelMetadata

DATA_NAMESPACE = ChannelMetadata.namespace()

ALL_DATA_TABLES = [ChannelMetadata]
