# kronicle/repo/core/core_channel_repo.py


from kronicle.db.core.models.core_channel import Channel
from kronicle.repo.kronicle_repo import KronicleRepository


class CoreChannelRepository(KronicleRepository[Channel]):

    model = Channel
