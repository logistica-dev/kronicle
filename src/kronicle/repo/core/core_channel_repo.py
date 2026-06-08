# kronicle/repo/core/core_channel_repo.py


from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.repo.kronicle_repo import KronicleRepository


class CoreChannelRepository(KronicleRepository[CoreChannel]):

    model = CoreChannel
