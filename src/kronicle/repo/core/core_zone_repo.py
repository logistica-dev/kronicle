# kronicle/repo/core/core_zone_repo.py


from kronicle.db.core.models.core_zone import CoreZone
from kronicle.repo.kronicle_repo import KronicleRepository


class CoreZoneRepository(KronicleRepository[CoreZone]):

    model = CoreZone
