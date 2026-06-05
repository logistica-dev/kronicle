# kronicle/repo/core/core_zone_repo.py


from kronicle.db.core.models.core_zone import Zone
from kronicle.repo.kronicle_repo import KronicleRepository


class CoreZoneRepository(KronicleRepository[Zone]):

    model = Zone
