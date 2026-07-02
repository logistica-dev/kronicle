# kronicle/repo/core/core_row_repo.py

from kronicle.db.core.models.core_row import CoreRow
from kronicle.repo.kronicle_repo import KronicleRepository


class CoreRowRepository(KronicleRepository[CoreRow]):

    model = CoreRow
