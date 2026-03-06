from sqlalchemy import String
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_view import KronicleView
from kronicle.db.core.models.channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.zone import Zone
from kronicle.utils.str_utils import normalize_pg_identifier


class CoreResource(KronicleView):
    __tablename__ = "resources"

    id: Mapped[PgUUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    type: Mapped[str] = mapped_column(String(16), nullable=False)

    @classmethod
    def namespace(cls):
        return CoreEntity.namespace()

    @classmethod
    def create_view_sql(cls) -> str:
        ns = normalize_pg_identifier(cls.namespace())
        return f"""
        CREATE OR REPLACE VIEW {ns}.{cls.tablename()} AS
        SELECT id, 'zone' AS type FROM {ns}.{Zone.tablename()}
        UNION ALL
        SELECT id, 'channel' AS type FROM {ns}.{Channel.tablename()}
        """
