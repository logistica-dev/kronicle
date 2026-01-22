# kronicle/db/core/models/app_metadata.py
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import JSONB

from kronicle.db.core.models.core_entity import CoreEntity


class AppMetadata(CoreEntity):
    __tablename__ = "kronicle_app"
    __mapper_args__ = {"eager_defaults": True}

    version = Column(String(20), nullable=False)
    tables_version = Column(JSONB, default={})  # Ex: {"rbac.users": 1, "targets.channels": 2}

    def __repr__(self):
        return f"<AppMetadata {self.name} (v{self.version})>"
