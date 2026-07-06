# kronicle/schemas/rbac/input_schema.py
from __future__ import annotations

from typing import Any, Self
from uuid import UUID, uuid4

from kronicle_sdk.utils.str_utils import serialize
from pydantic import BaseModel

from kronicle.db.base.kronicle_base import KronicleBase

mod = "input"


class OutputSchema(BaseModel):
    id: UUID
    name: str | None = None
    details: dict[str, Any] | None = None

    def model_dump(self, *args, exclude_none=True, **kwargs) -> dict:
        d = super().model_dump(*args, exclude_none=exclude_none, **kwargs)
        return {k: serialize(v) if isinstance(v, UUID) else v for k, v in d.items()}

    def model_dump_json(self, *args, exclude_none=True, **kwargs) -> str:
        d = super().model_dump(*args, exclude_none=exclude_none, **kwargs)
        return serialize(d)

    def __str__(self) -> str:
        return super().model_dump_json(exclude_none=True)

    @classmethod
    def from_db(cls, row: KronicleBase) -> Self:
        return cls.model_validate(row, from_attributes=True)


if __name__ == "__main__":  # pragma: no cover
    test1 = OutputSchema(id=uuid4(), name="test1")
    print(test1)
