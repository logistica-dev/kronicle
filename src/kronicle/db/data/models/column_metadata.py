# kronicle/db/data/models/column_metadata.py
from __future__ import annotations

from typing import Any, Iterable, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

from kronicle.errors.error_types import BadRequestError

mod = "col_meta"


# --------------------------------------------------------------------------------------------------
# ColumnMetadata
# --------------------------------------------------------------------------------------------------
class ColumnMetadata(BaseModel):
    """
    Structured, mutable, per-column annotations for a channel's columns.

    Stored in `ChannelMetadata.column_metadata` as `dict[<db_col_name>, ColumnMetadata]`.
    It is intentionally NOT part of `ChannelSchema`: the schema keeps the immutable
    structure (db column name + type), while this carries the mutable labels:
    - `unit`: physical unit, e.g. 'Cel' (UCUM) or 'C'
    - `description`: free text
    - `ontology`: URI/IRI towards an external ontology term (not URI-validated)

    Unknown descriptors are accepted (`extra="allow"`) so the wire format can grow
    descriptive fields without a breaking change.
    """

    model_config = ConfigDict(extra="allow")

    unit: str | None = Field(default=None, max_length=64, description="Physical unit, e.g. 'Cel' (UCUM) or 'C'")
    description: str | None = Field(default=None, max_length=4096, description="Free-text description")
    ontology: str | None = Field(default=None, max_length=512, description="URI/IRI to an external ontology term")

    @field_validator("unit", "description", "ontology", mode="before")
    @classmethod
    def _strip_empty_to_none(cls, v: Any) -> str | None:
        """Coerce to stripped string; whitespace-only / empty becomes None."""
        if v is None:
            return None
        if not isinstance(v, str):
            raise TypeError("must be a string or None")
        v = v.strip()
        return v or None

    # ----------------------------------------------------------------------------------------------
    # Serialization helpers
    # ----------------------------------------------------------------------------------------------
    def is_empty(self) -> bool:
        """True when there is no descriptive content at all (no known key set, no extras)."""
        return not self.to_json()

    def to_json(self) -> dict[str, Any]:
        """Serializable form: drop None known keys, keep every extra."""
        return self.model_dump(exclude_none=True)

    def __bool__(self) -> bool:
        """Empty descriptors are falsy (see `is_empty`)."""
        return not self.is_empty()


ColumnMetadataMap = dict[str, ColumnMetadata]


# --------------------------------------------------------------------------------------------------
# Container helpers
# --------------------------------------------------------------------------------------------------
def normalize_column_metadata(
    column_metadata: Mapping[str, ColumnMetadata | Mapping[str, Any]] | None,
    allowed_columns: Iterable[str],
) -> ColumnMetadataMap:
    """
    Validate and normalize a {<db_col_name> -> ColumnMetadata} mapping.

    Keys are the normalized DB table columns (NOT user column names).
    - Rejects keys that are not real columns (catches typos), even when empty.
    - Coerces bare dicts into ColumnMetadata.
    - Drops entries that carry no content ({"temp": {}} never accumulates).

    Args:
        column_metadata: raw mapping (values may be dicts or ColumnMetadata).
        allowed_columns: the channel's DB column names (from ChannelSchema).

    Raises:
        BadRequestError: if a key is not an allowed column.
    """
    if not column_metadata:
        return {}

    allowed = set(allowed_columns)
    unknown = sorted(k for k in column_metadata if k not in allowed)
    if unknown:
        raise BadRequestError("Unknown column(s) in column_metadata", details={"columns": unknown})

    normalized: ColumnMetadataMap = {}
    for name, cm in column_metadata.items():
        meta = cm if isinstance(cm, ColumnMetadata) else ColumnMetadata(**cm)
        if not meta.is_empty():
            normalized[name] = meta
    return normalized


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    from kronicle.utils.dev_logs import log_d, log_w

    here = "col_meta.test"

    log_d(here, "basic", ColumnMetadata(unit="Cel", description="Air temperature").to_json())
    log_d(here, "extras", ColumnMetadata(unit="%", precision=2, source="sensor-A").to_json())  # type: ignore[reportCallIssue]
    log_d(here, "empty", "is_empty:", ColumnMetadata(unit="   ", description="").is_empty())

    mapped = normalize_column_metadata(
        {"temperature": {"unit": "Cel"}, "humidity": {}},
        allowed_columns=["temperature", "humidity", "time"],
    )
    log_d(here, "normalized map", {k: v.to_json() for k, v in mapped.items()})

    try:
        normalize_column_metadata({"temp": {"unit": "C"}}, allowed_columns=["temperature"])
    except BadRequestError as e:
        log_w(here, "unknown col rejected", e.message, e.details)
