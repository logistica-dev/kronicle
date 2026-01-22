# kronicle/schemas/payload/op_feedback.py
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass
class OpDetail:
    """
    Represents a single validation error detail.
    - field: top-level payload field (e.g., 'rows', 'tags')
    - subfield: optional subfield (e.g., row number, tag key)
    - message: human-readable explanation
    - extra: optional dict for any extra context
    """

    message: str
    field: str
    subfield: str | None = None
    extra: Any | None = None

    @property
    def json(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OpFeedback:
    """
    Collects validation error details for a payload.
    """

    details: List[OpDetail] = field(default_factory=list)

    def add_detail(self, message: str, field: str, subfield: str | None = None, extra: Any | None = None) -> None:
        """Add a new validation error detail."""
        self.details.append(OpDetail(message=message, field=field, subfield=subfield, extra=extra))

    @property
    def has_details(self) -> bool:
        return bool(self.details)

    def json(self) -> Dict[str, Any]:
        """Serialize all details to a JSON-friendly dictionary."""
        return {"details": [d.json for d in self.details]} if self.details else {}
