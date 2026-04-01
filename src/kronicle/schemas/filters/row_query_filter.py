# kronicle/schemas/filters/row_query_filter.py
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from fastapi import Query, Request
from pydantic import BaseModel, PrivateAttr, field_validator

from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.utils.dev_logs import log_block, log_d
from kronicle.utils.str_utils import normalize_name, normalize_name_keep_dots, normalize_sort_name, split_strip

DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0
MAX_LIMIT = 500
MAX_OFFSET = 10_000  # arbitrary, can adjust
DEFAULT_STRICT_MODE = False


class RowQueryFilter(BaseModel):
    """
    Human-friendly query filter for rows/columns.

    URL syntax examples:
        ?col[name]=Tintin
        ?min[temperature]=20
        ?max[temperature]=30
        ?list[tag]=room1,room2
    """

    # Row-level pagination/sorting
    limit: int | None = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Max rows to return")
    offset: int | None = Query(None, ge=0, le=MAX_OFFSET, description="Number of rows to skip")
    sort: str | list[str] | None = Query(
        None,
        description="Sort columns, prefix with '-' for descending order. Can be comma-separated string or repeated parameter (e.g. ?sort=target,-time)",
    )

    # Column selection
    columns: list[str] | None = Query(None, description="Columns to include in the result")
    skip_received: bool | None = Query(True, description="False to include reception timestamp")

    # Row filters
    col: dict[str, str | list[str]] | None = Query(default={}, description="Exact match filters: ?col[name]=Tintin")
    min: dict[str, str | list[str]] | None = Query(
        default={}, description="Minimum value filters: ?min[time]=2026-03-20"
    )
    max: dict[str, str | list[str]] | None = Query(
        default={}, description="Maximum value filters: ?max[time]=2026-03-21"
    )
    any: dict[str, list[str]] | None = Query(default={}, description="Multi-value filters: ?any[status]=ok,fail")
    has: dict[str, list[str]] | None = Query(default={}, description="Multi-value filters: ?any[status]=ok,fail")

    # Strict mode and feedback
    strict: bool | None = Query(DEFAULT_STRICT_MODE, description="Raise errors if true, otherwise accumulate warnings")
    _feedback: OpFeedback = PrivateAttr(default_factory=OpFeedback)

    @property
    def feedback(self) -> OpFeedback:
        return self._feedback

    # --------------------------------------------------------------------------
    # Normalization
    # --------------------------------------------------------------------------
    @field_validator("limit", mode="before")
    @classmethod
    def cap_limit(cls, v):
        if v is None or v < 1:
            return DEFAULT_LIMIT
        return min(v, MAX_LIMIT)

    @field_validator("offset", mode="before")
    @classmethod
    def cap_offset(cls, v):
        if v is None:
            return None
        if v < 1:
            v = DEFAULT_OFFSET
        return min(v, MAX_OFFSET)

    # | Operator | Single or Multi | Comma splitting | Repeated query param | Dot notation |
    # | -------- | --------------- | --------------- | -------------------- | ------------ |
    # | `col`    | Single          | No              | Reject or first      | Allowed      |
    # | `min`    | Single          | No              | Reject or first      | Not allowed  |
    # | `max`    | Single          | No              | Reject or first      | Not allowed  |
    # | `any`    | Multi           | Yes             | Flatten              | Allowed      |
    # | `has`    | Multi           | Yes             | Flatten              | Not allowed  |
    # | `sort`   | Multi           | Yes             | Flatten              | Not allowed  |
    def normalize_single_value(self, filt: dict | None, norm: Callable, field_name: str):
        if not filt:
            return {}
        if not isinstance(filt, dict):
            self._feedback.add_detail(message="Expected dict", field="query", subfield=field_name)
            return {}
        result = {}
        for key, val in filt.items():
            if isinstance(val, list):
                if len(val) > 1:
                    self._feedback.add_detail(
                        message="Multiple values not allowed for single-value filter",
                        field="query",
                        subfield=f"{field_name}[{key}]",
                    )
                    continue
                val = val[0]
            try:
                norm_k = norm(key)
                if norm_k:
                    result[norm_k] = val
            except ValueError:
                self._feedback.add_detail(
                    message="Invalid column name",
                    field="query",
                    subfield=f"{field_name}[{key}]",
                )
        return result

    def normalize_multi_value(self, filt: dict | None, norm: Callable, field_name: str) -> dict[str, list[str]]:
        if not filt:
            return {}
        if not isinstance(filt, dict):
            self._feedback.add_detail(message="Expected dict", field="query", subfield=field_name)
            return {}
        result = {}
        for key, val in filt.items():
            if isinstance(val, list):
                # flatten comma-separated elements
                vals: list[str] = []
                for s in val:
                    vals.extend(split_strip(s, ","))
            else:
                vals = split_strip(str(val), ",")
            try:
                norm_k = norm(key)
                if norm_k:
                    result[norm_k] = vals
            except ValueError:
                self._feedback.add_detail(
                    message="Invalid column name",
                    field="query",
                    subfield=f"{field_name}[{key}]",
                )
        return result

    def model_post_init(self, __context: Any) -> None:
        here = "model_post_init"
        with log_block(here, "resolving filters"):
            self.col = self.normalize_single_value(self.col, normalize_name_keep_dots, field_name="col")
            self.min = self.normalize_single_value(self.min, normalize_name, field_name="min")
            self.max = self.normalize_single_value(self.max, normalize_name, field_name="max")
            self.any = self.normalize_multi_value(self.any, normalize_name_keep_dots, field_name="any")
            self.has = self.normalize_multi_value(self.has, normalize_name, field_name="has")
            self.sort = self.normalize_sort()
            self.columns = self.split_column_selection()

            log_d(here, "filter", self.model_dump_json())

    def normalize_sort(self) -> list[str] | None:
        """
        What is expected is either a list of column names (JSON not allowed),
        or a string with comma-separated column names.
        """
        v = self.sort
        if v is None:
            return None
        if isinstance(v, str):
            # Split comma-separated string
            try:
                return split_strip(v, ",", normalize_sort_name) or None
            except ValueError:
                self._feedback.add_detail(message="Invalid column name", field="query", subfield=f"sort={v}")
                return
        if isinstance(v, list):
            # flatten comma-separated elements
            result: list[str] = []
            for s in v:
                try:
                    result.extend(split_strip(s, ",", normalize_sort_name))
                except ValueError:
                    self._feedback.add_detail(message="Invalid column name", field="query", subfield=f"sort={s}")
                    return
            return result or None
        # fallback
        return None

    def split_column_selection(self):
        cols = self.columns
        if self.columns is None:
            return None
        if isinstance(cols, str):
            try:
                return split_strip(cols, ",", normalize_name)
            except ValueError:
                self._feedback.add_detail(message="Invalid column name", field="columns", subfield=f"columns={cols}")
                return
        if isinstance(cols, list):
            try:
                return [normed for col in cols if (normed := normalize_name(col))]
            except ValueError:
                self._feedback.add_detail(message="Invalid column name", field="columns", subfield=f"columns={cols}")
                return
        return None

    @classmethod
    def _is_key(cls, key: str, val: Any, filt: str, filt_dict: dict):
        if key.startswith(f"{filt}[") and key.endswith("]"):
            filt_dict[key[4:-1]] = val

    @classmethod
    def parse_bracketed_filters(cls, query_params: list[tuple[str, str]]) -> dict[str, dict]:
        adv_filters: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

        for key, val in query_params:
            if len(key) > 4 and key[3] == "[" and key.endswith("]"):
                prefix = key[:3]  # e.g. 'min', 'max', 'col'
                inner = key[4:-1]  # strip 'min[' and trailing ']'
                adv_filters[prefix][inner].append(val)

        return adv_filters

    @classmethod
    def from_query_params(cls, request: Request) -> RowQueryFilter:
        # Pydantic parse simple params
        query_params = request.query_params
        simple_fields = {
            "limit": int(limit) if (limit := query_params.get("limit")) is not None else None,
            "offset": query_params.get("offset"),
            "sort": query_params.get("sort"),
            "columns": query_params.get("columns"),
            "skip_received": query_params.get("skip_received"),
            "strict": query_params.get("strict"),
        }
        # Parse advanced bracketed filters manually
        adv = cls.parse_bracketed_filters(query_params.multi_items())

        # Convert lists to single values for single-value filters
        min_dict = {k: v[0] for k, v in adv.get("min", {}).items()}
        max_dict = {k: v[0] for k, v in adv.get("max", {}).items()}
        col_dict = {k: v[0] for k, v in adv.get("col", {}).items()}
        any_dict = adv.get("any", {})
        has_dict = adv.get("has", {})
        return RowQueryFilter(
            min=min_dict,
            max=max_dict,
            col=col_dict,
            any=any_dict,
            has=has_dict,
            **simple_fields,
        )


if __name__ == "__main__":  # pragma: no cover
    from kronicle.utils.dev_logs import log_block, log_d, setup_logging

    here = "RowQueryFilter"

    setup_logging()
    adv = RowQueryFilter.parse_bracketed_filters([("min[time]", "2026-03-31T12:04:14.476554Z")])
    log_d(here, "adv", adv)
