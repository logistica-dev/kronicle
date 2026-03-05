# kronicle/db/data/models/channel_timeseries.py

from __future__ import annotations

from typing import Any
from uuid import UUID

from asyncpg.pool import PoolConnectionProxy

from kronicle.db.data.models.channel_metadata import ChannelMetadata
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.schemas.payload.request_filter import RequestFilter
from kronicle.utils.asyncpg_utils import table_exists
from kronicle.utils.dev_logs import log_d, log_i, log_w
from kronicle.utils.str_utils import check_is_uuid4

mod = "chan_ts"


class ChannelTimeseries:
    """
    ChannelTimeseries represents the runtime, database-bound instance
    of a channel's timeseries.

    Responsibilities:
    - Bind a ChannelSchema to a specific channel_id.
    - Define table naming.
    - Generate CREATE TABLE SQL.
    - Define INSERT column order.
    - Convert validated rows to DB tuples.
    - Verify DB structure compatibility at startup.
    - Maintain an in-memory buffer of validated rows.

    It is:
    - Storage-aware.
    - Responsible for DB materialization.
    - The guardian of schema ↔ table consistency.

    It does NOT:
    - Mutate the ChannelSchema.
    - Redefine structural validation rules.

    Immutable fields:
    - channel_id
    - channel_schema

    Mutable operations:
    - add_rows, set_rows
    - fetch, insert, delete
    """

    _NAMESPACE: str = ChannelMetadata.namespace()

    def __init__(
        self,
        channel_id: UUID,
        channel_schema: ChannelSchema,
        op_feedback: OpFeedback | None = None,
    ):
        self.channel_id = check_is_uuid4(channel_id)
        self.channel_schema = channel_schema
        self._rows: list[dict[str, Any]] = []
        self.op_feedback: OpFeedback = op_feedback or OpFeedback()

    # ----------------------------------------------------------------------------------------------
    # Table Identity
    # ----------------------------------------------------------------------------------------------
    @property
    def table_name(self) -> str:
        return f"channel_{str(self.channel_id).replace('-', '')}"

    # ----------------------------------------------------------------------------------------------
    # DB Column Layout
    # ----------------------------------------------------------------------------------------------
    @property
    def _sql_insert_columns(self) -> list[str]:
        """
        Columns used for INSERT statements (row_id excluded).
        """
        return ["time"] + list(self.channel_schema.user_columns.keys()) + ["received_at"]

    # ----------------------------------------------------------------------------------------------
    # DB tuple Conversion
    # ----------------------------------------------------------------------------------------------
    def get_db_tuples(self) -> list[tuple]:
        """
        Convert stored rows into DB-ready tuples.
        All columns (required and optional) are guaranteed to exist in each row.
        """
        cols = self._sql_insert_columns
        return [tuple(row[col] for col in cols) for row in self._rows]

    # ----------------------------------------------------------------------------------------------
    # Row Handling
    # ----------------------------------------------------------------------------------------------
    @property
    def rows(self):
        return self._rows

    def add_rows(self, rows: list[dict[str, Any]], strict: bool = False) -> ChannelTimeseries:
        """
        Validate and store multiple rows in the channel timeseries.

        Responsibilities:
        - Validates each row against the associated ChannelSchema.
        - Stores all successfully validated rows in-memory.
        - Collects per-row validation errors as warnings.

        Parameters:
        -----------
        rows : list[dict[str, Any]]
            List of input rows to validate and store.
        strict : bool, default=False
            If True, raises BadRequestError after validating all rows
            if any row failed validation.
            If False, stores valid rows and returns a warnings dictionary for invalid rows.
        warnings : dict[str, str] | None, optional
            Dictionary to collect row-level warnings when `strict=False`.
            Keys are 1-based, zero-padded row indices (e.g., 'row_01') and values are error messages.

        Returns:
        --------
        warnings : dict[str, str]
            Mapping of row indices to error messages for rows that failed validation.
            Empty if all rows were successfully validated.

        Raises:
        -------
        BadRequestError
            - If `rows` is empty.
            - If `strict=True` and any row fails validation.
            - If no rows are valid (even if `strict=False`).

        Notes:
        ------
        - Row indices in warnings are 1-based and zero-padded for readability.
        - Validated rows are appended to `self._rows`.
        - This method replaces the previous ProcessedPayload._validate_rows functionality.
        """
        if not rows:
            raise BadRequestError("No rows to validate", details={"channel_id": str(self.channel_id)})

        validated_rows = []

        pad_width = len(str(len(rows)))
        for idx, row in enumerate(rows, start=1):
            try:
                validated_rows.append(self.channel_schema.validate_row(row, from_user=True))
            except ValueError as e:
                row_label = f"row_{str(idx).zfill(pad_width)}"
                self.op_feedback.add_detail(message=str(e), field="rows", subfield=row_label)

        if strict and self.op_feedback.has_details:
            raise BadRequestError(
                "Validation failed for some rows",
                details=self.op_feedback.json(),
            )

        if self.rows and not validated_rows:
            # No valid rows survived
            self.op_feedback.add_detail("No valid rows to insert", field="rows")
            raise BadRequestError(
                "No valid rows to insert",
                details=self.op_feedback.json(),
            )

        self._rows.extend(validated_rows)
        return self

    def clear_rows(self) -> None:
        self._rows.clear()

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        self.clear_rows()
        self.add_rows(rows)

    def __len__(self) -> int:
        return len(self._rows)

    # ----------------------------------------------------------------------------------------------
    # DB Schema Verification
    # ----------------------------------------------------------------------------------------------
    def verify_db_schema(self, db_columns: dict[str, str]) -> None:
        """
        Verify live DB columns match this schema strictly.
        Raises ValueError if mismatch is detected.
        """
        expected_types = {col: col_type.db_type for col, col_type in self.channel_schema.user_columns.items()}

        for col, expected in expected_types.items():
            actual = db_columns.get(col)
            if actual is None:
                raise ValueError(f"Missing column '{col}' in DB table {self.table_name}")
            if actual.upper() != expected.upper():
                raise ValueError(f"Column '{col}' type mismatch (expected {expected}, found {actual})")

        extra_cols = set(db_columns) - set(expected_types)
        if extra_cols:
            raise ValueError(f"Unexpected columns in DB table {self.table_name}: {extra_cols}")

    # ----------------------------------------------------------------------------------------------
    # User-facing rows
    # ----------------------------------------------------------------------------------------------
    def to_user_rows(self, *, skip_received: bool = True) -> list[dict[str, Any]]:
        """
        Convert internal DB rows into user-facing rows.

        Responsibilities:
        - Renames columns according to the original user-provided names.
        - Optionally skips system/internal fields like 'received_at'.
        - Returns a list of dicts ready to be serialized to the user.

        Parameters
        ----------
        skip_received : bool, default=True
            If True, remove the 'received_at' field from each row.

        Returns
        -------
        list[dict[str, Any]]
            List of rows with user column names.
        """
        user_rows = []

        for row in self._rows:
            user_row = {}
            for db_col, val in row.items():
                if skip_received and db_col == "received_at":
                    continue
                # Get user column name from schema, fallback to db_col
                user_col = self.channel_schema.get_usr_col_name(db_col)
                user_row[user_col] = val
            user_rows.append(user_row)

        return user_rows

    # ----------------------------------------------------------------------------------------------
    # SQL Table Definition
    # ----------------------------------------------------------------------------------------------
    def create_table_sql(self) -> str:
        """
        Generate CREATE TABLE SQL based on the schema.
        """
        user_columns_sql = []
        for col, col_type in self.channel_schema.user_columns.items():
            user_columns_sql.append(f"{col} {col_type.db_type}{'' if col_type.optional else ' NOT NULL' }")

        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            row_id BIGSERIAL PRIMARY KEY,
            time TIMESTAMPTZ NOT NULL,
            {', '.join(user_columns_sql)},
            received_at TIMESTAMPTZ NOT NULL
        );
        """.strip()

    # ----------------------------------------------------------------------------------------------
    # DB operations
    # ----------------------------------------------------------------------------------------------
    async def table_exists(self, conn: PoolConnectionProxy) -> bool:
        """Return True if the ChannelMetadata table exists."""
        return await table_exists(conn, namespace=self._NAMESPACE, table_name=self.table_name)

    async def ensure_table(self, conn: PoolConnectionProxy):
        """Ensure the table exists in the database."""
        here = f"{mod}.ensure_table"
        if await self.table_exists(conn):
            return
        await conn.execute(self.create_table_sql())
        log_d(here, "Table created:", self.table_name)

    async def count_rows(self, conn: PoolConnectionProxy) -> int:
        """
        Return the total number of rows in the channel's timeseries table.
        """
        if not await self.table_exists(conn):
            return 0
        query = f'SELECT COUNT(*) FROM "{self.table_name}"'
        result = await conn.fetchval(query)
        return result or 0

    async def fetch(self, conn: PoolConnectionProxy, *, filter: RequestFilter | None = None) -> ChannelTimeseries:
        """
        Fetch rows from the timeseries table for this channel.

        Parameters
        ----------
        conn : asyncpg.Connection
            Connection to the database
        filter : RequestFilter | None
            Optional filter with from_date, to_date, limit, offset, skip_received

        Returns
        -------
        self : ChannelTimeseries
            Instance with rows populated from DB
        """
        here = f"{mod}.fetch_rows"

        if not await self.table_exists(conn):
            log_w(here, f"Table '{self.table_name}' not found, returning empty list")
            self.clear_rows()
            return self

        filter = filter or RequestFilter()
        sql_fragment, params = filter.to_sql_clauses(start_idx=1)

        sql_req = f'SELECT * FROM "{self.table_name}" {sql_fragment} ORDER BY time ASC'
        rows = await conn.fetch(sql_req, *params)
        self.set_rows([dict(r) for r in rows])
        return self

    async def insert(
        self, conn: PoolConnectionProxy, *, strict: bool = False, warnings: dict[str, str] | None = None
    ) -> ChannelTimeseries:
        """
        Insert all rows currently held in the ChannelTimeseries into the DB.

        Parameters
        ----------
        conn : asyncpg.Connection
        strict : bool
            If True, aborts insertion if any row fails validation
        warnings : dict | None
            Optional dictionary to collect per-row validation warnings

        Returns
        -------
        self : ChannelTimeseries
            Instance with successfully inserted rows
        """
        here = f"{mod}.insert"

        if not self._rows:
            return self

        warnings = warnings or {}
        pad_width = len(str(len(self.rows)))

        tuples = self.get_db_tuples()
        cols = self._sql_insert_columns
        placeholders = [f"${i+1}" for i in range(len(cols))]

        sql = f"""
        INSERT INTO "{self.table_name}" ({', '.join(cols)})
        VALUES ({', '.join(placeholders)});
        """

        await self.ensure_table(conn)

        for idx, row_tuple in enumerate(tuples):
            try:
                await conn.execute(sql, *row_tuple)
            except Exception as e:
                warnings[f"row_{str(idx).zfill(pad_width)}"] = str(e)

        if strict and warnings:
            log_w(here, "Failed to insert some rows", {"channel_id": str(self.channel_id), **warnings})
            raise BadRequestError(
                "Failed to insert some rows", details={"channel_id": str(self.channel_id), **warnings}
            )
        return self

    async def delete(self, conn: PoolConnectionProxy, *, filter: RequestFilter | None = None) -> ChannelTimeseries:
        """
        Delete rows in the timeseries table based on filter.

        Parameters
        ----------
        conn : asyncpg.Connection
        filter : RequestFilter | None

        Returns
        -------
        self : ChannelTimeseries
            Instance containing the rows that were deleted
        """
        here = f"{mod}.delete"

        if not await self.table_exists(conn):
            log_w(here, f"Table '{self.table_name}' not found, cannot delete")
            raise NotFoundError(f"No timeseries stored for channel '{self.channel_id}'")

        filter = filter or RequestFilter()
        sql_fragment, params = filter.to_sql_clauses(start_idx=1)

        sql_req = f'DELETE FROM "{self.table_name}" {sql_fragment} RETURNING *'
        rows = await conn.fetch(sql_req, *params)
        self.set_rows([dict(r) for r in rows])
        return self

    async def drop(self, conn: PoolConnectionProxy) -> ChannelTimeseries | None:
        """
        Drop the entire timeseries table for this channel.

        Warning:
            This permanently deletes all rows and the table itself.
        """
        here = f"{mod}.drop"
        if not await self.table_exists(conn):
            log_w(here, f"Table '{self.table_name}' not found, nothing to drop")
            return
        sql_fetch = f'SELECT * FROM "{self.table_name}"'
        rows = await conn.fetch(sql_fetch)
        rows_data = [dict(r) for r in rows]

        sql_drop = f'DROP TABLE "{self.table_name}"'
        await conn.execute(sql_drop)
        log_i(here, f"Table '{self.table_name}' dropped, {len(rows_data)} rows were removed")
        self.clear_rows()
        if rows_data:
            self.add_rows(rows_data)
        return self
