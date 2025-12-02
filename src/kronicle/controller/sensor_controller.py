# kronicle/controller/sensor_controller.py

from typing import Any
from uuid import UUID, uuid4

from kronicle.controller.db_wrapper import DatabaseWrapper
from kronicle.controller.input_payloads import InputPayload
from kronicle.controller.processed_payload import ProcessedMetadata, ProcessedPayload
from kronicle.controller.response_payload import ResponsePayload
from kronicle.db.sensor_metadata import SensorMetadata
from kronicle.db.sensor_schema import SensorSchema
from kronicle.types.errors import BadRequestError, NotFoundError
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dev_logs import log_d
from kronicle.utils.str_utils import ensure_uuid4


class SensorController:
    """
    Orchestration layer between the user/FASTAPI layer and the DB layer:
    - sanitization & validation
    - CRUD operations
    - Atomic operations

    Controller layer to orchestrate DB operations.
    - Rows can only be inserted (append-only)
    - Metadata is always upserted (create or update)
    - All inputs validated via Payload classes

    The pipeline InputPayload → ProcessedPayload → SensorMetadata → ResponsePayload should stay coherent to avoid
    shortcutting the processed stage.

    Controller is responsible for presenting the data to the user according to the API defined.
    """

    def __init__(self, db: DatabaseWrapper):
        self._db = db

    async def ping(self) -> bool:
        return await self._db.direct_ping()  # type: ignore[attr-defined]

    # ----------------------------------------------------------------------------------------------
    # Metadata CRUD
    # ----------------------------------------------------------------------------------------------
    async def create_metadata(self, payload: InputPayload) -> ResponsePayload:
        """
        Create new sensor metadata (fail if already exists).
        """
        sensor_id = payload.ensure_sensor_id()

        # Fail if metadata already exists
        await self._db.ensure_no_metadata(sensor_id)

        schema: SensorSchema = payload.ensure_sensor_schema()

        processed = ProcessedMetadata.from_input(payload, schema)
        metadata = processed.to_db_metadata()
        await self._db.insert_or_update_metadata(metadata)
        return ResponsePayload.from_metadata(metadata)

    async def _prepare_metadata_for_upsert(self, payload: InputPayload, with_rows: bool = False):
        """
        Shared logic for upserting metadata or validating existing schema.

        Args:
            payload: InputPayload provided by the user.
            with_rows:
                - False (default): only process metadata, returns a ProcessedMetadata instance.
                - True: process metadata and rows, validates rows, returns a ProcessedPayload instance.

        Behavior:
            - If the sensor already exists:
                - Validates provided schema against existing one (if any).
            - If the sensor does not exist:
                - Requires a schema to create a new sensor.
            - When with_rows=True:
                - Rows are validated against the schema.
                - Warnings or errors are captured in the ProcessedPayload.op_status/op_details.

        Returns:
            Tuple of (processed_payload, SensorMetadata)
            - processed_payload: ProcessedMetadata or ProcessedPayload depending on `with_rows`.
            - SensorMetadata: ready for insertion/upsert in the DB.
        """
        here = "_prepare_metadata_for_upsert"
        log_d(here)

        sensor_id: UUID = payload.ensure_sensor_id()
        log_d(here, "sensor_id", sensor_id)
        existing: SensorMetadata | None = await self._db.fetch_metadata(sensor_id)
        log_d(here, "existing meta", existing)

        if existing:
            # Validate schema if provided
            if payload.sensor_schema:
                log_d(here, "payload.sensor_schema", payload.sensor_schema)
                input_schema = SensorSchema.from_user_json(payload.sensor_schema)
                log_d(here, "input_schema", input_schema)

                if not existing.sensor_schema.equivalent_to(input_schema):
                    raise BadRequestError(
                        "Schema mismatch",
                        details={
                            "sensor_id": str(sensor_id),
                            "provided_schema": payload.sensor_schema,
                            "existing_schema": existing.sensor_schema.to_user_json(),
                        },
                    )
            schema = existing.sensor_schema
        else:
            schema = payload.ensure_sensor_schema()
        log_d(here, "schema", schema)

        if with_rows:
            processed = ProcessedPayload.from_input(payload, schema, strict=payload.strict)
        else:
            processed = ProcessedMetadata.from_input(payload, schema)
        log_d(here, "processed", processed)

        meta = processed.to_db_metadata()
        log_d(here, "meta", meta)

        return processed, meta

    async def upsert_metadata(self, payload: InputPayload) -> ResponsePayload:
        """
        Create or update sensor metadata (idempotent).
        - If sensor does not exist -> require schema (create new).
        - If sensor exists -> schema must either be absent or identical to stored one.
        """
        _, meta = await self._prepare_metadata_for_upsert(payload)
        await self._db.insert_or_update_metadata(meta)
        return ResponsePayload.from_metadata(meta)

    async def fetch_metadata(self, sensor_id: UUID) -> ResponsePayload:
        """
        Fetch metadata for a given sensor_id.
        """
        sensor_id = ensure_uuid4(sensor_id)
        existing = await self._db.ensure_metadata(sensor_id)

        # Count rows for this sensor
        row_count = await self._db.count_sensor_rows(existing)
        return ResponsePayload.from_metadata(existing, available_rows=row_count)

    async def fetch_all_metadata(self) -> list[ResponsePayload]:
        """
        Fetch all metadata entries and row counts.
        """
        all_meta = await self._db.fetch_all_metadata()
        response_list: list[ResponsePayload] = []
        for meta in all_meta:
            count = await self._db.count_sensor_rows(meta)
            response_list.append(ResponsePayload.from_metadata(meta, available_rows=count))
        return response_list

    async def fetch_metadata_by_name(self, name: str) -> list[ResponsePayload]:
        """
        Fetch all metadata entries and row counts.
        """
        all_meta = await self._db.fetch_metadata_by_name(name=name)
        response_list: list[ResponsePayload] = []
        for meta in all_meta:
            count = await self._db.count_sensor_rows(meta)
            response_list.append(ResponsePayload.from_metadata(meta, available_rows=count))
        return response_list

    async def fetch_metadata_by_tag(self, tag_key: str, tag_value: Any) -> list[ResponsePayload]:
        """
        Fetch all metadata entries and row counts.
        """
        all_meta = await self._db.fetch_metadata_by_tag(tag_key=tag_key, tag_value=tag_value)
        response_list: list[ResponsePayload] = []
        for meta in all_meta:
            count = await self._db.count_sensor_rows(meta)
            response_list.append(ResponsePayload.from_metadata(meta, available_rows=count))
        return response_list

    async def delete_sensor(self, sensor_id: UUID) -> ResponsePayload:
        """
        Delete a sensor metadata and its data table.
        """
        sensor_id = ensure_uuid4(sensor_id)
        existing = await self._db.ensure_metadata(sensor_id)
        deleted = await self._db.delete_metadata_and_table(sensor_id, drop_table=True)
        if not deleted:
            raise NotFoundError("Sensor not found", details={"sensor_id": str(sensor_id)})
        return ResponsePayload.from_metadata(existing)

    async def delete_sensors(self, sensor_ids: list[UUID]) -> list[ResponsePayload]:
        res = []
        for sensor_id in sensor_ids:
            res.append(self.delete_sensor(sensor_id))
        return res

    # ----------------------------------------------------------------------------------------------
    # Row operations
    # ----------------------------------------------------------------------------------------------
    async def insert_sensor_rows(self, payload: InputPayload, *, strict: bool = False) -> ResponsePayload:
        """
        Insert new rows for an existing sensor.
        Validates rows first; in strict mode, aborts on any validation errors.
        """
        here = "ctrl.insert_sensor_rows"
        log_d(here)
        sensor_id: UUID = payload.ensure_sensor_id()
        existing = await self._db.ensure_metadata(sensor_id)

        # Process input payload into ProcessedPayload
        log_d(here, "existing", existing)

        processed = ProcessedPayload.from_input(payload=payload, schema=existing.sensor_schema, strict=strict)
        log_d(here, "from_input", processed.rows)
        if processed.rows:
            # Insert validated rows
            await self._db.insert_sensor_rows(metadata=existing, rows=processed.rows)

        # Build response
        return ResponsePayload.from_processing_and_insertion(processed, existing)

    async def upsert_metadata_and_insert_rows(self, payload: InputPayload, strict: bool = False) -> ResponsePayload:
        """
        Upsert metadata and insert rows in one operation.
        - Metadata is validated first; if invalid, no rows are inserted.
        - Rows are validated; warnings collected if strict=False, error raised with every detail otherwise
        """
        here = "insert_rows"
        log_d(here)
        # Prepare metadata & processed payload
        processed, meta = await self._prepare_metadata_for_upsert(payload, with_rows=True)
        log_d(here, "meta", meta)
        assert isinstance(processed, ProcessedPayload)  # Always OK
        log_d(here, "processed", processed)

        # Insert rows after metadata upsert
        await self._db.insert_or_update_metadata(meta)
        log_d(here, "insert_or_update_metadata", "done")

        await self._db.insert_sensor_rows(meta, processed.rows)  # TODO: retrieve DB insertion errors!
        log_d(here, "insert_sensor_rows", "done")

        res = ResponsePayload.from_processing_and_insertion(processed, meta)
        log_d(here, "op_status", res.op_status)
        return res

    async def fetch_rows(
        self,
        sensor_id: UUID,
        *,
        from_date: str | IsoDateTime | None = None,
        to_date: str | IsoDateTime | None = None,
        skip_received: bool = True,
    ) -> ResponsePayload:
        """
        Fetch rows for a given sensor.
        """
        # here = "fetch_rows"
        from_date = None if from_date is None else IsoDateTime.normalize_value(from_date)
        to_date = None if to_date is None else IsoDateTime.normalize_value(to_date)

        ensure_uuid4(sensor_id)
        existing = await self._db.ensure_metadata(sensor_id)

        rows = await self._db.fetch_sensor_rows(existing, from_date=from_date, to_date=to_date)
        return ResponsePayload.from_db_data(existing, rows, skip_received=skip_received)

    async def fetch_all_rows(self) -> list[ResponsePayload]:
        """
        Fetch all rows for all sensors.
        Returns a list of ResponsePayload objects.
        Each payload includes sensor_id, metadata, tags, rows, and issued_at.
        """
        all_meta = await self._db.fetch_all_metadata()
        response_list: list[ResponsePayload] = []
        for meta in all_meta:
            rows = await self._db.fetch_sensor_rows(meta)  # rows as list[dict]
            response_list.append(ResponsePayload.from_db_data(meta, rows))
        return response_list

    async def delete_all_rows_for_sensor(self, sensor_id: UUID) -> ResponsePayload:
        """
        Remove all data rows for a sensor, keep its metadata, and return the metadata
        with available_rows set to 0.
        """
        sensor_id = ensure_uuid4(sensor_id)
        existing = await self._db.ensure_metadata(sensor_id)

        await self._db.delete_all_rows_for_sensor(sensor_id)
        # we return the metadata and explicitly state rows are now 0 (available_rows=0)
        return ResponsePayload.from_metadata(existing, available_rows=0)

    async def fetch_columns(
        self,
        sensor_id: UUID,
        *,
        from_date: str | IsoDateTime | None = None,
        to_date: str | IsoDateTime | None = None,
    ) -> ResponsePayload:
        """
        Fetch rows for a given sensor.
        """
        res = await self.fetch_rows(sensor_id=sensor_id, from_date=from_date, to_date=to_date)
        if not res or not res.rows:
            return res
        res.rows_to_columns(strict=True)
        return res

    # ----------------------------------------------------------------------------------------------
    # Optional setup operations
    # ----------------------------------------------------------------------------------------------

    async def clone_channel(self, source_sensor_id: UUID, payload: InputPayload) -> ResponsePayload:
        """
        Clone a channel's schema (and optionally metadata) into a new sensor_id.
        - Does NOT copy data rows.
        - Generates a new sensor_id automatically.
        - Metadata/tags from payload override source if provided.
        """
        source_sensor_id = ensure_uuid4(source_sensor_id)
        existing = await self._db.ensure_metadata(source_sensor_id)

        # Generate new sensor_id automatically and assign to payload
        payload.sensor_id = uuid4()

        if payload.sensor_schema:
            sensor_schema = SensorSchema.from_user_json(payload.sensor_schema)
        else:
            sensor_schema = existing.sensor_schema

        if not payload.name and existing.sensor_name:
            payload.name = existing.sensor_name + "_copy"
        if not payload.metadata:
            payload.metadata = existing.metadata
        if not payload.tags:
            payload.tags = existing.tags

        # Process the payload before any DB action (validate metadata, tags, rows)
        processed = ProcessedMetadata.from_input(payload, sensor_schema)
        new_meta = processed.to_db_metadata()
        await self._db.insert_or_update_metadata(new_meta)
        return ResponsePayload.from_metadata(new_meta)

    async def patch_metadata(self, payload: InputPayload) -> ResponsePayload:
        """
        Partially update metadata, tags, or schema for an existing sensor.
        - Only provided fields are updated.
        """
        sensor_id = ensure_uuid4(payload.sensor_id)
        existing = await self._db.ensure_metadata(sensor_id)

        # Update metadata fields if provided
        if payload.metadata and existing.metadata:
            existing.metadata.update(payload.metadata)

        if payload.tags and existing.tags:
            existing.tags.update(payload.tags)

        # Update schema if provided
        if payload.sensor_schema:
            input_schema = SensorSchema.from_user_json(payload.sensor_schema)
            if input_schema != existing.sensor_schema:
                existing.sensor_schema = input_schema

        await self._db.insert_or_update_metadata(existing)
        return ResponsePayload.from_metadata(existing)
