# tests/integration/data/channel_writer.py

from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_writer import KronicleWriter
from kronicle_sdk.models.iso_datetime import IsoDateTime

if __name__ == "__main__":
    from kronicle_sdk.models.iso_datetime import now_local
    from kronicle_sdk.utils.log import log_d
    from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str

    here = "KronicleWriter"
    log_d(here)
    co = Settings().connection
    kronicle_writer = KronicleWriter(co.url, co.usr, co.pwd)
    [log_d(here, f"Channel {channel.channel_id}", channel) for channel in kronicle_writer.all_channels]
    max_chan_id, _ = kronicle_writer.get_channel_with_max_rows()
    if max_chan_id:
        log_d(here, "channel with max rows", kronicle_writer.get_channel(max_chan_id))
        rows: list = kronicle_writer.get_rows_for_channel(max_chan_id, "dict")  # type: ignore
        for i, row in enumerate(rows):
            log_d(here, f"row {i}", row)
        log_d(here, "nb rows", len(rows))

    channel_id = uuid4_str()
    channel_name = f"demo_channel_{tiny_id()}"
    now_tag = now_local()

    payload = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_schema": {"time": IsoDateTime, "temperature": float},
        "metadata": {"unit": "°C"},
        "tags": {"test": now_tag},
        "rows": [
            {"time": "2025-01-01T00:00:00Z", "temperature": 12.3},
            {"time": "2025-01-01T00:01:00Z", "temperature": 12.8},
        ],
    }
    log_d(here, "payload", payload)
    result = kronicle_writer.insert_rows_and_upsert_channel(payload)
    log_d(here, "result", result)
