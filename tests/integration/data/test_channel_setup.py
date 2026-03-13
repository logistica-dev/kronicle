# tests/integration/data/channel_setup.py

from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.models.iso_datetime import now_local
from kronicle_sdk.utils.log import log_d, log_w
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str

if __name__ == "__main__":
    from kronicle_sdk.utils.log import log_d

    here = "ksetup"
    log_d(here)
    co = Settings().connection
    kronicle_setup = KronicleSetup(co.url, co.usr, co.pwd)
    log_d(here, "Channel list vvv")
    [log_d(here, f"channel {channel.channel_id}", channel) for channel in kronicle_setup.all_channels]
    log_d(here, "Channel list ^^^")

    max_chan_id, _ = kronicle_setup.get_channel_with_max_rows()
    if max_chan_id:
        log_d(here, "channel with max rows", kronicle_setup.get_channel(max_chan_id))
        rows: list = kronicle_setup.get_rows_for_channel(max_chan_id, "dict")  # type: ignore
        for i, row in enumerate(rows):
            log_d(here, f"row {i}", row)
        log_d(here, "nb rows", len(rows))

    channel_id = uuid4_str()
    channel_name = f"demo_channel_{tiny_id()}"

    now_tag = now_local()

    payload = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_schema": {"time": "datetime", "temperature": "float"},
        "metadata": {"unit": "°C"},
        "tags": {"test": now_tag},
        "rows": [
            {"time": "2025-01-10T00:00:00Z", "temperature": 12.3},
            {"time": "2025-01-10T00:01:00Z", "temperature": 12.8},
        ],
    }
    log_d(here, "payload", payload)
    result = kronicle_setup.insert_rows_and_upsert_channel(payload)
    log_d(here, "result", result)
    log_d(here, "column types", kronicle_setup.column_types)
    try:
        kronicle_setup.get(route="route/that/does/not/exist", strict=False)
    except Exception as e:
        log_w(here, "OK, exception caught:", e)
