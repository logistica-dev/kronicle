# tests/integration/data/channel_reader.py

from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_reader import KronicleReader
from kronicle_sdk.utils.log import log_d

if __name__ == "__main__":

    here = "read Kronicle"
    log_d(here)
    co = Settings().connection
    kronicle_reader = KronicleReader(co.url, co.usr, co.pwd)
    log_d(here, "is_alive", kronicle_reader.is_alive())
    log_d(here, "is_ready", kronicle_reader.is_ready())
    log_d(here, "nb channels", len(kronicle_reader.all_channels))
    chan_id, _ = kronicle_reader.get_channel_with_max_rows()
    if chan_id:
        log_d(here, "channel with max rows", kronicle_reader.get_channel(chan_id))

    # try:
    #     id = uuid4()
    #     log_d(here, "random channel", kronicle_reader.get_channel(id))
    # except KronicleHTTPError as e:
    #     log_w(here, f"Channel {id}", e)
