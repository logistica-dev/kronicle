# kronicle/db/core/models/channel.py

from kronicle.db.core.models.core_entity import CoreEntity


class Channel(CoreEntity):
    __tablename__ = "channel"


if __name__ == "__main__":  # pragma: no cover
    from sqlalchemy import create_engine

    from kronicle.deps.settings import get_settings

    print(Channel.namespace())  # prints core => OK
    try:
        Channel.namespace = "testsing"  # type: ignore
    except AttributeError:
        print("Caught AttributeError: OK")
    print(Channel.namespace())  # prints core => OK
    print(Channel.table_args())  # prints {'schema': 'core'} => OK
    conf = get_settings(".conf/config.ini")
    engine = create_engine(conf.rbac.connection_url, future=True)

    Channel.validate_table(engine)
