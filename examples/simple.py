import asyncio
from aiomysql_replication import create_binlog_stream

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": ""
}

loop = asyncio.get_event_loop()


@asyncio.coroutine
def test_example():
    stream = yield from create_binlog_stream(connection_settings=MYSQL_SETTINGS,
                            server_id=3,
                            blocking=True, loop=loop)
    while True:
        event = yield from stream.fetchone()
        event.dump()
    stream.close()


loop.run_until_complete(test_example())
