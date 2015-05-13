import asyncio
import copy
import os
import unittest
from functools import wraps

import aiomysql
from aiomysql_replication import create_binlog_stream


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), 15, loop=loop))
        return ret

    return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        del self.loop


class ReplicationTestCase(BaseTest):
    def ignoredEvents(self):
        return []

    def setUp(self):
        super().setUp()
        self.database = {
            "host": "localhost",
            "user": "root",
            "password": "",
            "use_unicode": True,
            "charset": "utf8",
            "db": "pymysqlreplication_test",
            "loop": self.loop,
        }
        if os.getenv("TRAVIS") is not None:
            self.database["user"] = "travis"

        self.conn_control = None
        db = copy.copy(self.database)
        db["db"] = None
        db['loop'] = self.loop
        self.loop.run_until_complete(self._prepare(db))

    @asyncio.coroutine
    def _prepare(self, db):
        yield from self.connect_conn_control(db)
        yield from self.execute("DROP DATABASE IF EXISTS "
                                "pymysqlreplication_test")
        yield from self.execute("CREATE DATABASE pymysqlreplication_test")
        db = copy.copy(self.database)
        yield from self.connect_conn_control(db)
        self.stream = None
        yield from self.resetBinLog()
        yield from self.isMySQL56AndMore()

    @asyncio.coroutine
    def getMySQLVersion(self):
        """Return the MySQL version of the server
        If version is 5.6.10-log the result is 5.6.10
        """
        cursor = yield from self.execute("SELECT VERSION()")
        data = yield from cursor.fetchone()
        return data[0].split('-')[0]

    @asyncio.coroutine
    def isMySQL56AndMore(self):
        d = yield from self.getMySQLVersion()
        version = float(d.rsplit('.', 1)[0])
        if version >= 5.6:
            return True
        return False

    @asyncio.coroutine
    def connect_conn_control(self, db):
        if self.conn_control is not None:
            self.conn_control.close()
        self.conn_control = yield from aiomysql.connect(**db)

    def tearDown(self):
        self.conn_control.close()
        self.conn_control = None
        self.stream.close()
        self.stream = None

    @asyncio.coroutine
    def execute(self, query):
        c = yield from self.conn_control.cursor()
        yield from c.execute(query)
        return c

    @asyncio.coroutine
    def resetBinLog(self):
        yield from self.execute("RESET MASTER")
        if self.stream is not None:
            self.stream.close()
        self.stream = yield from create_binlog_stream(
            self.database, server_id=1024, ignored_events=self.ignoredEvents(),
            loop=self.loop)
