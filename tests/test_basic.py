from tests import base
from aiomysql_replication import create_binlog_stream
from aiomysql_replication.event import *
from aiomysql_replication.row_event import *
from tests.base import run_until_complete


class TestBasicBinLogStreamReader(base.ReplicationTestCase):
    def ignoredEvents(self):
        return [GtidEvent]

    def test_allowed_event_list(self):

        self.assertEqual(
            len(self.stream._allowed_event_list(None, None, False)), 11)
        self.assertEqual(
            len(self.stream._allowed_event_list(None, None, True)), 10)
        self.assertEqual(
            len(self.stream._allowed_event_list(None, [RotateEvent], False)),
            10)
        self.assertEqual(
            len(self.stream._allowed_event_list([RotateEvent], None, False)),
            1)

    @run_until_complete
    def test_read_query_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        event = yield from self.stream.fetchone()
        self.assertEqual(event.position, 4)
        self.assertEqual(event.next_binlog, "mysql-bin.000001")
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

    @run_until_complete
    def test_read_query_event_with_unicode(self):
        query = "CREATE TABLE `testÈ` (id INT NOT NULL AUTO_INCREMENT, " \
                "dataÈ VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        event = yield from self.stream.fetchone()
        self.assertEqual(event.position, 4)
        self.assertEqual(event.next_binlog, "mysql-bin.000001")
        self.assertIsInstance(event, RotateEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

    @run_until_complete
    def test_reading_rotate_event(self):
        query = "CREATE TABLE test_2 (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        self.stream.close()

        query = "CREATE TABLE test_3 (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        # Rotate event
        self.stream = yield from create_binlog_stream(
            self.database, server_id=1024, blocking=True,
            ignored_events=self.ignoredEvents(), loop=self.loop)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)

    @run_until_complete
    def test_connection_stream_lost_event(self):
        self.stream.close()
        self.stream = yield from create_binlog_stream(
            self.database, server_id=1024, blocking=True,
            ignored_events=self.ignoredEvents(), loop=self.loop)

        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query2 = "INSERT INTO test (data) VALUES('a')"
        for i in range(0, 100):
            yield from self.execute(query2)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

        self.conn_control.kill(self.stream._stream_connection.thread_id())
        for i in range(0, 10):
            event = yield from self.stream.fetchone()
            self.assertIsNotNone(event)

    @run_until_complete
    def test_filtering_only_events(self):
        self.stream.close()
        self.stream = yield from create_binlog_stream(
            self.database, server_id=1024, only_events=[QueryEvent],
            loop=self.loop)
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

    @run_until_complete
    def test_filtering_ignore_events(self):
        self.stream.close()
        self.stream = yield from create_binlog_stream(
            self.database, server_id=1024, ignored_events=[QueryEvent],
            loop=self.loop)
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)

    @run_until_complete
    def test_filtering_table_event(self):
        self.stream.close()
        self.stream = yield from create_binlog_stream(
            self.database,
            server_id=1024,
            only_events=[WriteRowsEvent],
            only_tables = ["test_2"], loop=self.loop)

        query = "CREATE TABLE test_2 (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "CREATE TABLE test_3 (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        yield from self.execute("INSERT INTO test_2 (data) VALUES ('alpha')")
        yield from self.execute("INSERT INTO test_3 (data) VALUES ('alpha')")
        yield from self.execute("INSERT INTO test_2 (data) VALUES ('beta')")
        yield from self.execute("COMMIT")
        event = yield from self.stream.fetchone()
        self.assertEqual(event.table, "test_2")
        event = yield from self.stream.fetchone()
        self.assertEqual(event.table, "test_2")

    @run_until_complete
    def test_write_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        #QueryEvent for the Create Table
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.WRITE_ROWS_EVENT_V2)
        # else:
        # self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V1)
        self.assertIsInstance(event, WriteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello World")
        self.assertEqual(event.schema, "pymysqlreplication_test")
        self.assertEqual(event.table, "test")
        self.assertEqual(event.columns[1].name, 'data')

    @run_until_complete
    def test_delete_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        yield from self.execute(query)

        yield from self.resetBinLog()

        query = "DELETE FROM test WHERE id = 1"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.DELETE_ROWS_EVENT_V2)
        # else:
        # self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V1)
        self.assertIsInstance(event, DeleteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello World")

    @run_until_complete
    def test_update_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        yield from self.execute(query)

        yield from self.resetBinLog()

        query = "UPDATE test SET data = 'World' WHERE id = 1"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.UPDATE_ROWS_EVENT_V2)
        # else:
        #     self.assertEqual(event.event_type, BinLog.UPDATE_ROWS_EVENT_V1)
        self.assertIsInstance(event, UpdateRowsEvent)
        self.assertEqual(event.rows[0]["before_values"]["id"], 1)
        self.assertEqual(event.rows[0]["before_values"]["data"], "Hello")
        self.assertEqual(event.rows[0]["after_values"]["id"], 1)
        self.assertEqual(event.rows[0]["after_values"]["data"], "World")

    @run_until_complete
    def test_minimal_image_write_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "SET SESSION binlog_row_image = 'minimal'"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        #QueryEvent for the Create Table
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.WRITE_ROWS_EVENT_V2)
        # else:
        #     self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V1)
        self.assertIsInstance(event, WriteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello World")
        self.assertEqual(event.schema, "pymysqlreplication_test")
        self.assertEqual(event.table, "test")
        self.assertEqual(event.columns[1].name, 'data')

    @run_until_complete
    def test_minimal_image_delete_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        yield from self.execute(query)
        query = "SET SESSION binlog_row_image = 'minimal'"
        yield from self.execute(query)
        yield from self.resetBinLog()

        query = "DELETE FROM test WHERE id = 1"
        yield from self.execute(query)
        yield from self.execute("COMMIT")
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.DELETE_ROWS_EVENT_V2)
        # else:
        #     self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V1)
        self.assertIsInstance(event, DeleteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], None)

    @run_until_complete
    def test_minimal_image_update_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        yield from self.execute(query)
        query = "SET SESSION binlog_row_image = 'minimal'"
        yield from self.execute(query)
        yield from self.resetBinLog()

        query = "UPDATE test SET data = 'World' WHERE id = 1"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.UPDATE_ROWS_EVENT_V2)
        # else:
        #     self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V1)
        self.assertIsInstance(event, UpdateRowsEvent)
        self.assertEqual(event.rows[0]["before_values"]["id"], 1)
        self.assertEqual(event.rows[0]["before_values"]["data"], None)
        self.assertEqual(event.rows[0]["after_values"]["id"], None)
        self.assertEqual(event.rows[0]["after_values"]["data"], "World")

    @run_until_complete
    def test_log_pos(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        for i in range(6):
            yield from self.stream.fetchone()
        # record position after insert
        log_file, log_pos = self.stream.log_file, self.stream.log_pos

        query = "UPDATE test SET data = 'World' WHERE id = 1"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        # resume stream from previous position
        if self.stream is not None:
            self.stream.close()
        self.stream = yield from create_binlog_stream(
            self.database,
            server_id=1024,
            resume_stream=True,
            log_file=log_file,
            log_pos=log_pos,
            ignored_events=self.ignoredEvents(),
            loop=self.loop
        )
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, XidEvent)
        # QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, UpdateRowsEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, XidEvent)

    @run_until_complete
    def test_log_pos_handles_disconnects(self):

        self.stream.close()
        self.stream = yield from create_binlog_stream(
            self.database,
            server_id=1024,
            resume_stream=False,
            only_events = [FormatDescriptionEvent, QueryEvent, TableMapEvent,
                           WriteRowsEvent, XidEvent],
            loop=self.loop
        )

        query = "CREATE TABLE test (id INT  PRIMARY KEY AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL)"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        self.assertGreater(self.stream.log_pos, 0)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, WriteRowsEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, XidEvent)

        self.assertGreater(self.stream.log_pos, 0)

class TestMultipleRowBinLogStreamReader(base.ReplicationTestCase):

    def ignoredEvents(self):
        return [GtidEvent]

    def test_insert_multiple_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)

        yield from self.resetBinLog()

        query = "INSERT INTO test (data) VALUES('Hello'),('World')"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.WRITE_ROWS_EVENT_V2)
        # else:
        #     self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V1)
        self.assertIsInstance(event, WriteRowsEvent)
        self.assertEqual(len(event.rows), 2)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello")

        self.assertEqual(event.rows[1]["values"]["id"], 2)
        self.assertEqual(event.rows[1]["values"]["data"], "World")

    @run_until_complete
    def test_update_multiple_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('World')"
        yield from self.execute(query)

        yield from self.resetBinLog()

        query = "UPDATE test SET data = 'Toto'"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.UPDATE_ROWS_EVENT_V2)
        # else:
        #     self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V1)
        self.assertIsInstance(event, UpdateRowsEvent)
        self.assertEqual(len(event.rows), 2)
        self.assertEqual(event.rows[0]["before_values"]["id"], 1)
        self.assertEqual(event.rows[0]["before_values"]["data"], "Hello")
        self.assertEqual(event.rows[0]["after_values"]["id"], 1)
        self.assertEqual(event.rows[0]["after_values"]["data"], "Toto")

        self.assertEqual(event.rows[1]["before_values"]["id"], 2)
        self.assertEqual(event.rows[1]["before_values"]["data"], "World")
        self.assertEqual(event.rows[1]["after_values"]["id"], 2)
        self.assertEqual(event.rows[1]["after_values"]["data"], "Toto")

    @run_until_complete
    def test_delete_multiple_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, " \
                "data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        yield from self.execute(query)
        query = "INSERT INTO test (data) VALUES('World')"
        yield from self.execute(query)

        yield from self.resetBinLog()

        query = "DELETE FROM test"
        yield from self.execute(query)
        yield from self.execute("COMMIT")

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        #QueryEvent for the BEGIN
        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)

        event = yield from self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = yield from self.stream.fetchone()
        # if self.isMySQL56AndMore():
        self.assertEqual(event.event_type, BinLog.DELETE_ROWS_EVENT_V2)
        # else:
        #     self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V1)
        self.assertIsInstance(event, DeleteRowsEvent)
        self.assertEqual(len(event.rows), 2)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello")

        self.assertEqual(event.rows[1]["values"]["id"], 2)
        self.assertEqual(event.rows[1]["values"]["data"], "World")
#
#
# class TestGtidBinLogStreamReader(base.PyMySQLReplicationTestCase):
#     def test_read_query_event(self):
#         query = "CREATE TABLE test (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
#         yield from self.execute(query)
#         query = "SELECT @@global.gtid_executed;"
#         gtid = yield from self.execute(query).fetchone()[0]
#
#         self.stream.close()
#         self.stream = yield from create_binlog_stream(
#             self.database, server_id=1024, blocking=True, auto_position=gtid)
#
#         self.assertIsInstance(self.stream.fetchone(), RotateEvent)
#         self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
#
#         # Insert first event
#         query = "BEGIN;"
#         yield from self.execute(query)
#         query = "INSERT INTO test (id, data) VALUES(1, 'Hello');"
#         yield from self.execute(query)
#         query = "COMMIT;"
#         yield from self.execute(query)
#
#         firstevent = self.stream.fetchone()
#         self.assertIsInstance(firstevent, GtidEvent)
#
#         self.assertIsInstance(self.stream.fetchone(), QueryEvent)
#         self.assertIsInstance(self.stream.fetchone(), TableMapEvent)
#         self.assertIsInstance(self.stream.fetchone(), WriteRowsEvent)
#         self.assertIsInstance(self.stream.fetchone(), XidEvent)
#
#         # Insert second event
#         query = "BEGIN;"
#         yield from self.execute(query)
#         query = "INSERT INTO test (id, data) VALUES(2, 'Hello');"
#         yield from self.execute(query)
#         query = "COMMIT;"
#         yield from self.execute(query)
#
#         secondevent = self.stream.fetchone()
#         self.assertIsInstance(secondevent, GtidEvent)
#
#         self.assertIsInstance(self.stream.fetchone(), QueryEvent)
#         self.assertIsInstance(self.stream.fetchone(), TableMapEvent)
#         self.assertIsInstance(self.stream.fetchone(), WriteRowsEvent)
#         self.assertIsInstance(self.stream.fetchone(), XidEvent)
#
#         self.assertEqual(secondevent.gno, firstevent.gno + 1)
#
#     def test_position_gtid(self):
#         query = "CREATE TABLE test (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
#         yield from self.execute(query)
#         query = "BEGIN;"
#         yield from self.execute(query)
#         query = "INSERT INTO test (id, data) VALUES(1, 'Hello');"
#         yield from self.execute(query)
#         query = "COMMIT;"
#         yield from self.execute(query)
#
#         query = "CREATE TABLE test2 (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
#         yield from self.execute(query)
#         query = "SELECT @@global.gtid_executed;"
#         gtid = yield from self.execute(query).fetchone()[0]
#
#         self.stream.close()
#         self.stream = yield from create_binlog_stream(
#             self.database, server_id=1024, blocking=True, auto_position=gtid)
#
#         self.assertIsInstance(self.stream.fetchone(), RotateEvent)
#         self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
#         self.assertIsInstance(self.stream.fetchone(), GtidEvent)
#         event = self.stream.fetchone()
#
#         self.assertEqual(event.query, 'CREATE TABLE test2 (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))');
