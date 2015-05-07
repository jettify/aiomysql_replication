# -*- coding: utf-8 -*-

import pymysql
import struct

from pymysql.constants.COMMAND import COM_BINLOG_DUMP
from pymysql.cursors import DictCursor
from pymysql.util import int2byte

from .packet import BinLogPacketWrapper
from .constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT, STOP_EVENT
from .gtid import GtidSet
from .event import (
    QueryEvent, RotateEvent, FormatDescriptionEvent,
    XidEvent, GtidEvent, StopEvent, NotImplementedEvent)
from .row_event import (
    UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent, TableMapEvent)

try:
    from pymysql.constants.COMMAND import COM_BINLOG_DUMP_GTID
except ImportError:
    # Handle old pymysql versions
    # See: https://github.com/PyMySQL/PyMySQL/pull/261
    COM_BINLOG_DUMP_GTID = 0x1e

# 2013 Connection Lost
# 2006 MySQL server has gone away
MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]


class BinLogStreamReader(object):
    """Connect to replication stream and read event
    """

    def __init__(self, connection_settings, server_id, resume_stream=False,
                 blocking=False, only_events=None, log_file=None, log_pos=None,
                 filter_non_implemented_events=True,
                 ignored_events=None, auto_position=None,
                 only_tables=None, only_schemas=None,
                 freeze_schema=False):
        """
        Attributes:
            resume_stream: Start for event from position or the latest event of
                           binlog or from older available event
            blocking: Read on stream is blocking
            only_events: Array of allowed events
            ignored_events: Array of ignored events
            log_file: Set replication start log file
            log_pos: Set replication start log pos
            auto_position: Use master_auto_position gtid to set position
            only_tables: An array with the tables you want to watch
            only_schemas: An array with the schemas you want to watch
            freeze_schema: If true do not support ALTER TABLE. It's faster.
        """
        self.__connection_settings = connection_settings
        self.__connection_settings["charset"] = "utf8"

        self.__connected_stream = False
        self.__connected_ctl = False
        self.__resume_stream = resume_stream
        self.__blocking = blocking

        self.__only_tables = only_tables
        self.__only_schemas = only_schemas
        self.__freeze_schema = freeze_schema
        self.__allowed_events = self._allowed_event_list(
            only_events, ignored_events, filter_non_implemented_events)

        # We can't filter on packet level TABLE_MAP and rotate event because
        # we need them for handling other operations
        self.__allowed_events_in_packet = frozenset(
            [TableMapEvent, RotateEvent]).union(self.__allowed_events)

        self.__server_id = server_id
        self.__use_checksum = False

        # Store table meta information
        self.table_map = {}
        self.log_pos = log_pos
        self.log_file = log_file
        self.auto_position = auto_position

    def close(self):
        if self.__connected_stream:
            self._stream_connection.close()
            self.__connected_stream = False
        if self.__connected_ctl:
            # break reference cycle between stream reader and underlying
            # mysql connection object
            self._ctl_connection._get_table_information = None
            self._ctl_connection.close()
            self.__connected_ctl = False

    def __connect_to_ctl(self):
        self._ctl_connection_settings = dict(self.__connection_settings)
        self._ctl_connection_settings["db"] = "information_schema"
        self._ctl_connection_settings["cursorclass"] = DictCursor
        self._ctl_connection = pymysql.connect(**self._ctl_connection_settings)
        self._ctl_connection._get_table_information = self.__get_table_information
        self.__connected_ctl = True

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        cur = self._stream_connection.cursor()
        cur.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
        result = cur.fetchone()
        cur.close()

        if result is None:
            return False
        var, value = result[:2]
        if value == 'NONE':
            return False
        return True

    def __connect_to_stream(self):
        # log_pos (4) -- position in the binlog-file to start the stream with
        # flags (2) BINLOG_DUMP_NON_BLOCK (0 or 1)
        # server_id (4) -- server id of this slave
        # log_file (string.EOF) -- filename of the binlog on the master
        self._stream_connection = pymysql.connect(**self.__connection_settings)

        self.__use_checksum = self.__checksum_enabled()

        # If checksum is enabled we need to inform the server about the that
        # we support it
        if self.__use_checksum:
            cur = self._stream_connection.cursor()
            cur.execute("set @master_binlog_checksum= @@global.binlog_checksum")
            cur.close()

        if not self.auto_position:
            # only when log_file and log_pos both provided, the position info is
            # valid, if not, get the current position from master
            if self.log_file is None or self.log_pos is None:
                cur = self._stream_connection.cursor()
                cur.execute("SHOW MASTER STATUS")
                self.log_file, self.log_pos = cur.fetchone()[:2]
                cur.close()

            prelude = struct.pack('<i', len(self.log_file) + 11) \
                + int2byte(COM_BINLOG_DUMP)

            if self.__resume_stream:
                prelude += struct.pack('<I', self.log_pos)
            else:
                prelude += struct.pack('<I', 4)

            if self.__blocking:
                prelude += struct.pack('<h', 0)
            else:
                prelude += struct.pack('<h', 1)

            prelude += struct.pack('<I', self.__server_id)
            prelude += self.log_file.encode()
        else:
            # Format for mysql packet master_auto_position
            #
            # All fields are little endian
            # All fields are unsigned

            # Packet length   uint   4bytes
            # Packet type     byte   1byte   == 0x1e
            # Binlog flags    ushort 2bytes  == 0 (for retrocompatibilty)
            # Server id       uint   4bytes
            # binlognamesize  uint   4bytes
            # binlogname      str    Nbytes  N = binlognamesize
            #                                Zeroified
            # binlog position uint   4bytes  == 4
            # payload_size    uint   4bytes

            # What come next, is the payload, where the slave gtid_executed
            # is sent to the master
            # n_sid           ulong  8bytes  == which size is the gtid_set
            # | sid           uuid   16bytes UUID as a binary
            # | n_intervals   ulong  8bytes  == how many intervals are sent for this gtid
            # | | start       ulong  8bytes  Start position of this interval
            # | | stop        ulong  8bytes  Stop position of this interval

            # A gtid set looks like:
            #   19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10,
            #   1c2aad49-ae92-409a-b4df-d05a03e4702e:42-47:80-100:130-140
            #
            # In this particular gtid set, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10
            # is the first member of the set, it is called a gtid.
            # In this gtid, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457 is the sid
            # and have two intervals, 1-3 and 8-10, 1 is the start position of the first interval
            # 3 is the stop position of the first interval.

            gtid_set = GtidSet(self.auto_position)
            encoded_data_size = gtid_set.encoded_length

            header_size = (2 +  # binlog_flags
                           4 +  # server_id
                           4 +  # binlog_name_info_size
                           4 +  # empty binlog name
                           8 +  # binlog_pos_info_size
                           4)  # encoded_data_size

            prelude = b'' + struct.pack('<i', header_size + encoded_data_size) \
                + int2byte(COM_BINLOG_DUMP_GTID)

            # binlog_flags = 0 (2 bytes)
            prelude += struct.pack('<H', 0)
            # server_id (4 bytes)
            prelude += struct.pack('<I', self.__server_id)
            # binlog_name_info_size (4 bytes)
            prelude += struct.pack('<I', 3)
            # empty_binlog_name (4 bytes)
            prelude += b'\0\0\0'
            # binlog_pos_info (8 bytes)
            prelude += struct.pack('<Q', 4)

            # encoded_data_size (4 bytes)
            prelude += struct.pack('<I', gtid_set.encoded_length)
            # encoded_data
            prelude += gtid_set.encoded()

        if pymysql.__version__ < "0.6":
            self._stream_connection.wfile.write(prelude)
            self._stream_connection.wfile.flush()
        else:
            self._stream_connection._write_bytes(prelude)
        self.__connected_stream = True

    def fetchone(self):
        while True:
            if not self.__connected_stream:
                self.__connect_to_stream()

            if not self.__connected_ctl:
                self.__connect_to_ctl()

            try:
                if pymysql.__version__ < "0.6":
                    pkt = self._stream_connection.read_packet()
                else:
                    pkt = self._stream_connection._read_packet()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self.__connected_stream = False
                    continue

            if pkt.is_eof_packet():
                return None

            if not pkt.is_ok_packet():
                continue

            binlog_event = BinLogPacketWrapper(pkt, self.table_map,
                                               self._ctl_connection,
                                               self.__use_checksum,
                                               self.__allowed_events_in_packet,
                                               self.__only_tables,
                                               self.__only_schemas,
                                               self.__freeze_schema)

            if binlog_event.event_type == TABLE_MAP_EVENT and \
                    binlog_event.event is not None:
                self.table_map[binlog_event.event.table_id] = \
                    binlog_event.event.get_table()

            if binlog_event.event_type == ROTATE_EVENT:
                self.log_pos = binlog_event.event.position
                self.log_file = binlog_event.event.next_binlog
                # Table Id in binlog are NOT persistent in MySQL - they are in-memory identifiers
                # that means that when MySQL master restarts, it will reuse same table id for different tables
                # which will cause errors for us since our in-memory map will try to decode row data with
                # wrong table schema.
                # The fix is to rely on the fact that MySQL will also rotate to a new binlog file every time it
                # restarts. That means every rotation we see *could* be a sign of restart and so potentially
                # invalidates all our cached table id to schema mappings. This means we have to load them all
                # again for each logfile which is potentially wasted effort but we can't really do much better
                # without being broken in restart case
                self.table_map = {}
            elif binlog_event.log_pos:
                self.log_pos = binlog_event.log_pos

            # event is none if we have filter it on packet level
            # we filter also not allowed events
            if binlog_event.event is None or (binlog_event.event.__class__ not in self.__allowed_events):
                continue

            return binlog_event.event

    def _allowed_event_list(self, only_events, ignored_events,
                            filter_non_implemented_events):
        if only_events is not None:
            events = set(only_events)
        else:
            events = set((
                QueryEvent,
                RotateEvent,
                StopEvent,
                FormatDescriptionEvent,
                XidEvent,
                GtidEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                DeleteRowsEvent,
                TableMapEvent,
                NotImplementedEvent))
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)
        if filter_non_implemented_events:
            try:
                events.remove(NotImplementedEvent)
            except KeyError:
                pass
        return frozenset(events)

    def __get_table_information(self, schema, table):
        for i in range(1, 3):
            try:
                if not self.__connected_ctl:
                    self.__connect_to_ctl()

                cur = self._ctl_connection.cursor()
                cur.execute("""
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY
                    FROM
                        columns
                    WHERE
                        table_schema = %s AND table_name = %s
                    """, (schema, table))

                return cur.fetchall()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self.__connected_ctl = False
                    continue
                else:
                    raise error

    def __iter__(self):
        return iter(self.fetchone, None)
