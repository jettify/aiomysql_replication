aiomysql_replication (Work in Progress)
=======================================

**aiomysql_replication** -- implementation of MySQL replication protocol build
on top of aiomysql and PyMySQ for asynico (PEP-3156/tulip). This library is
fork of python-mysql-replication_ with asyncio_ support.


Basic Example
-------------

.. code:: python

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
        stream = yield from create_binlog_stream(
            connection_settings=MYSQL_SETTINGS, server_id=3, blocking=True,
            loop=loop)

        while True:
            event = yield from stream.fetchone()
            event.dump()
        stream.close()


    loop.run_until_complete(test_example())


Use cases
---------

* MySQL to NoSQL database replication
* MySQL to search engine replication
* Invalidate cache when something change in database
* Audit
* Real time analytics


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* PyMySQL_
* aiomysql_


.. _Python: https://www.python.org
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _PyMySQL: https://github.com/PyMySQL/PyMySQL
.. _python-mysql-replication: https://github.com/noplay/python-mysql-replication
.. _aiomysql: https://github.com/aio-libs/aiomysql
