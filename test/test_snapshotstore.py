import psycopg2
import unittest
import testing.postgresql
from unittest.mock import MagicMock
from sqlalchemy import create_engine
from app import snapshotstore
from datetime import datetime

Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

def tearDownModule(self):
    # clear cached database at end of tests
    Postgresql.clear_cache()

class WorkplaceTest(unittest.TestCase):

    def setUp(self):
        self.dbname = 'somedatabase'
        self.user = 'someuser'
        self.password = 'somepassword'
        self.host = 'somehost'
        self.port = 'someport'

        self.postgresql = Postgresql()

    def tearDown(self):
        self.postgresql.stop()

    def test_add_snapshot(self):

        engine = create_engine(self.postgresql.url())
        conn = self.open_connection()
        connfactory = MagicMock()

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE snapshots(created timestamptz, tablename varchar(256))")
        conn.commit()

        connfactory.get_conn.return_value = self.open_connection()
        store = snapshotstore.Snapshotstore(self.dbname, self.user, self.password, self.host, self.port, connfactory)
        store.add_snapshot('sometable1')

        connfactory.get_conn.return_value = self.open_connection()
        store = snapshotstore.Snapshotstore(self.dbname, self.user, self.password, self.host, self.port, connfactory)
        store.add_snapshot('sometable2')

        cursor.execute("SELECT created, tablename FROM snapshots ORDER BY created DESC LIMIT 1")
        actual = cursor.fetchone()
        cursor.close()
        conn.close()

        expected = 'sometable2'

        self.assertEqual(actual[1],expected)

    def open_connection(self):
        return psycopg2.connect(**self.postgresql.dsn())
