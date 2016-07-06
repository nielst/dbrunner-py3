import psycopg2
import unittest
import testing.postgresql
from unittest.mock import MagicMock
from sqlalchemy import create_engine
from app import snapshotstore
from time import time

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

        conn = psycopg2.connect(**self.postgresql.dsn())

        connfactory = MagicMock()
        connfactory.get_conn().return_value = conn

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE snapshots(created timestamp, tablename varchar(256))")

        store = snapshotstore.Snapshotstore(self.dbname, self.user, self.password, self.host, self.port, connfactory)
        input_created = time()
        input_tablename = 'sometable'
        store.add_snapshot(input_created, input_tablename)

        cursor.execute("SELECT created, tablename FROM snapshots ORDER BY created DESC LIMIT 1")
        actual = cursor.fetchone()

        cursor.close()
        conn.rollback()
        conn.close()

        expected = [[input_created, input_tablename]]

        self.assertEqual(actual,expected)
