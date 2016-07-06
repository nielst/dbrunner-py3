import psycopg2
import unittest
from unittest.mock import MagicMock
import testing.postgresql
from sqlalchemy import create_engine
from app import workplace

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

    def test_updated_value(self):

        engine = create_engine(self.postgresql.url())

        conn = psycopg2.connect(**self.postgresql.dsn())

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE old(id int, name varchar(256), purchases int)")
        cursor.execute("CREATE TABLE new(id int, name varchar(256), purchases int)")
        cursor.execute("INSERT INTO old values(1, 'Niels', 5), (2, 'John', 0), (3, 'Bob', 1)")
        cursor.execute("INSERT INTO new values(1, 'Niels', 5), (2, 'John', 1), (3, 'Bob', 1)")

        nullinputquery = None
        nullconnfactory = None

        wp = workplace.Workplace(self.dbname, self.user, self.password, self.host, self.port, nullinputquery, nullconnfactory)
        updates = wp._Workplace__detect_updates('new','old',conn)

        cursor.close()
        conn.rollback()
        conn.close()

        expected = [[2, 'John', 1, '(2,John,1)', None]]

        self.assertEqual(updates,expected)

    def test_new_record(self):

        engine = create_engine(self.postgresql.url())

        conn = psycopg2.connect(**self.postgresql.dsn())

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE old(id int, name varchar(256), purchases int)")
        cursor.execute("CREATE TABLE new(id int, name varchar(256), purchases int)")
        cursor.execute("INSERT INTO old values(1, 'Niels', 5), (2, 'John', 1)")
        cursor.execute("INSERT INTO new values(1, 'Niels', 5), (2, 'John', 1), (3, 'Bob', 0)")

        nullinputquery = None
        nullconnfactory = None

        wp = workplace.Workplace(self.dbname, self.user, self.password, self.host, self.port, nullinputquery, nullconnfactory)
        updates = wp._Workplace__detect_updates('new','old',conn)

        cursor.close()
        conn.rollback()
        conn.close()

        expected = [[3, 'Bob', 0, '(3,Bob,0)', None]]

        self.assertEqual(updates,expected)

    def test_less_than_2_snapshots(self):

        engine = create_engine(self.postgresql.url())
        connfactory = MagicMock()
        connfactory.get_conn.return_value = self.open_connection()

        nullinputquery = None

        wp = workplace.Workplace(self.dbname, self.user, self.password, self.host, self.port, nullinputquery, connfactory)
        wp.snapshotstore.provision()

        connfactory.get_conn.return_value = self.open_connection()
        wp = workplace.Workplace(self.dbname, self.user, self.password, self.host, self.port, nullinputquery, connfactory)
        actual = wp.get_differences()

        expected = []

        self.assertEqual(actual,expected)

    def open_connection(self):
        return psycopg2.connect(**self.postgresql.dsn())
