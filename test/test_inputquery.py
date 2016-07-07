#python3 -m "nose" -v
import unittest
from unittest.mock import MagicMock
from app import inputquery
from app import connectionfactory
import collections

class InputQueryTest(unittest.TestCase):
    def setUp(self):
        Column = collections.namedtuple('Column', 'name type_code')
        self.col1 = Column(name='ID', type_code=123)    #uppercase to ensure case insensitivity when checking for id column
        self.col2 = Column(name='name', type_code=456)
        self.col3 = Column(name='phone', type_code=456)

        self.postgre_type_codes = [(123, 'int'), (456, 'varchar')]

        self.inputsql = "some sql that produces the expected columns"
        self.dbname = 'somedatabase'
        self.user = 'someuser'
        self.password = 'somepassword'
        self.host = 'somehost'
        self.port = 'someport'


    def test_column_identification(self):
        querycolumns = (self.col1, self.col2, self.col3)

        connfactory = MagicMock()
        connfactory.get_conn().cursor().fetchall.return_value = self.postgre_type_codes
        connfactory.get_conn().cursor().description = querycolumns

        query = inputquery.InputQuery(
            self.inputsql, self.dbname, self.user, self.password, self.host, self.port, connfactory)

        self.assertEqual(
            query.get_columns(),
                [
                    {'datatype': 'int', 'name': 'ID'},
                    {'datatype': 'varchar', 'name': 'name'},
                    {'datatype': 'varchar', 'name': 'phone'}
                ]
            )
        connfactory.get_conn().cursor().fetchall.assert_called_with()


    def test_missing_id_column(self):
        querycolumns = (self.col2, self.col3)

        connfactory = MagicMock()
        connfactory.get_conn().cursor().fetchall.return_value = self.postgre_type_codes
        connfactory.get_conn().cursor().description = querycolumns

        query = inputquery.InputQuery(
            self.inputsql, self.dbname, self.user, self.password, self.host, self.port, connfactory)

        self.assertRaises(RuntimeError, query.get_columns)
