import psycopg2
import psycopg2.extras
import time
import json
from app import snapshotstore

class Workplace:

    def __init__(self, dbname, user, password, host, port, inputquery, connectionfactory):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.inputquery = inputquery
        self.updates = []
        self.connectionfactory = connectionfactory

        self.connectionstring = ("dbname='{}' user='{}' host='{}' port='{}' password='{}'").format(dbname, user, host, port, password)
        self.snapshotstore = snapshotstore.Snapshotstore(dbname, user, password, host, port, connectionfactory)

    def download_data(self):
        conn = self.connectionfactory.get_conn(self.connectionstring)

        cur = conn.cursor()

        # expecting CREATE EXTENSION dblink; already done when provisioning database
        cur.execute("""SELECT dblink_connect('{}');""".format(self.inputquery.connectionstring.replace('\'','')))

        formattedcolumnlist = self.__format_column_list(self.inputquery.get_columns())
        table_name = self.__get_table_name()

        cur.execute("""SELECT mytable.*
                INTO {}
                FROM dblink('{}')
                AS mytable({});""".format(table_name, self.inputquery.sql, formattedcolumnlist))

        conn.commit()
        cur.close()
        conn.close()

        self.snapshotstore.add_snapshot(table_name)

    def get_differences(self):
        conn = self.connectionfactory.get_conn(self.connectionstring)
        snapshots = self.snapshotstore.get_latest(2)

        if len(snapshots) < 2:
            return []

        #TODO unit test that we are comparing the right two tables
        updatedrows = self.__detect_updates(snapshots[0][1], snapshots[1][1], conn)

        conn.close()

        return updatedrows

    def __format_column_list(self,columns):
        columnstring = ''
        for col in columns:
            columnstring += col['name'] + ' ' + col['datatype'] + ','
        return columnstring.rstrip(',')

    def __get_table_name(self):
        return 'snapshot' + time.strftime("%Y%m%d_%H%M%S")

    def __detect_updates(self, new_tablename, last_tablename, conn):
        deltasql = """SELECT *
                        FROM ( SELECT *, "{0}"::text AS value FROM "public"."{0}" ) new
                        LEFT JOIN (SELECT "{1}"::text AS value FROM "public"."{1}" ) old ON old.value = new.value
                        WHERE old.value is null""".format(new_tablename, last_tablename)

        dict_cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
        dict_cur.execute(deltasql)
        records = dict_cur.fetchall()
        dict_cur.close
        return records
