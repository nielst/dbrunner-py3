import psycopg2

class InputQuery:

    def __init__(self, sql, dbname, user, password, host, port, connectionfactory):
        self.sql = sql
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connectionfactory = connectionfactory

        self.connectionstring = ("dbname='{}' user='{}' host='{}' port='{}' password='{}'").format(dbname, user, host, port, password)

    def get_columns(self):
        self.connection = self.connectionfactory.get_conn(self.connectionstring)

        cur = self.connection.cursor()

        cur.execute("select oid, typname from pg_type")
        type_codes = cur.fetchall()

        cur.execute(self.sql)

        cols = []

        INDEXOFTHETUPLE = 0
        SECONDELEMENTOFTHETUPLE = 1

        desc = cur.description

        for col in desc:
            columnname = col.name
            datatype = [item for item in type_codes if item[0] == col.type_code][INDEXOFTHETUPLE][SECONDELEMENTOFTHETUPLE]
            cols.append({'name': columnname, 'datatype': datatype})

        idcolumn = [item for item in cols if item['name'].lower() == 'id']
        if not idcolumn:
            raise RuntimeError('InputQuery must have an id column')

        cur.close()
        self.connection.close()

        return cols
