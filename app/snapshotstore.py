class Snapshotstore:

    def __init__(self, dbname, user, password, host, port, connectionfactory):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connectionfactory = connectionfactory

        self.connectionstring = ("dbname='{}' user='{}' host='{}' port='{}' password='{}'").format(dbname, user, host, port, password)

    def add_snapshot(self, tablename, configid):
        connection = self.connectionfactory.get_conn(self.connectionstring)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO snapshots (created, tablename, configid) VALUES (CURRENT_TIMESTAMP,'{0}', '{1}')".format(tablename,configid))

        connection.commit()
        cursor.close()
        connection.close()

    def get_latest(self, limit, configid):
        connection = self.connectionfactory.get_conn(self.connectionstring)
        cursor = connection.cursor()
        cursor.execute("SELECT created, tablename FROM snapshots WHERE configid = '{0}' ORDER BY created DESC LIMIT {1}".format(configid,limit))
        records = cursor.fetchall()

        connection.commit()
        cursor.close()
        connection.close()

        return records

    def provision(self):
        connection = self.connectionfactory.get_conn(self.connectionstring)
        cursor = connection.cursor()

        cursor.execute("SELECT EXISTS(select * from information_schema.tables where table_name='snapshots')")
        if not cursor.fetchone()[0]:
            cursor.execute("CREATE TABLE snapshots(created timestamptz, tablename varchar(256))")
            connection.commit()

        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='snapshots' and column_name='configid';")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE snapshots ADD COLUMN configid varchar(100)")
            connection.commit()

        cursor.close()
        connection.close()
