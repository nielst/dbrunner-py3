class Snapshotstore:

    def __init__(self, dbname, user, password, host, port, connectionfactory):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connectionfactory = connectionfactory

        self.connectionstring = ("dbname='{}' user='{}' host='{}' port='{}' password='{}'").format(dbname, user, host, port, password)

    def add_snapshot(self, tablename):
        connection = self.connectionfactory.get_conn(self.connectionstring)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO snapshots (created, tablename) VALUES (CURRENT_TIMESTAMP,'{0}')".format(tablename))

        connection.commit()
        cursor.close()
        connection.close()

    def get_latest(self, limit):
        connection = self.connectionfactory.get_conn(self.connectionstring)
        cursor = connection.cursor()
        cursor.execute("SELECT created, tablename FROM snapshots ORDER BY created DESC LIMIT {}".format(limit))
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

        cursor.close()
        connection.close()
