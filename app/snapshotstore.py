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
        return 0
