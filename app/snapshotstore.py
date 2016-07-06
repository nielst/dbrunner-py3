class Snapshotstore:

    def __init__(self, dbname, user, password, host, port, connectionfactory):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connectionfactory = connectionfactory

    def add_snapshot(self, created, tablename):
        return 0
