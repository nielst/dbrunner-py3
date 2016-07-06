import psycopg2

class ConnectionFactory:

    def get_conn(self, connstring):
        try:
            return psycopg2.connect(connstring)
        except:
            raise RuntimeError('Unable to connect to database')
