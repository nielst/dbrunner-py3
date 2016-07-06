from app import inputquery
from app import workplace
from app import segmentupdater
from app import connectionfactory

connectionfactory = connectionfactory.ConnectionFactory()

inputsql = """SELECT userid as id, firstname, lastname, total_quantity
FROM   (SELECT buyerid, sum(qtysold) total_quantity
        FROM  sales
        GROUP BY buyerid
        ORDER BY total_quantity desc limit 10) Q, users
WHERE Q.buyerid = userid
ORDER BY Q.total_quantity desc;"""

inputquery = inputquery.InputQuery(inputsql, 'dev', 'masteruser', 'Hackfun57', 'examplecluster.cih5kokdgm01.us-west-2.redshift.amazonaws.com', '5439', connectionfactory)

y = workplace.Workplace('nielstest', 'nielst', 'Funhack75', 'nielstest.cuw6bpg82nly.us-west-2.rds.amazonaws.com', '5432', inputquery, connectionfactory)

y.download_data()
updatedrows = y.get_differences()

print(updatedrows)

segmentupdater = segmentupdater.SegmentUpdater()
segmentupdater.identify(updatedrows, 'abc')
