from app import inputquery
from app import workplace
from app import segmentupdater
from app import connectionfactory

#run a full sequence locally

#connectionfactory is here to be able to mock the connection
connectionfactory = connectionfactory.ConnectionFactory()

#redshift contains standard AWS sample data. this is a sample query
inputsql = """SELECT userid as id, firstname, lastname, total_quantity
FROM   (SELECT buyerid, sum(qtysold) total_quantity
        FROM  sales
        GROUP BY buyerid
        ORDER BY total_quantity desc limit 10) Q, users
WHERE Q.buyerid = userid
ORDER BY Q.total_quantity desc;"""

#setup the input query
inputquery = inputquery.InputQuery(inputsql, 'dev', 'masteruser', 'Hackfun57', 'examplecluster.cih5kokdgm01.us-west-2.redshift.amazonaws.com', '5439', connectionfactory)

#workplace represents the postgre worker database
work = workplace.Workplace('nielstest', 'nielst', 'Funhack75', 'nielstest.cuw6bpg82nly.us-west-2.rds.amazonaws.com', '5432', inputquery, connectionfactory)
work.snapshotstore.provision()

#run query and store result
work.download_data()

#get the differences, if any
updatedrows = work.get_differences()
print(updatedrows)

#specify a writekey and send identify calls to segment for each updated record
#segmentupdater = segmentupdater.SegmentUpdater()
#segmentupdater.identify(updatedrows, 'abc')
