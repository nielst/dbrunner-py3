from app import inputquery
from app import workplace
from app import segmentupdater
from app import connectionfactory
from app import configstore

#run a full sequence locally

store = configstore.ConfigStore()
config = store.get_config('1')

#connectionfactory is here to be able to mock the connection
connectionfactory = connectionfactory.ConnectionFactory()

#setup the input query
inputquery = inputquery.InputQuery(
    config['query'],
    config['warehouse']['dbname'], config['warehouse']['user'], config['warehouse']['password'],
    config['warehouse']['host'], config['warehouse']['port'], connectionfactory)

#workplace represents the postgre worker database
work = workplace.Workplace(
    config['workplace']['dbname'], config['workplace']['user'], config['workplace']['password'],
    config['workplace']['host'], config['workplace']['port'], inputquery, connectionfactory)
work.snapshotstore.provision()

#run query and store result
work.download_data()

#get the differences, if any
updatedrows = work.get_differences()
print(updatedrows)

#specify a writekey and send identify calls to segment for each updated record
#segmentupdater = segmentupdater.SegmentUpdater()
#segmentupdater.identify(updatedrows, 'abc')
