#!flask/bin/python
from flask import Flask, jsonify, request
from app import inputquery
from app import workplace
from app import segmentupdater
from app import connectionfactory
from app import configstore

application = Flask(__name__)

@application.route('/run/<string:config_id>', methods=['POST'])
def run(config_id):
    connfactory = connectionfactory.ConnectionFactory()

    store = configstore.ConfigStore()
    config = store.get_config(config_id)

    query = inputquery.InputQuery(
        config['sql'],
        config['warehouse']['dbname'],
        config['warehouse']['user'],
        config['warehouse']['password'],
        config['warehouse']['host'],
        config['warehouse']['port'],
        connfactory)

    #demo hack
    if request.json.get('force_change'):
        tempconn = connfactory.get_conn(query.connectionstring)    #borrowing string from inputquery
        tempcur = tempconn.cursor()
        tempcur.execute("UPDATE sales SET qtysold = qtysold + 1 FROM sales WHERE salesid = 22990")
        tempconn.commit()
        tempcur.close
        tempconn.close

    y = workplace.Workplace(
            config['workplace']['dbname'],
            config['workplace']['user'],
            config['workplace']['password'],
            config['workplace']['host'],
            config['workplace']['port'],
            query,
            connfactory)

    y.download_data()
    updatedrows = y.get_differences()

    if request.json.get('writekey'):
        segment = segmentupdater.SegmentUpdater()
        segment.identify(updatedrows, request.json['writekey'])

    return jsonify(updatedrows)




if __name__ == '__main__':
    application.run(debug=True)
