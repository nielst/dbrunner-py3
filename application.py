#!flask/bin/python
from flask import Flask, jsonify, request
from app import inputquery
from app import workplace
from app import segmentupdater
from app import connectionfactory
from app import configstore
from flask import make_response
from flask import abort
import uuid

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

@application.route('/dbrunner/api/v1.0/configs', methods=['GET'])
def get_configs():
    connfactory = connectionfactory.ConnectionFactory()

    startkey = request.args.get('startkey')

    store = configstore.ConfigStore()
    return jsonify(store.get_configs(startkey))

@application.route('/dbrunner/api/v1.0/configs/<string:config_id>', methods=['GET'])
def get_config(config_id):
    connfactory = connectionfactory.ConnectionFactory()

    store = configstore.ConfigStore()
    config = store.get_config(config_id)
    if not config:
        abort(404)

    return jsonify(config)

@application.route('/dbrunner/api/v1.0/configs', methods=['POST'])
def insert_config():
    connfactory = connectionfactory.ConnectionFactory()

    newconfig = {}
    newconfig['id'] = str(uuid.uuid4())
    newconfig['query'] = request.json.get('query')
    newconfig['warehouse'] = request.json.get('warehouse')
    newconfig['workplace'] = request.json.get('workplace')

    store = configstore.ConfigStore()
    store.insert_config(newconfig)

    return ('', 204)

@application.route('/dbrunner/api/v1.0/configs/<string:config_id>', methods=['PUT'])
def update_config(config_id):
    connfactory = connectionfactory.ConnectionFactory()

    store = configstore.ConfigStore()
    config = store.get_config(config_id)
    if not config:
        abort(404)

    updateconfig = {}
    updateconfig['id'] = config_id
    updateconfig['query'] = request.json.get('query')
    updateconfig['warehouse'] = request.json.get('warehouse')
    updateconfig['workplace'] = request.json.get('workplace')

    return jsonify(store.update_config(updateconfig))


@application.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

if __name__ == '__main__':
    application.run(debug=True)
