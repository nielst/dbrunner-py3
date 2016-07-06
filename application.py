#!flask/bin/python
from flask import Flask, jsonify, request
from app import inputquery
from app import workplace
from app import segmentupdater
from app import connectionfactory

application = Flask(__name__)

@application.route('/run', methods=['POST'])
def run():
    print(request.json)

    connfactory = connectionfactory.ConnectionFactory()

    if not request.json['warehouse'].get('dbname'):
        request.json['warehouse']['dbname'] = 'dev'
    if not request.json['warehouse'].get('user'):
        request.json['warehouse']['user'] = 'masteruser'
    if not request.json['warehouse'].get('password'):
        request.json['warehouse']['password'] = 'Hackfun57'
    if not request.json['warehouse'].get('host'):
        request.json['warehouse']['host'] = 'examplecluster.cih5kokdgm01.us-west-2.redshift.amazonaws.com'
    if not request.json['warehouse'].get('port'):
        request.json['warehouse']['port'] = '5439'

    if not request.json['workplace'].get('dbname'):
        request.json['workplace']['dbname'] = 'nielstest'
    if not request.json['workplace'].get('user'):
        request.json['workplace']['user'] = 'nielst'
    if not request.json['workplace'].get('password'):
        request.json['workplace']['password'] = 'Funhack75'
    if not request.json['workplace'].get('host'):
        request.json['workplace']['host'] = 'nielstest.cuw6bpg82nly.us-west-2.rds.amazonaws.com'
    if not request.json['workplace'].get('port'):
        request.json['workplace']['port'] = '5432'

    inputsql = """SELECT userid as id, firstname, lastname, total_quantity
    FROM   (SELECT buyerid, sum(qtysold) total_quantity
            FROM  sales
            GROUP BY buyerid
            ORDER BY total_quantity desc limit 10) Q, users
    WHERE Q.buyerid = userid
    ORDER BY Q.total_quantity desc;"""

    if not request.json.get('sql'):
        request.json['sql'] = inputsql

    query = inputquery.InputQuery(
        request.json['sql'],
        request.json['warehouse']['dbname'],
        request.json['warehouse']['user'],
        request.json['warehouse']['password'],
        request.json['warehouse']['host'],
        request.json['warehouse']['port'],
        connfactory)

    #demo hack
    if request.json.get('force_change'):
        tempconn = connfactory.get_conn(query.connectionstring)    #borrowing string from inputquery
        tempcur = tempconn.cursor()
        tempcur.execute("UPDATE sales SET qtysold = qtysold * 2 FROM sales WHERE salesid = 22990")
        tempconn.commit()
        tempcur.close
        tempconn.close

    y = workplace.Workplace(
            request.json['workplace']['dbname'],
            request.json['workplace']['user'],
            request.json['workplace']['password'],
            request.json['workplace']['host'],
            request.json['workplace']['port'],
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
