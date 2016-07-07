#no unit tests as we cant mock assert expressions (not deterministic)
#ephemeral dynamodb not available for local integration, ddbmock no longer maintained
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import decimal

class ConfigStore:
    def __init__(self):
        self.TABLENAME = 'dbrunner_configs'

    def get_conn(self):
        return boto3.resource('dynamodb', region_name='us-west-2')

    def get_config(self,id):
        dynamodb = self.get_conn()
        table = dynamodb.Table(self.TABLENAME)

        response = table.query(
            KeyConditionExpression=Key('id').eq('1')
        )

        if len(response['Items']) == 0:
            return None

        return response['Items'][0]

    def get_configs(self, startkey):
        dynamodb = self.get_conn()
        table = dynamodb.Table(self.TABLENAME)

        pe = "id, #query, warehouse.host, warehouse.dbname, workplace.host, workplace.dbname"
        ean = { "#query": "query", }

        if startkey:
            response = table.scan(ProjectionExpression=pe, ExpressionAttributeNames=ean, ExclusiveStartKey=startkey)
        else:
            response = table.scan(ProjectionExpression=pe, ExpressionAttributeNames=ean)

        result = {}
        result['configs'] = response['Items']
        if response.get('LastEvaluatedKey'):
            result['last_key'] = response['LastEvaluatedKey']
        return result

    def update_config(self,config):
        dynamodb = self.get_conn()
        table = dynamodb.Table(self.TABLENAME)

        response = table.update_item(
            Key={
                'id': config['id']
            },
            UpdateExpression="set #q = :q, warehouse = :wa, workplace = :wp",
            ExpressionAttributeValues={
                ':q': config['query'],
                ':wa': config['warehouse'],
                ':wp': config['workplace']
            },
            ExpressionAttributeNames= {
                '#q': 'query',
            },
            ReturnValues="UPDATED_NEW"
        )

        return response['Attributes']

    def insert_config(self,config):
        dynamodb = self.get_conn()
        table = dynamodb.Table(self.TABLENAME)

        response = table.put_item(
           Item={
                'id': config['id'],
                'query': config['query'],
                'warehouse': config['warehouse'],
                'workplace': config['workplace']
            }
        )


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
