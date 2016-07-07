import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import decimal

class ConfigStore:
    def get_config(self,id):
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table('dbrunner_configs')

        response = table.query(
            KeyConditionExpression=Key('id').eq(id)
        )

        if len(response['Items']) != 1:
            raise RuntimeError('get_config returned {} items'.format(len(response['Items'])))

        return response['Items'][0]


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
