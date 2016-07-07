from app import configstore
from app.configstore import DecimalEncoder
import json

store = configstore.ConfigStore()

config = store.get_config('432423423')

config['id'] = '3'
config['warehouse']['dbname'] = '3'

store.insert_config(config)

#print(json.dumps(res, indent=4, cls=DecimalEncoder))
