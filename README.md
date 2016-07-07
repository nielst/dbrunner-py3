# dbrunner-py3

POC for finding differences over time in SQL results and raising those to segment.com

Configurations
--------------

Configuration for each job is stored in Dynamodb in the following format:
    
    {
      "id": "1",
      "query": "SELECT userid as id, firstname, lastname, total_quantity\nFROM   (SELECT buyerid, sum(qtysold) total_quantity\nFROM  sales\nGROUP BY buyerid\nORDER BY total_quantity desc limit 10) Q, users\nWHERE Q.buyerid = userid\nORDER BY Q.total_quantity desc",
      "warehouse": {
        "dbname": "",
        "host": "",
        "password": "",
        "port": "",
        "user": ""
      },
      "workplace": {
        "dbname": "",
        "host": "",
        "password": "",
        "port": "",
        "user": ""
      }
    }

The configurations are managed the following REST API:

GET http://dbrunner-env.us-west-2.elasticbeanstalk.com/dbrunner/api/v1.0/configs

GET http://dbrunner-env.us-west-2.elasticbeanstalk.com/dbrunner/api/v1.0/configs/{id}

POST http://dbrunner-env.us-west-2.elasticbeanstalk.com/dbrunner/api/v1.0/configs

PUT http://dbrunner-env.us-west-2.elasticbeanstalk.com/dbrunner/api/v1.0/configs/{id}

The API does not have authentication yet

How to run
--------------

POST to the following endpoint to run a stored configuration: Configuration 1 is in working state

    POST: http://dbrunner-env.us-west-2.elasticbeanstalk.com/run/{config_id}
    Content-Type: application/json
    Required payload: {}

The endpoint will synchronously do the following
- execute the query against the warehouse
- store the result in a new table in Postgressql
- compare the contents to the previous snapshot and find updated records
- if given a writekey, send an identify call to Segment for each updated record

        Payload: { "writekey": "123" }

Usually there will not be any changes, since the warehouse contains stale sample data.
However you can easily force a single record to update:

    Payload: { "force_change": "true" }

You can also make your own updates to the redshift warehouse, or connect to your own.


Unit tests
--------------

Some logic is to be run by Postgre, and the tests include launching a database. These require postgres installed.

    #python3 -m "nose" -v

Full sequence
--------------

Run the full sequence locally with test.py



.
