# dbrunner-py3

POC for finding differences over time in SQL results and raising those to segment.com

HTTP API
--------------

Best way to experiment is to run the HTTP API

    POST: http://dbrunner-env.us-west-2.elasticbeanstalk.com/run

Minimum arguments:
    { "warehouse":{}, "workplace":{} }
In this case it will run with default parameters, and return an empty list of no records updated

Force a change to see a record get returned:
    { "force_change": "true", "warehouse":{}, "workplace":{} }

And add a writekey for the change to be identified to segment:
    { "writekey": "yourkey", "force_change": "true", "warehouse":{}, "workplace":{} }

You can also specify your own warehouse, the query to run and Postgre worker store:

  {
    "sql": "sql query to execute in warehouse",
    "warehouse":{
    	"dbname":"",
    	"user":"",
    	"password":"",
    	"host":"",
    	"port":""
    },
    "workplace":{
    	"dbname":"",
    	"user":"",
    	"password":"",
    	"host":"",
    	"port":""
    }
  }
  
  
Unit tests
--------------

Some logic is to be run by Postgre, and the tests include launching a database

    #python3 -m "nose" -v

Full sequence
--------------

Run the full sequence locally with test.py
