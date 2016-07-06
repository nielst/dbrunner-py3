# dbrunner-py3

POC for finding differences over time in SQL results and raising those to segment.com

HTTP API
--------------

Best way to experiment is to run the HTTP API

    http://dbrunner-env.us-west-2.elasticbeanstalk.com/run

All arguments are optional, but warehouse and workplace must be present even if they are empty
Warehouse is where we want to execute the sql query and monitor for changes
Workplace is the Postgre where we store results and compare

  {
    "writekey": "abc",
    "force_change": "true",
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
