import json
import application
import unittest

class ApplicationTestCase(unittest.TestCase):

    def setUp(self):
        application.application.testing = True
        self.app = application.application.test_client()

    def test_good_empty_run(self):
        response = self.app.post('/run/1', data='{}', headers={'content-type':'application/json'})
        self.assertEqual(response.status_code,200)
        self.assertEqual(json.loads(response.data.decode('ascii')),[])

    def test_missing_config(self):
        response = self.app.post('/run/fsdfsafasf', data='{}', headers={'content-type':'application/json'})
        self.assertEqual(response.status_code,404)

    def test_run_force_change(self):
        response = self.app.post('/run/1', data='{"force_change":"true"}', headers={'content-type':'application/json'})
        self.assertEqual(response.status_code,200)
        id_of_user_being_updated = 8933
        self.assertEqual(json.loads(response.data.decode('ascii'))[0][0],id_of_user_being_updated)
