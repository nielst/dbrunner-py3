import analytics    #pip install analytics-python --ignore-installed six
import logging

class SegmentUpdater:

    def identify(self, updatedrows, writekey):
        analytics.write_key = writekey

        # change to cloudwatch
        logging.basicConfig(filename='identify.log',level=logging.INFO,format='%(asctime)s %(message)s')

        for row in updatedrows:
            traits = {}
            userid = ''
            for key, value in row.items():
                if key == 'id':
                    userid = value
                else:
                    traits[key] = value

            if not userid:
                raise Exception('Missing id in row ' + row)

            if not traits:
                raise Exception('No traits for row ' + row)

            analytics.identify(userid, traits)

            logging.info('%s userId:%s traits:%s', writekey, userid, traits)
