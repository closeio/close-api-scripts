import sys
import argparse
import logging
from closeio_api import Client as CloseIO_API
import csv
reload(sys)
sys.setdefaultencoding('utf-8')


parser = argparse.ArgumentParser(description='Create a CSV of unattached calls for an organization')

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--development', '-d', action='store_true',
                    help='Use a development (testing) server rather than production.')

args = parser.parse_args()


api = CloseIO_API(args.api_key, development=args.development)

offset = 0
has_more = True
activities = []

while has_more: 
	resp = api.get('activity/call', params={'_skip':offset, 'date_created__gte':'2018-07-18'})
	print offset
	calls = resp['data']
	for call in calls:
		if call['lead_id'] == None and call['contact_id'] == None and call['direction']=='inbound':
			activities.append(call)
	print offset
	offset+=len(calls)
	has_more = resp['has_more']

f = open('unattached_calls.csv', 'wt')
try:
    writer = csv.writer(f)
    writer.writerow( ('Date', 'User', 'Customer Phone', 'Direction', 'Duration', 'Recording Url') )
    for a in activities:
        writer.writerow(('%s' % a['date_created'], '%s' % a['updated_by_name'], '%s' % a['remote_phone'], '%s' % a['direction'], '%s' % a ['duration'], '%s' % a['recording_url'])) 
finally:
    f.close()