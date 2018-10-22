import sys
import argparse
import logging
from closeio_api import Client as CloseIO_API, APIError
import csv
reload(sys)
sys.setdefaultencoding('utf-8')


parser = argparse.ArgumentParser(description='Download a CSV of calls from/to a specific Close.io number over a specified time range')

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--phone-number', '-p', required=True,
                    help='The phone number you\'d like to export the calls for in E164 international format. Example: +18552567346')
parser.add_argument('--start-date', '-s',
                    help='The start of the date range you want to export call data for in yyyy-mm-dd format.')
parser.add_argument('--end-date', '-e',
                    help='The end of the date range you want to export call data for in yyyy-mm-dd format.')
parser.add_argument('--direction', '-d', required=False, default=None, choices=['inbound', 'outbound'], 
					help='Use this field to only export inbound calls or outbound calls. Leave this field blank to export both.')
parser.add_argument('--missed-or-voicemail', '-m', action='store_true',
                    help='Use this field to only export missed calls, voicemails, or calls of a duration 0')


args = parser.parse_args()

api = CloseIO_API(args.api_key)

org_id = api.get('api_key/' + args.api_key)['organization_id']
org = api.get('organization/' + org_id)
users = [m for m in org['memberships']] + [m for m in org['inactive_memberships']]
user_names = {}
for a in users:
	user_names[a['user_id']] = a['user_full_name']

has_more = True
offset = 0 
calls_to_number = []
leads = {}

params = {'_fields':'id,user_id,duration,direction,date_created,remote_phone,local_phone,voicemail_url,recording_url,lead_id'}

if args.start_date:
	params['date_created__gte'] = args.start_date
if args.end_date:
	params['date_created__lte'] = args.end_date

while has_more:
    params['_skip'] = offset
    resp = api.get('activity/call', params=params)
    calls = [i for i in resp['data'] if i['local_phone'] == args.phone_number]
    if args.direction:
        calls = [i for i in calls if i['direction'] == args.direction]
    if args.missed_or_voicemail:
        calls = [i for i in calls if i['duration'] == 0]
    for i in range(0, len(calls)):
        calls[i]['lead_name'] = "None"
        if calls[i]['lead_id'] != None:
            try:
                if calls[i]['lead_id'] not in leads:
                    calls[i]['lead_name'] = api.get('lead/' + calls[i]['lead_id'], params={'fields':'id,display_name'})['display_name']
                else:
                    calls[i]['lead_name'] = leads[calls[i]['lead_id']]
            except:
                print "Error getting name for %s" % calls[i]['lead_id']
    calls_to_number += calls
    
    offset+=len(resp['data'])
    has_more = resp['has_more']

f = open('%s Calls.csv' % args.phone_number, 'wt')
try:
    writer = csv.writer(f)
    writer.writerow( ('Date', 'User', 'Lead ID', 'Lead Name', 'Close.io Number', 'Customer Phone', 'Direction', 'Duration', 'Recording URL') )
    for a in calls_to_number:
    	recording_url = "N/A"
    	if 'recording_url' in a and a['recording_url'] != None:
    		recording_url = a['recording_url']
    	elif 'voicemail_url' in a and a['voicemail_url'] != None:
    		recording_url = a['voicemail_url']
    	username = "N/A"
    	if 'user_id' in a and a['user_id'] != None:
    		username = user_names[a['user_id']]
        writer.writerow( ('%s' % a['date_created'], '%s' % username, '%s' % a['lead_id'], '%s' % a['lead_name'], '%s' % a['local_phone'], '%s' % a['remote_phone'], '%s' % a['direction'], '%s' % a['duration'], '%s' % recording_url) ) 
finally:
    f.close()