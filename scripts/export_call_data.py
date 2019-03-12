import sys
import argparse
import logging
from closeio_api import Client as CloseIO_API, APIError
import csv
reload(sys)
sys.setdefaultencoding('utf-8')


parser = argparse.ArgumentParser(description='Download a CSV of calls from/to a specific Close.io number over a specified time range')

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--direction', '-d', default=None, choices=['inbound', 'outbound'], 
                    help='Use this field to only export inbound calls or outbound calls. Leave this field blank to export both.')
parser.add_argument('--missed-or-voicemail', '-m', action='store_true', help='Use this field to only export missed calls, voicemails, or calls of a duration 0')
parser.add_argument('--end-date', '-e',
                    help='The end of the date range you want to export call data for in yyyy-mm-dd format.')
parser.add_argument('--start-date', '-s',
                    help='The start of the date range you want to export call data for in yyyy-mm-dd format.')
parser.add_argument('--phone-number', '-p',
                    help='The phone number you\'d like to export the calls for in E164 international format. Example: +18552567346')
parser.add_argument('--unattached-only', '-o', action='store_true', help='Use this field if you only want to find calls not attached to a lead')
parser.add_argument('--user-id', '-u', help='Use this field if you only want to find calls for a specific user')


args = parser.parse_args()

api = CloseIO_API(args.api_key)

org_id = api.get('api_key/' + args.api_key)['organization_id']
org_name = api.get('organization/' + org_id)['name'].replace('/', "")

params = {}
has_more = True
offset = 0 
calls = []
display_names = {}
query = '(has:calls'

if args.start_date:
    params['date_created__gte'] = args.start_date
    query = query + ' date >= "%s"' % args.start_date

if args.end_date:
    params['date_created__lte'] = args.end_date
    query = query + ' date <= "%s"' % args.end_date
query = query + ")"

if args.user_id:
    params['user_id'] = args.user_id

if not args.unattached_only:
    print "Getting Lead Display Names..."
    while has_more:
        resp = api.get('lead', params={ '_skip': offset, 'query': query, '_fields': 'id,display_name' })
        for lead in resp['data']:
            display_names[lead['id']] = lead['display_name']
        print offset
        offset+=len(resp['data'])
        has_more = resp['has_more']

has_more = True
offset = 0 
params['_fields'] = 'id,user_id,duration,direction,date_created,remote_phone,local_phone,voicemail_url,recording_url,source,lead_id,updated_by_name'
print "Getting Calls:"
while has_more:
    params['_skip'] = offset
    resp = api.get('activity/call', params=params)
    for call in resp['data']:
        if call.get('lead_id') and display_names.get(call['lead_id']):
            call['lead_name'] = display_names[call['lead_id']]
        else:
            call['lead_name'] = ""
        calls.append(call)
    offset+=len(resp['data'])
    print offset
    has_more = resp['has_more']

if args.missed_or_voicemail:
    calls = [i for i in calls if i['duration'] == 0]
if args.direction:
    calls = [i for i in calls if i['direction'] == args.direction]
if args.phone_number:
    calls = [i for i in calls if i['local_phone'] == args.phone_number]
if args.unattached_only:
    calls = [i for i in calls if i['lead_id'] == None]

f = open('%s Calls.csv' % org_name, 'wt')
try: 
    keys = ['date_created', 'updated_by_name'] + [i for i in params['_fields'].split(',') if i not in ['date_created', 'updated_by_name', 'lead_id']] + ['lead_id', 'lead_name']
    writer = csv.DictWriter(f, keys)
    writer.writeheader()
    writer.writerows(calls)
finally:
    f.close()