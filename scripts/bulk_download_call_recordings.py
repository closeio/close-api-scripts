import sys
import argparse
import logging
from closeio_api import Client as CloseIO_API, APIError
import base64
import requests


parser = argparse.ArgumentParser(description='Bulk Download Close.io Call Recordings into a specified Folder')

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--development', '-d', action='store_true',
                    help='Use a development (testing) server rather than production.')
parser.add_argument('--start-date', '-s',
                    help='The start of the date range you want to download recordings for in yyyy-mm-dd format.')
parser.add_argument('--end-date', '-e',
                    help='The end of the date range you want to download recordings for in yyyy-mm-dd format.')
parser.add_argument('--file-path', '-f', required=True, 
                    help='The file path to the folder where the recordings will be stored.')

args = parser.parse_args()

api = CloseIO_API(args.api_key, development=args.development)
api_encoded = "Basic " + str(base64.b64encode(args.api_key))

has_more = True
offset = 0 
leads = {}

params = {}
query = "call((recording_duration > 0 or voicemail_duration > 0)"

if args.start_date:
	params['date_created__gte'] = args.start_date
	query = query + ' date >= "%s"' % args.start_date

if args.end_date:
	params['date_created__lte'] = args.end_date
	query = query + ' date <= "%s"' % args.end_date
query = query + ")"

while has_more:
	
	resp = api.get('lead', params={'_skip':offset, 'query':query, '_fields':'id,display_name'})
	for lead in resp['data']:
		leads[lead['id']] = lead['display_name']
		
	offset+=len(resp['data'])
	has_more = resp['has_more']

has_more = True
offset = 0
params['_fields'] = 'recording_url,voicemail_url,date_created,lead_id,duration,voicemail_duration'
while has_more:
	params['offset'] = offset
	resp_calls = api.get('activity/call', params=params)
	for call in resp_calls['data']:
		if call['duration'] > 0 or call['voicemail_duration'] > 0: 
			lead_name = "Detached Call" 
			if 'lead_id' in call and call['lead_id'] != None and call['lead_id'] in leads:
				lead_name = leads[call['lead_id']]
			call_title = lead_name + " " + call['date_created'] + ".mp3"
			call_title = call_title.replace('/', '_').replace(' ', '_')
			if 'recording_url' in call and call['recording_url'] != None: 
				try:
					doc = requests.get(call['recording_url'], headers={'Content-Type':'application/json', 'Authorization':api_encoded})
					with open("%s/%s" % (args.file_path, call_title), 'wb') as f:
						f.write(doc.content)
				except Exception as e:
					print e
			elif 'voicemail_url' in call and call['voicemail_url'] != None: 
				try:
					doc = requests.get(call['voicemail_url'], headers={'Content-Type':'application/json', 'Authorization':api_encoded})
					with open("%s/Voicemail %s" % (args.file_path, call_title), 'wb') as f:
						f.write(doc.content)
				except Exception as e:
					print e
	offset+=len(resp_calls['data'])
	has_more = resp_calls['has_more']
