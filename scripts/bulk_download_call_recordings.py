import argparse
import base64
import csv
from datetime import datetime
from operator import itemgetter

import gevent.monkey
import requests
from closeio_api import Client as CloseIO_API
from dateutil.relativedelta import relativedelta
from gevent.pool import Pool

gevent.monkey.patch_all()

parser = argparse.ArgumentParser(
    description='Bulk Download Close.io Call Recordings into a specified Folder'
)
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--date_start',
    '-s',
    required=True,
    help='The start of the date range you want to download recordings for in yyyy-mm-dd format.',
)
parser.add_argument(
    '--date_end',
    '-e',
    required=True,
    help='The end of the date range you want to download recordings for in yyyy-mm-dd format.',
)
parser.add_argument(
    '--file-path',
    '-f',
    required=True,
    help='The file path to the folder where the recordings will be stored.',
)
args = parser.parse_args()

api = CloseIO_API(args.api_key)
org_id = api.get(
    f'api_key/{args.api_key}', params={'_fields': 'organization_id'}
)['organization_id']
org_name = api.get('organization/' + org_id, params={'_fields': 'name'})[
    'name'
].replace('/', '')
days = []
calls = []
downloaded_calls = []
starting_date = datetime.strptime(args.date_start, '%Y-%m-%d')
ending_date = (
    starting_date + relativedelta(days=+1) - relativedelta(seconds=+1)
)
ending_date_final = datetime.strptime(args.date_end, '%Y-%m-%d')

# Generate a list of days to cycle through in the date range
while starting_date < ending_date_final:
    starting_date_string = datetime.strftime(
        starting_date, "%Y-%m-%dT%H:%M:%S"
    )
    ending_date_string = datetime.strftime(ending_date, "%Y-%m-%dT%H:%M:%S")
    days.append(
        {
            'day': starting_date.strftime('%Y-%m-%d'),
            'start_date': starting_date_string,
            'end_date': ending_date_string,
        }
    )
    starting_date = starting_date + relativedelta(days=+1)
    ending_date = (
        starting_date + relativedelta(days=+1) - relativedelta(seconds=+1)
    )


# Method to get all of the recordings for a specific day.
def getRecordedCalls(day):
    print(f"Getting all recorded call activities for {day['day']}...")
    has_more = True
    offset = 0
    while has_more:
        resp = api.get(
            'activity/call',
            params={
                '_skip': offset,
                'date_created__gte': day['start_date'],
                'date_created__lte': day['end_date'],
                '_fields': 'id,recording_url,voicemail_url,date_created,lead_id,duration,voicemail_duration,date_created',
            },
        )
        for call in resp['data']:
            if (call['duration'] > 0 or call['voicemail_duration'] > 0) and (
                call.get('recording_url') or call.get('voicemail_url')
            ):
                call['url'] = call.get(
                    'recording_url', call.get('voicemail_url')
                )
                if call['duration'] > 0:
                    call['Type'] = 'Answered Call'
                    call['Answered or Voicemail Duration'] = call['duration']
                else:
                    call['Type'] = 'Voicemail'
                    call['Answered or Voicemail Duration'] = call[
                        'voicemail_duration'
                    ]
                calls.append(call)
        offset += len(resp['data'])
        has_more = resp['has_more']


pool = Pool(5)
pool.map(getRecordedCalls, days)

# Sort all calls by date_created to be in order because they were pulled in parallel
calls = sorted(calls, key=itemgetter('date_created'), reverse=True)


# Method to download a call recording or voicemail recording
def downloadCall(call):
    try:
        call_title = "close-recording-%s.mp3" % call['id']
        url = call['url']
        doc = requests.get(
            url,
            headers={'Content-Type': 'application/json'},
            auth=(args.api_key, ''),
        )
        with open("%s/%s" % (args.file_path, call_title), 'wb') as f:
            f.write(doc.content)
        downloaded_calls.append(
            {
                'Call Activity ID': call['id'],
                'Date Created': call['date_created'],
                'Type': call['Type'],
                'Duration': call['Answered or Voicemail Duration'],
                'Lead ID': call['lead_id'],
                'Filename': call_title,
                'url': url,
            }
        )
        print(
            f"{(calls.index(call) + 1)} of {len(calls)}: Downloading {call_title}"
        )
    except Exception as e:
        print(e)


pool.map(downloadCall, calls)

# Sort all downloaded calls by date_created to be in order because they were pulled in parallel
downloaded_calls = sorted(
    downloaded_calls, key=itemgetter('Date Created'), reverse=True
)
# Write Filename Output to CSV
f = open(
    f'{args.file_path}/{org_name} Downloaded Call Recordings from {args.date_start} to {args.date_end} Reference.csv',
    'wt',
    encoding='utf-8',
)
try:
    ordered_keys = [
        'Call Activity ID',
        'Filename',
        'Date Created',
        'Type',
        'Duration',
        'Lead ID',
        'url',
    ]
    writer = csv.DictWriter(f, ordered_keys)
    writer.writeheader()
    writer.writerows(downloaded_calls)
finally:
    f.close()
