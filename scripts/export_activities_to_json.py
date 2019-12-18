import argparse
import json
from datetime import datetime
from operator import itemgetter

import gevent.monkey
gevent.monkey.patch_all()
from closeio_api import Client as CloseIO_API
from dateutil.relativedelta import relativedelta
from gevent.pool import Pool

parser = argparse.ArgumentParser(description='Export Close activity data within a date range into a JSON file')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--date-start', '-s', required=True, help='The yyyy-mm-dd you want to start looking for activities')
parser.add_argument('--date-end', '-e', required=True, help='The yyyy-mm-dd you want to end looking for activities')
parser.add_argument('--activity-type', '-t', choices=['call', 'created', 'email', 'lead_status_change', 'note', 'opportunity_status_change', 'sms', 'task_completed'], required=True, help='The type of activity you\'d like to export to JSON')
args = parser.parse_args()

api = CloseIO_API(args.api_key)
org_id = api.get('api_key/' + args.api_key, params={'_fields': 'organization_id'})['organization_id']
org_name = api.get('organization/' + org_id, params={'_fields': 'name'})['name'].replace('/', '')
days = []
activities = []

endpoint = args.activity_type
if endpoint == 'opportunity_status_change':
    endpoint = 'status_change/opportunity'
elif endpoint == 'lead_status_change':
    endpoint = 'status_change/lead'

starting_date = datetime.strptime(args.date_start, '%Y-%m-%d')
ending_date = starting_date + relativedelta(days=+1) - relativedelta(seconds=+1)
ending_date_final = datetime.strptime(args.date_end, '%Y-%m-%d')

# Generate a list of days to cycle through in the date range
while starting_date < ending_date_final:
    starting_date_string = datetime.strftime(starting_date, "%Y-%m-%dT%H:%M:%S")
    ending_date_string = datetime.strftime(ending_date, "%Y-%m-%dT%H:%M:%S")
    days.append({
        'day': starting_date.strftime('%Y-%m-%d'),
        'start_date': starting_date_string,
        'end_date': ending_date_string
    })
    starting_date = starting_date + relativedelta(days=+1)
    ending_date = starting_date + relativedelta(days=+1) - relativedelta(seconds=+1)


# Method to get all of the specified activities for a specific day.
def getActivities(day):
    print(f"Getting all {args.activity_type} activites for {day['day']}...")
    has_more = True
    offset = 0
    while has_more:
        resp = api.get('activity/' + endpoint, params={'_skip': offset, 'date_created__gte': day['start_date'], 'date_created__lte': day['end_date']})
        for activity in resp['data']:
            activities.append(activity)
        offset += len(resp['data'])
        has_more = resp['has_more']


pool = Pool(5)
pool.map(getActivities, days)

# Sort all activities by date_created to be in order because they were pulled in parallel
activities = sorted(activities, key=itemgetter('date_created'), reverse=True)

with open('%s - %s activity export between %s and %s.json' % (org_name, args.activity_type, args.date_start, args.date_end), 'w') as outfile:
    json.dump(activities, outfile, indent=4)
