import argparse
import csv
import sys

from closeio_api import APIError, Client as CloseIO_API

parser = argparse.ArgumentParser(
    description='Export a list of custom field changes for a specific custom field'
)

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--start-date',
    '-s',
    help='The start of the date range you want to export call data for in yyyy-mm-dd format.',
)
parser.add_argument(
    '--end-date',
    '-e',
    help='The end of the date range you want to export call data for in yyyy-mm-dd format.',
)
parser.add_argument(
    '--custom-field',
    '-f',
    required=True,
    help='The lcf id of the custom field you\'re searching for',
)
parser.add_argument(
    '--lead-id',
    '-l',
    help='Use this field if you want to narrow your search to a specific lead_id',
)
parser.add_argument(
    '--user-id',
    '-u',
    help='Use this field if you want to narrow your search to changes done by a specific user',
)
args = parser.parse_args()

api = CloseIO_API(args.api_key)
org_id = api.get('api_key/' + args.api_key)['organization_id']
org = api.get(
    'organization/' + org_id,
    params={
        '_fields': 'id,name,memberships,inactive_memberships,lead_custom_fields'
    },
)
org_name = org['name'].replace('/', "")
org_memberships = org['memberships'] + org['inactive_memberships']
try:
    custom_field_name = [
        i for i in org['lead_custom_fields'] if i['id'] == args.custom_field
    ][0]['name']
except IndexError as e:
    print(
        f"ERROR: Could not find custom field {args.custom_field} in {org_name}"
    )
    sys.exit()

users = {}

for member in org_memberships:
    users[member['user_id']] = member['user_full_name']

params = {'object_type': 'lead', 'action': 'updated'}

events = []

custom_lcf = "custom." + str(args.custom_field)

if args.start_date:
    params['date_updated__gte'] = args.start_date
if args.end_date:
    params['date_updated__lte'] = args.end_date
if args.lead_id:
    params['lead_id'] = args.lead_id
if args.user_id:
    params['user_id'] = args.user_id

has_more = True
cursor = ''
count = 0
while has_more:
    params['_cursor'] = cursor
    try:
        resp = api.get('event', params=params)
        for event in resp['data']:
            if (
                custom_lcf in event['changed_fields']
                and event.get('previous_data')
                and event.get('data')
            ):
                events.append(
                    {
                        'Date': event['date_created'],
                        'Lead ID': event['lead_id'],
                        'Lead Name': event['data']['display_name'],
                        'User that Made the Change': users[event['user_id']],
                        'Old Value': event['previous_data'].get(custom_lcf),
                        'New Value': event['data'].get(custom_lcf),
                    }
                )
        cursor = resp['cursor_next']
        count += len(resp['data'])
        print(f"Analyzed Events: {count}")
        has_more = bool(resp['cursor_next'])
    except APIError as e:
        pass

print(f"Total {custom_field_name} Change Events Found: {len(events)}")

f = open(
    f'{org_name} {custom_field_name} Custom Field Changes.csv',
    'w',
    newline='',
    encoding='utf-8',
)
try:
    ordered_keys = [
        'Date',
        'Lead ID',
        'Lead Name',
        'User that Made the Change',
        'Old Value',
        'New Value',
    ]
    writer = csv.DictWriter(f, ordered_keys)
    writer.writeheader()
    writer.writerows(events)
finally:
    f.close()
