import argparse
import csv
import gevent.monkey
from closeio_api import Client as CloseIO_API, APIError
from gevent.pool import Pool

gevent.monkey.patch_all()

parser = argparse.ArgumentParser(description='Get a list of all lead merge events for the last 30 days from your Close organization')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
args = parser.parse_args()

# Initialize the Close API and get all users in the org
api = CloseIO_API(args.api_key)
org_id = api.get('api_key/' + args.api_key)['organization_id']
org = api.get('organization/' + org_id, params={'_fields': 'inactive_memberships,memberships,name'})
org_name = org['name'].replace('/', '')
memberships = org['memberships'] + org['inactive_memberships']
users = {membership['user_id']: membership['user_full_name'] for membership in memberships}


# Method to get data about the deleted source lead added to the event
def getSourceLeadData(event):
    print(f"{(events.index(event) + 1)} of {len(events)}: {event['Merge Event ID']}")
    source_delete_event = api.get('event', params={'object_type': 'lead', 'action': 'deleted', 'lead_id': event['Source Lead ID']})
    if len(source_delete_event['data']) > 0:
        delete_event = source_delete_event['data'][0]
        if delete_event.get('previous_data'):
            event['Source Lead Status'] = delete_event['previous_data'].get('status_label')
            event['Source Lead Name'] = delete_event['previous_data'].get('display_name')


print("Getting all merge events...")

has_more = True
cursor = ''
events = []
offset = 0

# Get all merge events
while has_more:
    try:
        resp = api.get('event', params={'object_type': 'lead', 'action': 'merged', '_cursor': cursor})
        for event in resp['data']:
            if event.get('data') and event.get('meta') and event['meta'].get('merge_source_lead_id'):
                event_data = {
                    'Current Lead URL': 'https://app.close.io/lead/%s/' % event['meta']['merge_destination_lead_id'],
                    'Date': event['date_created'],
                    'Destination Lead Name': event['data']['display_name'],
                    'Destination Lead Status': event['data']['status_label'],
                    'Destination Lead ID': event['meta']['merge_destination_lead_id'],
                    'Source Lead ID': event['meta']['merge_source_lead_id'],
                    'Merge Event ID': event['id'],
                    'Close API Request ID': event['request_id']
                }

                if event.get('user_id') and event['user_id'] in users:
                    event_data['User'] = users[event['user_id']]

                events.append(event_data)
        cursor = resp['cursor_next']
        has_more = bool(cursor)
        offset = len(events)
        print(f"Events found: {offset}")
    except APIError as e:
        print(f"Could not pull data for cursor: {cursor}")

print("Getting data about the source lead for each merge event...")
pool = Pool(7)
pool.map(getSourceLeadData, events)

# Write data to a CSV
f = open(f'{org_name} Merge Lead Events in Last 30 Days.csv', 'wt')
try:
    ordered_keys = ['Merge Event ID', 'Close API Request ID', 'Date', 'User', 'Destination Lead Name', 'Destination Lead Status', 'Destination Lead ID', 'Source Lead Name', 'Source Lead Status', 'Source Lead ID', 'Current Lead URL']
    writer = csv.DictWriter(f, ordered_keys)
    writer.writeheader()
    writer.writerows(events)
finally:
    f.close()
