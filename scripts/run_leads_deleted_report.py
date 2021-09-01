import argparse
import csv

from closeio_api import Client as CloseIO_API

parser = argparse.ArgumentParser(
    description='Create a CSV of all deleted leads in the past 30 days and see how they were deleted'
)

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--print-lead-ids',
    '-p',
    action='store_true',
    help='Use this field to print lead_ids deleted in an array at the end of the script',
)

args = parser.parse_args()

api = CloseIO_API(args.api_key)

has_more = True
cursor = ''
events = []
leads = []
reverted_imports = {}

api_key_info = api.get('api_key/' + args.api_key)
org = api.get(
    'organization/' + api_key_info['organization_id'],
    params={'_fields': 'name,memberships,inactive_memberships'},
)
org_memberships = org['memberships'] + org['inactive_memberships']
org_name = org['name']

api_key_user_info = next(
    (x for x in org_memberships if api_key_info['user_id'] == x['user_id']),
    None,
)

assert (
    'role' in api_key_user_info and api_key_user_info['role'] == 'admin'
), 'ERROR: You must be an admin in your Close.io organization to run this script'

users = {}

for member in org_memberships:
    users[member['user_id']] = member['user_full_name']

print("Getting Leads deleted...")

while has_more:
    resp = api.get(
        'event',
        params={'object_type': 'lead', 'action': 'deleted', '_cursor': cursor},
    )
    for event in resp['data']:
        if args.print_lead_ids:
            leads.append(event['lead_id'])

        event_data = {
            'username': "",
            'date_created': event['date_created'],
            'display_name': event['previous_data']['display_name'],
            'lead_status': event['previous_data']['status_label'],
            'lead_id': event['lead_id'],
            'how_deleted': "",
        }

        if 'meta' in event:
            if 'bulk_action_id' in event['meta']:
                event_data['how_deleted'] = "Bulk Delete via Close.io (%s)" % (
                    event['meta']['bulk_action_id']
                )
            elif 'merge_source_lead_id' in event['meta']:
                event_data['how_deleted'] = "Merged with another lead (%s)" % (
                    event['meta']['merge_destination_lead_id']
                )
            elif 'revert_import_id' in event['meta']:
                event_data[
                    'how_deleted'
                ] = "A Close.io Import Was Reverted (%s)" % (
                    event['meta']['revert_import_id']
                )
                if event['meta']['revert_import_id'] not in reverted_imports:
                    reverted_import_activities = api.get(
                        'event',
                        params={
                            'object_type': 'import',
                            'object_id': event['meta']['revert_import_id'],
                        },
                    )
                    import_deletions = [
                        i
                        for i in reverted_import_activities['data']
                        if i['action'] == 'deleted'
                    ]
                    if (
                        len(import_deletions) > 0
                        and 'user_id' in import_deletions[0]
                    ):
                        reverted_imports[
                            event['meta']['revert_import_id']
                        ] = import_deletions[0]['user_id']
                if event['meta']['revert_import_id'] in reverted_imports:
                    event_data['username'] = users[
                        reverted_imports[event['meta']['revert_import_id']]
                    ]
            else:
                event_data[
                    'how_deleted'
                ] = "Manually in Close.io or via a single API Call"

        if (
            'user_id' in event
            and event['user_id'] != None
            and event_data['username'] == ""
        ):
            event_data['username'] = users[event['user_id']]

        events.append(event_data)
    print(len(events))
    cursor = resp['cursor_next']
    has_more = bool(cursor)

f = open(f'{org_name} Delete Lead Events in 30 Days.csv', 'w', newline='', encoding='utf-8')
try:
    writer = csv.writer(f)
    writer.writerow(
        (
            'Date',
            'User',
            ' Lead Name',
            ' Lead Status',
            'Lead ID',
            'How Was Lead Deleted?',
        )
    )
    for a in events:
        writer.writerow(
            (
                '%s' % a['date_created'],
                '%s' % a['username'],
                '%s' % a['display_name'],
                '%s' % a['lead_status'],
                '%s' % a['lead_id'],
                '%s' % a['how_deleted'],
            )
        )
finally:
    f.close()

if args.print_lead_ids:
    print(f"Total Leads: {len(leads)}")
    print(leads)
