import argparse
import csv
import math

import gevent.monkey

gevent.monkey.patch_all()
from gevent.pool import Pool

pool = Pool(7)

from closeio_api import Client as CloseApi

parser = argparse.ArgumentParser(
    description='Download a CSV of calls from/to a specific Close number over a specified time range'
)

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--direction',
    '-d',
    default=None,
    choices=['inbound', 'outbound'],
    help='Use this field to only export inbound calls or outbound calls. Leave this field blank to export both.',
)
parser.add_argument(
    '--missed-or-voicemail',
    '-m',
    action='store_true',
    help='Use this field to only export missed calls, voicemails, or calls of a duration 0',
)
parser.add_argument(
    '--end-date',
    '-e',
    help='The end of the date range you want to export call data for in yyyy-mm-dd format.',
)
parser.add_argument(
    '--start-date',
    '-s',
    help='The start of the date range you want to export call data for in yyyy-mm-dd format.',
)
parser.add_argument(
    '--phone-number',
    '-p',
    help='The phone number you\'d like to export the calls for in E164 international format. Example: +18552567346',
)
parser.add_argument(
    '--user-id',
    '-u',
    help='Use this field if you only want to find calls for a specific user',
)
parser.add_argument(
    '--call-costs',
    '-c',
    action='store_true',
    help='Use this field if you want to include a call cost column in your export CSV',
)
parser.add_argument(
    '--transcripts',
    '-t',
    action='store_true',
    help='Use this field if you want to include a call transcript column in your export CSV',
)

args = parser.parse_args()

api = CloseApi(args.api_key)

params = {}

if not args.start_date and not args.end_date:
    lead_query = 'has:calls'
else:
    lead_query = 'call('

    if args.start_date:
        params['date_created__gte'] = args.start_date
        lead_query += f' date >= "{args.start_date}"'

    if args.end_date:
        params['date_created__lt'] = args.end_date
        lead_query += f' date < "{args.end_date}"'

    lead_query += ")"

if args.user_id:
    params['user_id'] = args.user_id

print("Getting Leads...")
print(f'\t{lead_query}')

def get_all(url, params=None):
    if params is None:
        params = {}

    items = []
    has_more = True
    offset = 0
    while has_more:
        params["_skip"] = offset
        resp = api.get(url, params=params)
        items.extend(resp['data'])
        offset += len(resp["data"])
        has_more = resp["has_more"]
    return items

def get_all_leads_with_slices(params, slice_size=500):
    leads = []

    total_leads = api.get("lead", params={"_limit": 0, "query": params["query"], "_fields": "id"})[
        "total_results"]
    total_slices = int(math.ceil(float(total_leads) / slice_size))

    slices = []
    for slice_number in range(1, total_slices + 1):
        slices.append({"total_slices": total_slices, "slice": slice_number, "params": params})

    def _get_all_leads_slice(slice_obj):
        params = slice_obj["params"]

        new_params = params.copy()
        new_params["query"] = f'({params["query"]}) slice:{slice_obj["slice"]}/{slice_obj["total_slices"]}'

        leads.extend(get_all("lead", params=new_params))

    pool.map(_get_all_leads_slice, slices)

    return leads


leads = get_all_leads_with_slices(params={"query": lead_query, "_fields": "id,contacts,display_name"})

lead_id_to_name = {}
contacts_id_to_name = {}
for lead in leads:
    lead_id_to_name[lead["id"]] = lead["display_name"]
    for contact in lead["contacts"]:
        contacts_id_to_name[contact["id"]] = contact['name']

call_fields = [
    'id', 'user_id', 'duration', 'disposition', 'status', 'direction', 'date_created', 'remote_phone', 'local_phone',
    'voicemail_url', 'recording_url', 'source', 'lead_id', 'updated_by_name', 'contact_id',
]

if args.call_costs:
    call_fields += 'cost'

if args.transcripts:
    call_fields += ['recording_transcript']

params['_fields'] = ','.join(call_fields)

print("Getting Calls...")
calls = get_all("activity/call", params=params)

# Add lead names and formatted costs
for call in calls:
    call['lead_name'] = lead_id_to_name.get(call.get('lead_id'), '')
    call['contact_name'] = contacts_id_to_name.get(call.get('contact_id'), '')

    if call.get('cost'):
        call['formatted_cost'] = f"${(float(call['cost']) / 100)}"
    if call.get('recording_transcript'):
        call['recording_transcript'] = call.get('recording_transcript').get('summary_text')

# Filter calls
if args.missed_or_voicemail:
    calls = [i for i in calls if i['duration'] == 0]

if args.direction:
    calls = [i for i in calls if i['direction'] == args.direction]

if args.phone_number:
    calls = [i for i in calls if i['local_phone'] == args.phone_number]

# Write to CSV
organization = api.get('me')['organizations'][0]
organization_name = organization['name'].replace('/', "")
file_name = f'{organization_name} Calls.csv'

with open(file_name, 'w', newline='', encoding='utf-8') as f:
    keys = call_fields + ['lead_name', 'contact_name']
    if args.call_costs:
        keys += ['cost', 'formatted_cost']
    writer = csv.DictWriter(f, keys)
    writer.writeheader()
    writer.writerows(calls)

print(f'Done! Report is saved to `{file_name}`')