import argparse
import csv
import math

import gevent.monkey

gevent.monkey.patch_all()
from gevent.pool import Pool

pool = Pool(7)

from closeio_api import Client as CloseApi

arg_parser = argparse.ArgumentParser(description="Download a CSV of SMS messages over a specified time range")
arg_parser.add_argument("--api-key", "-k", required=True, help="API Key")
arg_parser.add_argument(
    "--start-date",
    "-s",
    help="The start of the date range you want to export SMS data for in yyyy-mm-dd format (inclusive).",
)
arg_parser.add_argument(
    "--end-date",
    "-e",
    help="The end of the date range you want to export SMS data for in yyyy-mm-dd format (exclusive).",
)
arg_parser.add_argument(
    "--user",
    "-u",
    help="Use this field if you only want to find SMS for a specific users - enter email, ID, or name",
)
arg_parser.add_argument(
    "--direction",
    "-d",
    default=None,
    choices=["inbound", "outbound"],
    help="Use this field to only export inbound SMS or outbound SMS. Leave this field blank to export both.",
)
arg_parser.add_argument(
    "--status",
    default=None,
    choices=["error", "inbox", "draft", "scheduled", "outbox", "sent"],
    help="Use this field to only export SMS in specific status.",
)
arg_parser.add_argument("--smart-view", help="Export SMS messages only for leads in a specific Smart View")
args = arg_parser.parse_args()

api = CloseApi(args.api_key)

organization = api.get("me")["organizations"][0]

sms_messages_fields = ['id', 'direction', 'local_phone', 'remote_phone', 'lead_id', 'contact_id', 'user_id',
                       'user_name', 'date_created', 'text', 'status', 'cost', 'source']
sms_messages_params = {
    "_fields": ','.join(sms_messages_fields)
}

if args.user:
    def get_membership(user_identifier):
        resp = api.get(f"organization/{organization['id']}", params={"_fields": "memberships,inactive_memberships"})
        memberships = resp["memberships"] + resp["inactive_memberships"]

        if user_identifier.startswith("user_"):
            return next(iter(x for x in memberships if x["user_id"] == user_identifier), None)
        elif "@" in user_identifier:
            return next(iter(x for x in memberships if x["user_email"] == user_identifier), None)
        else:
            return next(
                iter(x for x in memberships if x["user_full_name"] == user_identifier),
                None,
            )


    user = get_membership(args.user)
    if not user:
        print(f"Couldn't find user `{args.user}` in organization `{organization['name']}`")
        exit()

    sms_messages_params["user_id"] = user["user_id"]
else:
    user = None

query = ""

if args.start_date:
    query += f' date >= "{args.start_date}"'

if args.end_date:
    query += f' date < "{args.end_date}"'

if args.status:
    query += f" status:{args.status} "

if args.direction:
    query += f" direction:{args.direction} "

if user:
    query += f" user:{user['user_id']} "

if query:
    query = f"sms({query})"
else:
    query = "sms_messages > 0"

if args.smart_view:
    query += f' in:"{args.smart_view}"'

print("Getting Leads...")
print(f'\t{query}')

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


leads = get_all_leads_with_slices(params={"query": query, "_fields": "id,display_name"})

lead_id_to_name = {}
for lead in leads:
    lead_id_to_name[lead["id"]] = lead["display_name"]

print("Getting SMS messages...")


def get_sms_messages_for_lead(lead):
    sms_params = sms_messages_params.copy()
    sms_params["lead_id"] = lead["id"]

    if args.start_date:
        sms_params["date_created__gt"] = args.start_date
    if args.end_date:
        sms_params["date_created__lt"] = args.end_date

    sms_messages.extend(get_all("activity/sms", params=sms_params))


sms_messages = []
pool.map(get_sms_messages_for_lead, leads)

# Sort by newest first
sms_messages.sort(key=lambda x: x["date_created"], reverse=True)

if args.direction:
    sms_messages = [i for i in sms_messages if i["direction"] == args.direction]

if args.status:
    sms_messages = [i for i in sms_messages if i["status"] == args.status]

for sms_message in sms_messages:
    sms_message["lead_name"] = lead_id_to_name.get(sms_message.get("lead_id"), "")

    if sms_message.get("cost"):
        sms_message["formatted_cost"] = f"${(float(sms_message['cost']) / 100)}"

# Write to CSV
file_name = f"{organization['name']} SMS messages.csv"

with open(file_name, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, sms_messages_fields + ['lead_name', 'formatted_cost'])
    writer.writeheader()
    writer.writerows(sms_messages)

print(f'Done! Report is saved to `{file_name}`')
