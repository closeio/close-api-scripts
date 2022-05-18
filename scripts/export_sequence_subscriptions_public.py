import argparse
import csv
import math

import gevent.monkey

gevent.monkey.patch_all()

from closeio_api import Client as CloseApi
from gevent.pool import Pool

pool = Pool(10)

arg_parser = argparse.ArgumentParser(description="Download a CSV of email sequence subscriptions")
arg_parser.add_argument("--api-key", "-k", required=True, help="API Key")
arg_parser.add_argument("--sequence-id", help="Fetch only subscriptions from this Sequence ID")
args = arg_parser.parse_args()

api = CloseApi(args.api_key)

csv_data = []


def get_sequences():
    sequences = []

    has_more = True
    offset = 0
    while has_more:
        resp = api.get('sequence')
        sequences.extend(resp['data'])
        offset += len(resp['data'])
        has_more = resp['has_more']

    return sequences


sequences = get_sequences()

query = "contact(sequence_subscription(sequence:*)) "

# Get the total number of slices
total_leads = api.get('lead', params={'_limit': 0, 'query': query})['total_results']
total_slices = int(math.ceil(float(total_leads) / 1000))
slices = range(1, total_slices + 1)


def get_leads_slice(slice_index):
    print(f"Getting lead slice {slice_index} of {total_slices}...")
    has_more = True
    offset = 0
    while has_more:
        resp = api.get(
            'lead',
            params={
                '_skip': offset,
                'query': f'sort:created slice:{slice_index}/{total_slices}',
                '_fields': 'id'
            },
        )
        leads.extend(resp['data'])

        offset += len(resp['data'])
        has_more = resp['has_more']


leads = []
pool.map(get_leads_slice, slices)


def fetch_sequence_subscriptions(lead):
    params = {"lead_id": lead["id"]}

    if args.sequence_id:
        params["sequence_id"] = args.sequence_id

    def get_sequence_subscriptions(params):
        subscriptions = []

        has_more = True
        offset = 0
        while has_more:
            resp = api.get('sequence_subscription', params=params)
            subscriptions.extend(resp['data'])
            offset += len(resp['data'])
            has_more = resp['has_more']

        return subscriptions

    all_subs.extend(get_sequence_subscriptions(params))


all_subs = []
pool.map(fetch_sequence_subscriptions, leads)

sequence_names = dict(zip([x["id"] for x in sequences], [x["name"] for x in sequences]))
for subscription in all_subs:
    csv_data.append(
        {
            "id": subscription["id"],
            "sequence_id": subscription["sequence_id"],
            "sequence_name": sequence_names.get(subscription["sequence_id"]),
            "contact_id": subscription["contact_id"],
            "contact_email": subscription["contact_email"],
            "sender_account_id": subscription["sender_account_id"],
            "sender_email": subscription["sender_email"],
            "sender_name": subscription["sender_name"],
            "status": subscription["status"],
            "pause_reason": subscription["pause_reason"],
        }
    )

keys = [
    "id",
    "sequence_id",
    "sequence_name",
    "contact_id",
    "contact_email",
    "sender_account_id",
    "sender_email",
    "sender_name",
    "status",
    "pause_reason",
]

org_name = api.get("me")["organizations"][0]['name']
with open(f"{org_name} - Sequence subscriptions.csv", "wt") as f:
    writer = csv.DictWriter(f, keys)
    writer.writeheader()
    writer.writerows(csv_data)
