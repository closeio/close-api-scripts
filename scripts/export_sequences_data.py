import argparse
import csv

import gevent.monkey

gevent.monkey.patch_all()
from closeio_api import Client as CloseIO_API
from gevent.pool import Pool

parser = argparse.ArgumentParser(
    description='Download a CSV of email sequences and their subscription counts (number of active/paused/finished subscriptions)'
)

parser.add_argument('--api-key', '-k', required=True, help='API Key')
args = parser.parse_args()

api = CloseIO_API(args.api_key)
org_name = api.get('me')['organizations'][0]['name']

print('Getting email sequences...')

params = {'_fields': 'id'}
has_more = True
offset = 0
sequence_ids = []
while has_more:
    params['_skip'] = offset
    resp = api.get('sequence', params=params)
    for sequence in resp['data']:
        sequence_ids.append(sequence['id'])
    offset += len(resp['data'])
    has_more = resp['has_more']

print(f'Found {len(sequence_ids)} email sequences. Getting their details...')


def fetch_sequence(sequence_id):
    resp = api.get(f'sequence/{sequence_id}')

    counts_by_status = resp['subscription_counts_by_status']
    active_subscriptions = counts_by_status['active']
    paused_subscriptions = counts_by_status['paused']
    finished_subscriptions = counts_by_status['finished']
    total_subscriptions = (
        active_subscriptions + paused_subscriptions + finished_subscriptions
    )

    sequences.append(
        {
            'id': resp['id'],
            'name': resp['name'],
            'is_active': resp['status'] == 'active',
            'total_subscriptions': total_subscriptions,
            'active_subscriptions': active_subscriptions,
            'paused_subscriptions': paused_subscriptions,
            'finished_subscriptions': finished_subscriptions,
        }
    )


sequences = []

pool = Pool(5)
pool.map(fetch_sequence, sequence_ids)

file_name = f'{org_name.replace("/", " ")} Email Sequences.csv'
print(f'Exporting to `{file_name}`')

f = open(file_name, 'wt', encoding='utf-8')
try:
    keys = [
        'id',
        'name',
        'is_active',
        'total_subscriptions',
        'active_subscriptions',
        'paused_subscriptions',
        'finished_subscriptions',
    ]
    writer = csv.DictWriter(f, keys)
    writer.writeheader()
    writer.writerows(sequences)
finally:
    f.close()
