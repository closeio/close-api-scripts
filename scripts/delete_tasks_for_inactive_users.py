#!/usr/bin/env python

import argparse
import sys

from closeio_api import Client as CloseIO_API

parser = argparse.ArgumentParser(
    description='Remove tasks associated with inactive users'
)
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--confirmed',
    action='store_true',
    help='Confirm making changes. Otherwise this script is not going to modify any data.',
)
parser.add_argument(
    '--verbose', '-v', action='store_true', help='Increase logging verbosity.'
)
args = parser.parse_args()

api = CloseIO_API(args.api_key)

# Get IDs of all inactive users in a given org
org_id = api.get('me')['organizations'][0]['id']

org = api.get(f'organization/{org_id}')
inactive_users = [m['user_id'] for m in org['inactive_memberships']]

# Get IDs of all the tasks assigned to these inactive users
task_ids = []
total_cnt = len(inactive_users)
for idx, user_id in enumerate(inactive_users):
    if args.verbose:
        print(f'Gathering tasks for {user_id} ({(idx + 1)}/{total_cnt})')

    has_more = True
    skip = 0
    limit = 100
    while has_more:
        resp = api.get(
            'task',
            params={
                'assigned_to': user_id,
                '_skip': skip,
                '_limit': limit,
                '_fields': 'id',
            },
        )
        task_ids.extend(t['id'] for t in resp['data'])
        has_more = resp['has_more']
        skip += limit

if args.verbose:
    print(f'Found {len(task_ids)} tasks')

if not args.confirmed:
    print(
        'This is a dry run, so the tasks are not deleted. Use the --confirmed flag to delete them.'
    )
    sys.exit(0)

total_cnt = len(task_ids)
for idx, task_id in enumerate(task_ids):
    api.delete('task/' + task_id)
    if args.verbose:
        print(f'Deleting {(idx + 1)}/{total_cnt}')
