#!/usr/bin/env python

import argparse
import csv
import sys

from closeio_api import APIError, Client as CloseIO_API

parser = argparse.ArgumentParser(description='Remove email addresses from contacts in CSV file')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--confirmed', action='store_true', help='Confirm making changes. Otherwise this script is not going to modify any data.')
parser.add_argument('--verbose', '-v', action='store_true', help='Increase logging verbosity.')
parser.add_argument('file', help='Path to the csv file')
args = parser.parse_args()

reader = csv.DictReader(open(args.file))
if any(field not in reader.fieldnames for field in ['contact_id', 'email_address']):
    print('contact_id or email_address headers could not be found in your csv file.')
    sys.exit(-1)

api = CloseIO_API(args.api_key)

for row in reader:
    contact_id = row['contact_id']
    email_address = row['email_address']

    if args.verbose:
        print(f'Attempting to remove {email_address} from {contact_id}')

    try:
        contact = api.get('contact/' + contact_id)

        if not contact['emails']:
            if args.verbose:
                print(f'Skipping {contact_id} because it has no email addresses')
            continue

        emails = list(filter(lambda email: email['email'] != email_address, contact['emails']))
        if args.confirmed:
            resp = api.put('contact/' + contact_id, {'emails': emails})
            if args.verbose:
                print(f'Removed {email_address} from {contact_id}')
    except APIError as e:
        if args.verbose:
            print(f'Encountered an API error ({e.response.status_code}): {e.response.text}')
