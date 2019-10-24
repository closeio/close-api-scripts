#!/usr/bin/env python

from __future__ import print_function

import argparse
import csv
import json
import re
import sys
import time

import closeio_api
import unidecode
from closeio_api import Client as CloseIO_API
from closeio_api.utils import count_lines, title_case, uncamel
from progressbar import ProgressBar
from progressbar.widgets import Percentage, Bar, ETA, FileTransferSpeed
from requests.exceptions import ConnectionError

parser = argparse.ArgumentParser(description='Import leads from CSV file')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--skip_duplicates', action='store_true', help='Skip leads that are already present in Close.io (determined by company name).')
parser.add_argument('--no_grouping', action='store_true', help='Turn off the default group-by-company behavior.')
parser.add_argument('--development', action='store_true', help='Use a development server rather than production.')
parser.add_argument('file', help='Path to the csv file')
args = parser.parse_args()

reader = csv.DictReader(open(args.file))
headers = reader.fieldnames

import_count = count_lines(args.file) # may have no trailing newline

cnt = success_cnt = 0

def warning(*objs):
    print("WARNING: ", *objs, file=sys.stderr)

def slugify(str, separator='_'):
    str = unidecode.unidecode(str).lower().strip()
    return re.sub(r'\W+', separator, str).strip(separator)

# Look for headers/columns that match these, case-insensitive. All other headers will be treated as custom fields.
expected_headers = (
    'company', # multiple contacts will be grouped if company names match
    'url',
    'status',
    'contact', # name of contact
    'title',
    'email',
    'phone', # recommended to start with "+" followed by country code (e.g., +1 650 555 1234)
    'mobile_phone',
    'fax',
    'address',
    'address_1', # if address is missing, address_1 and address_2 will be combined to create it.
    'address_2', # if address is missing, address_1 and address_2 will be combined to create it.
    'city',
    'state',
    'zip',
    'country',
)

# Remove trailing empty column headers
while not len(headers[-1].strip()):
    del headers[-1]

# Check for duplicated column names
if len(set(headers)) != len(headers):
    raise Exception('Cannot have duplicate column header names')

# Check for duplicates after normalization
normalized_headers = [slugify(col) for col in headers]
if len(set(normalized_headers)) != len(normalized_headers):
    raise Exception('After column header names were normalized there were duplicate column header names')

# build a map of header names -> index in actual header row
header_indices   = { col: i for (i, col) in enumerate(normalized_headers) } # normalized columns as keys
header_indices.update({col: i for (i, col) in enumerate(headers)}) # add in original column names as keys
expected_headers = [ col for col in normalized_headers if col in expected_headers ]
custom_headers = list(set(normalized_headers) - set(expected_headers)) # non-recognized fields in slug-ed format

# restore original version (capitalization) to custom fields
custom_headers = [headers[header_indices[normalized_col]] for normalized_col in custom_headers]

print("\nRecognized these column names:")
print(f'> {", ".join(expected_headers)}')
if len(custom_headers):
    print("\nThe following column names weren't recognized, and will be imported as custom fields:")
    print(f'> {", ".join(custom_headers)}')
    print('')

def lead_from_row(row):
    row = {column_name: column_value.strip() for column_name, column_value in row.items()}  # strip unnecessary white spaces
    
    # check if the row isn't empty
    has_data = {column_name: column_value for column_name, column_value in row.items() if column_value}
    if not has_data:
        return None

    lead = {
        'name': row['company'],
        'contacts': [],
        'custom': {}
    }

    if 'url' in row:
        lead['url'] = row['url']

    if 'status' in row:
        lead['status'] = row['status']

    if lead.get('url') and '://' not in lead['url']:
        lead['url'] = 'http://%s' % lead['url']

    # custom fields
    for field in custom_headers:
        if field in row:
            lead['custom'][field] = row[field]

    # address
    address = {}
    if 'address' in row:
        address['address'] = row['address']
    elif 'address_1' in row or 'address_2' in row:
        address['address'] = f'{row["address_1"]} {row["address_2"]}'.strip()
    if 'city' in row:
        address['city'] = title_case(row['city'])
    if 'state' in row:
        address['state'] = row['state']
    if 'zip' in row:
        address['zipcode'] = row['zip']
    if 'country' in row:
        address['country'] = row['country']
    if len(address):
        lead['addresses'] = [address]

    # contact
    contact = {}
    if 'contact' in row:
        contact['name'] = uncamel(row['contact'])
    if 'title' in row:
        contact['title'] = row['title']

    phones = []
    if 'phone' in row:
        phones.append({
            'phone': row['phone'],
            'type': 'office'
        })
    if 'mobile_phone' in row:
        phones.append({
            'phone': row['mobile_phone'],
            'type': 'mobile'
        })
    if 'fax' in row:
        phones.append({
            'phone': row['fax'],
            'type': 'fax'
        })
    if len(phones):
        contact['phones'] = phones

    emails = []
    if 'email' in row:
        emails.append({
            'email': row['email'],
            'type': 'office'
        })
    if len(emails):
        contact['emails'] = emails

    if len(contact):
        lead['contacts'] = [contact]

    return lead


# Create leads, grouped by company name
unique_leads = {}
for i, row in enumerate(reader):
    lead = lead_from_row(row)
    if not lead:
        continue

    if args.no_grouping:
        grouper = 'row-num-%s' % i
    else:
        # group by lead Name (company) if possible, otherwise put each row in its own lead
        grouper = lead['name'] if lead['name'] else ('row-num-%s' % i)

    if grouper not in unique_leads:
        unique_leads[grouper] = lead
    elif lead['contacts'] not in unique_leads[grouper]['contacts']:
        unique_leads[grouper]['contacts'].extend(lead['contacts'])

print(f'Found {len(unique_leads)} leads (grouped by company) from {import_count} contacts.')

print('\nHere is a sample lead (last row):')
print(json.dumps(unique_leads[grouper], indent=4))

print('\nAre you sure you want to continue? (y/n) ')
if input('') != 'y':
    sys.exit()

##############################################################################

api = CloseIO_API(args.api_key, development=args.development)

progress_widgets = ['Importing %d rows: ' % import_count, Percentage(), ' ', Bar(), ' ', ETA(), ' ', FileTransferSpeed()]
pbar = ProgressBar(widgets=progress_widgets, maxval=import_count).start()

dupes_cnt = 0

for key, val in unique_leads.items():
    retries = 5

    # check if it's a duplicate
    dupe = False
    if args.skip_duplicates and val.get('name'):

        # get the org id necessary for search
        org_id = api.get('api_key')['data'][0]['organization_id']

        # get all the search results for given lead name
        search_results = []
        filters = {
            'organization_id': org_id,
            'query': 'name:"%s"' % key,
        }
        has_more = True
        skip = 0
        while has_more:
            filters['_skip'] = skip
            resp = api.get('lead', params=filters)
            results = resp['data']
            search_results.extend(results)
            has_more = resp['has_more']
            skip += len(results)

        for result in search_results:
            if result['display_name'] == val['name']:
                dupe = True
                break

    while retries > 0:
        if dupe:
            dupes_cnt += 1
            warning('Duplicate - not importing: %s' % val['name'])
            break

        try:
            retries -= 1
            api.post('lead', val)
            retries = 0
            success_cnt += 1
        except closeio_api.APIError as err:
            warning('An error occurred while saving "%s"' % key)
            warning(err)
            retries = 0
        except ConnectionError as e:
            warning('Connection error occurred, retrying... (%d/5)' % retries)
            if retries == 0:
                raise
            time.sleep(2)

    cnt += 1
    if cnt > import_count:
        warning('Warning: count overflow')
        cnt = import_count
    pbar.update(cnt)

pbar.finish()

print(f'Successful responses: {success_cnt} of {len(unique_leads)}')
if args.skip_duplicates:
    print(f'Duplicates: {dupes_cnt}')

