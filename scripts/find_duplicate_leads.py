import argparse
import csv
import math
from operator import itemgetter
from urllib.parse import urlparse

import gevent.monkey
from closeio_api import Client as CloseIO_API
from gevent.pool import Pool

gevent.monkey.patch_all()

pool = Pool(7)

parser = argparse.ArgumentParser(
    description='Find duplicate leads in your Close org via lead name, email address, phone number, or lead url hostname'
)
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--field',
    '-f',
    default='all',
    choices=['lead_name', 'email', 'phone', 'url', 'all'],
    required=False,
    help="Specify a field to compare uniqueness",
)
args = parser.parse_args()

# Initialize Close API Wrapper
api = CloseIO_API(args.api_key)
org_id = api.get('api_key/' + args.api_key)['organization_id']
org_name = api.get('organization/' + org_id, params={'_fields': 'name'})[
    'name'
].replace('/', '')

# Calculate number of slices necessary to get all leads
total_leads = api.get('lead', params={'_limit': 0, 'query': 'sort:created'})[
    'total_results'
]
total_slices = int(math.ceil(float(total_leads) / 1000))
slices = range(1, total_slices + 1)
leads = []


# Write data to a CSV
def writeCSV(type_name, items, ordered_keys):
    print("Writing data to CSV...")
    f = open(f'{org_name} {type_name} Duplicates.csv', 'wt', encoding='utf-8')
    try:
        writer = csv.DictWriter(f, ordered_keys)
        writer.writeheader()
        writer.writerows(items)
    finally:
        f.close()

    # Get leads for each slice


def getLeadsSlice(slice_num):
    print(f"Getting lead slice {slice_num} of {total_slices}...")
    has_more = True
    offset = 0
    while has_more:
        resp = api.get(
            'lead',
            params={
                '_skip': offset,
                'query': 'sort:created slice:%s/%s'
                % (slice_num, total_slices),
                '_fields': 'id,display_name,contacts,status_label,date_created,url',
            },
        )
        for lead in resp['data']:
            leads.append(lead)
        offset += len(resp['data'])
        has_more = resp['has_more']


# Add to a list of duplicates for lead names
def getDuplicatesForLeadName(lead_name):
    for dupe in lead_names[lead_name]:
        lead_name_duplicates.append(
            {
                'Lead Name': dupe['display_name'],
                'Status Label': dupe['status_label'],
                'Lead ID': dupe['id'],
                'Lead Date Created': dupe['date_created'],
                'Close URL': 'https://app.close.com/lead/%s/' % dupe['id'],
            }
        )
    print(
        f"{(keys_with_dupes_lead_name.index(lead_name) + 1)} of {len(keys_with_dupes_lead_name)}: {lead_name}"
    )


# Add to a list of duplicates for contact emails
def getDuplicatesForEmail(email):
    for dupe in emails[email]:
        email_duplicates.append(
            {
                'Email Address': email,
                'Lead Name': dupe['display_name'],
                'Status Label': dupe['status_label'],
                'Lead ID': dupe['id'],
                'Lead Date Created': dupe['date_created'],
                'Close URL': 'https://app.close.com/lead/%s/' % dupe['id'],
            }
        )
    print(
        f"{(keys_with_dupes_email.index(email) + 1)} of {len(keys_with_dupes_email)}: {email}"
    )


# Add to a list of duplicates for contact phones
def getDuplicatesForPhone(phone):
    for dupe in phones[phone]:
        phone_duplicates.append(
            {
                'Phone Number': phone,
                'Lead Name': dupe['display_name'],
                'Status Label': dupe['status_label'],
                'Lead ID': dupe['id'],
                'Lead Date Created': dupe['date_created'],
                'Close URL': 'https://app.close.com/lead/%s/' % dupe['id'],
            }
        )
    print(
        f"{(keys_with_dupes_phone.index(phone) + 1)} of {len(keys_with_dupes_phone)}: {phone}"
    )


# Add to a list of duplicates for lead URLs
def getDuplicatesForURL(url):
    for dupe in urls[url]:
        url_duplicates.append(
            {
                'URL Hostname': url,
                'Lead Name': dupe['display_name'],
                'Status Label': dupe['status_label'],
                'Lead ID': dupe['id'],
                'Lead Date Created': dupe['date_created'],
                'Close URL': 'https://app.close.com/lead/%s/' % dupe['id'],
            }
        )
    print(
        f"{(keys_with_dupes_url.index(url) + 1)} of {len(keys_with_dupes_url)}: {url}"
    )


print("Getting Leads...")
pool.map(getLeadsSlice, slices)
leads = sorted(leads, key=itemgetter('date_created'))

# Process duplicates
lead_names = {}
emails = {}
phones = {}
urls = {}
keys_with_dupes_lead_name = []
keys_with_dupes_email = []
keys_with_dupes_phone = []
keys_with_dupes_url = []
for lead in leads:
    if args.field in ['all', 'lead_name']:
        # Pouplate a dictionary of duplicate lead names, and keep track of those that appear more than once
        lower_name = lead['display_name'].strip().lower()
        if lead_names.get(lower_name) and lead not in lead_names[lower_name]:
            lead_names[lower_name].append(lead)
            keys_with_dupes_lead_name.append(lower_name)
        elif not lead_names.get(lower_name):
            lead_names[lower_name] = [lead]

    if args.field in ['all', 'url']:
        # Pouplate a dictionary of duplicate lead urls, and keep track of those that appear more than once
        if lead.get('url'):
            host_name = urlparse(lead['url']).hostname.lower()
            if urls.get(host_name) and lead not in urls[host_name]:
                urls[host_name].append(lead)
                keys_with_dupes_url.append(host_name)
            elif not urls.get(host_name):
                urls[host_name] = [lead]

    if args.field in ['all', 'email', 'phone']:
        for contact in lead['contacts']:
            # Populate a dictionary of emails, and keep track of those that appear more than once
            if args.field in ['all', 'email']:
                for email in contact['emails']:
                    if (
                        emails.get(email['email'])
                        and lead not in emails[email['email']]
                    ):
                        emails[email['email']].append(lead)
                        keys_with_dupes_email.append(email['email'])
                    elif not emails.get(email['email']):
                        emails[email['email']] = [lead]

            # Populate a dictionary of phones, and keep track of those that appear more than once
            if args.field in ['all', 'phone']:
                for phone in contact['phones']:
                    if (
                        phones.get(phone['phone'])
                        and lead not in phones[phone['phone']]
                    ):
                        phones[phone['phone']].append(lead)
                        keys_with_dupes_phone.append(phone['phone'])
                    elif not phones.get(phone['phone']):
                        phones[phone['phone']] = [lead]

if args.field in ['all', 'lead_name']:
    lead_name_duplicates = []
    print("Getting lead name duplicate data...")
    keys_with_dupes_lead_name = list(set(keys_with_dupes_lead_name))
    pool.map(getDuplicatesForLeadName, keys_with_dupes_lead_name)

    # Sort the duplicates alphabetically and write them to a CSV
    lead_name_duplicates = sorted(
        lead_name_duplicates, key=itemgetter('Lead Name')
    )
    writeCSV(
        "Lead Name",
        lead_name_duplicates,
        [
            'Lead Name',
            'Status Label',
            'Lead Date Created',
            'Lead ID',
            'Close URL',
        ],
    )

if args.field in ['all', 'email']:
    email_duplicates = []
    print("Getting email duplicate data...")
    keys_with_dupes_email = list(set(keys_with_dupes_email))
    pool.map(getDuplicatesForEmail, keys_with_dupes_email)

    # Sort the duplicates alphabetically and write them to a CSV
    email_duplicates = sorted(
        email_duplicates, key=itemgetter('Email Address')
    )
    writeCSV(
        "Email",
        email_duplicates,
        [
            'Email Address',
            'Lead Name',
            'Status Label',
            'Lead Date Created',
            'Lead ID',
            'Close URL',
        ],
    )

if args.field in ['all', 'phone']:
    phone_duplicates = []
    print("Getting phone duplicate data...")
    keys_with_dupes_phone = list(set(keys_with_dupes_phone))
    pool.map(getDuplicatesForPhone, keys_with_dupes_phone)

    # Sort the duplicates alphabetically and write them to a CSV
    phone_duplicates = sorted(phone_duplicates, key=itemgetter('Phone Number'))
    writeCSV(
        "Phone",
        phone_duplicates,
        [
            'Phone Number',
            'Lead Name',
            'Status Label',
            'Lead Date Created',
            'Lead ID',
            'Close URL',
        ],
    )

if args.field in ['all', 'url']:
    url_duplicates = []
    print("Getting URL duplicate data...")
    keys_with_dupes_url = list(set(keys_with_dupes_url))
    pool.map(getDuplicatesForURL, keys_with_dupes_url)

    # Sort the duplicates alphabetically and write them to a CSV
    url_duplicates = sorted(url_duplicates, key=itemgetter('URL Hostname'))
    writeCSV(
        "URL",
        url_duplicates,
        [
            'URL Hostname',
            'Lead Name',
            'Status Label',
            'Lead Date Created',
            'Lead ID',
            'Close URL',
        ],
    )
