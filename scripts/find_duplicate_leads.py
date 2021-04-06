import argparse
import csv
import math
from operator import itemgetter

import gevent.monkey

gevent.monkey.patch_all()
from urllib.parse import urlparse
from closeio_api import Client as CloseIO_API
from gevent.pool import Pool

pool = Pool(7)

parser = argparse.ArgumentParser(
    description='Find duplicate leads in your Close org via lead name, email address, phone number, or lead url hostname'
)
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--field',
    '-f',
    default='all',
    choices=[
        'lead_name',
        'contact_name',
        'email',
        'phone',
        'url',
        'all',
        'custom',
    ],
    help="Specify a field to compare uniqueness",
)
parser.add_argument(
    '--custom-field-name',
    '-c',
    help="Specify the custom field name if you're deduplicating by `custom` field",
)
args = parser.parse_args()

# Initialize Close API Wrapper
api = CloseIO_API(args.api_key)
organization = api.get('me')['organizations'][0]
org_id = organization['id']
org_name = organization['name']

# Calculate number of slices necessary to get all leads
total_leads = api.get('lead', params={'_limit': 0, 'query': 'sort:created'})[
    'total_results'
]
total_slices = int(math.ceil(float(total_leads) / 1000))
slices = range(1, total_slices + 1)
leads = []


# Write data to a CSV
def write_to_csv_file(type_name, items, ordered_keys):
    print("Writing data to CSV...")
    f = open(
        f'{org_name.replace("/", " ")} {type_name} Duplicates.csv',
        'wt',
        encoding='utf-8',
    )
    try:
        writer = csv.DictWriter(f, ordered_keys)
        writer.writeheader()
        writer.writerows(items)
    finally:
        f.close()


# Get leads for each slice
lead_params_fields = [
    'id',
    'display_name',
    'contacts',
    'status_label',
    'date_created',
    'url',
]
if args.field == 'custom':
    lead_params_fields += ['custom']

    if not args.custom_field_name:
        print(
            f"You need to provide custom field name while deduplicating by `custom`. Exiting..."
        )
        exit(1)


def get_leads_slice(slice_num):
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
                '_fields': ','.join(lead_params_fields),
            },
        )
        leads.extend(resp['data'])

        offset += len(resp['data'])
        has_more = resp['has_more']


# Add to a list of duplicates for lead names
def get_duplicates_for_lead_name(lead_name):
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


def get_duplicates_for_custom_field(custom_field_value):
    custom_field_name = args.custom_field_name

    for dupe in custom_fields[custom_field_value]:
        custom_field_duplicates.append(
            {
                f'custom.{custom_field_name}': custom_field_value,
                'Lead Name': dupe['display_name'],
                'Status Label': dupe['status_label'],
                'Lead ID': dupe['id'],
                'Lead Date Created': dupe['date_created'],
                'Close URL': 'https://app.close.com/lead/%s/' % dupe['id'],
            }
        )
    print(
        f"{(keys_with_dupes_custom_field.index(custom_field_value) + 1)} of {len(keys_with_dupes_custom_field)}: {custom_field_value}"
    )


# Add to a list of duplicates for contact emails
def get_duplicates_for_email(email):
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


# Add to a list of duplicates for contact emails
def get_duplicates_for_contact_name(contact_name):
    for dupe in contact_names[contact_name]:
        contact_name_duplicates.append(
            {
                'Contact Name': contact_name,
                'Lead Name': dupe['display_name'],
                'Status Label': dupe['status_label'],
                'Lead ID': dupe['id'],
                'Lead Date Created': dupe['date_created'],
                'Close URL': 'https://app.close.com/lead/%s/' % dupe['id'],
            }
        )
    print(
        f"{(keys_with_dupes_contact_name.index(contact_name) + 1)} of {len(keys_with_dupes_contact_name)}: {contact_name}"
    )


# Add to a list of duplicates for contact phones
def get_duplicates_for_phone(phone):
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
def get_duplicates_for_url(url):
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
pool.map(get_leads_slice, slices)
leads = sorted(leads, key=itemgetter('date_created'))

# Process duplicates
lead_names = {}
custom_fields = {}
contact_names = {}
emails = {}
phones = {}
urls = {}
keys_with_dupes_lead_name = []
keys_with_dupes_custom_field = []
keys_with_dupes_contact_name = []
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

    if args.field == 'custom':
        custom_field_value = lead['custom'].get(args.custom_field_name)
        if custom_field_value:
            if (
                custom_fields.get(custom_field_value)
                and lead not in custom_fields[custom_field_value]
            ):
                custom_fields[custom_field_value].append(lead)
                keys_with_dupes_custom_field.append(custom_field_value)
            elif not custom_fields.get(custom_field_value):
                custom_fields[custom_field_value] = [lead]

    if args.field in ['all', 'url']:
        # Pouplate a dictionary of duplicate lead urls, and keep track of those that appear more than once
        if lead.get('url'):
            host_name = urlparse(lead['url']).hostname.lower()
            if urls.get(host_name) and lead not in urls[host_name]:
                urls[host_name].append(lead)
                keys_with_dupes_url.append(host_name)
            elif not urls.get(host_name):
                urls[host_name] = [lead]

    if args.field in ['all', 'email', 'phone', 'contact_name']:
        for contact in lead['contacts']:
            if args.field in ['all', 'contact_name']:
                contact_name = contact['name'].strip().lower()
                if (
                    contact_names.get(contact_name)
                    and lead not in contact_names[contact_name]
                ):
                    contact_names[contact_name].append(lead)
                    keys_with_dupes_contact_name.append(contact_name)
                elif not contact_names.get(contact_name):
                    contact_names[contact_name] = [lead]

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
    pool.map(get_duplicates_for_lead_name, keys_with_dupes_lead_name)

    # Sort the duplicates alphabetically and write them to a CSV
    lead_name_duplicates = sorted(
        lead_name_duplicates, key=itemgetter('Lead Name')
    )
    write_to_csv_file(
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

if args.field == 'custom':
    custom_field_name = args.custom_field_name

    custom_field_duplicates = []
    print(f"Getting custom field `{custom_field_name}` duplicate data...")
    keys_with_dupes_lead_name = list(set(keys_with_dupes_custom_field))
    pool.map(get_duplicates_for_custom_field, keys_with_dupes_custom_field)

    # Sort the duplicates alphabetically and write them to a CSV
    custom_field_duplicates = sorted(
        custom_field_duplicates, key=itemgetter(f'custom.{custom_field_name}')
    )
    write_to_csv_file(
        f'Custom - {custom_field_name}',
        custom_field_duplicates,
        [
            f'custom.{custom_field_name}',
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
    pool.map(get_duplicates_for_email, keys_with_dupes_email)

    # Sort the duplicates alphabetically and write them to a CSV
    email_duplicates = sorted(
        email_duplicates, key=itemgetter('Email Address')
    )
    write_to_csv_file(
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

if args.field in ['all', 'contact_name']:
    contact_name_duplicates = []
    print("Getting contact duplicate data...")
    keys_with_dupes_contact_name = list(set(keys_with_dupes_contact_name))
    pool.map(get_duplicates_for_contact_name, keys_with_dupes_contact_name)

    # Sort the duplicates alphabetically and write them to a CSV
    contact_name_duplicates = sorted(
        contact_name_duplicates, key=itemgetter('Contact Name')
    )
    write_to_csv_file(
        "Contact Name",
        contact_name_duplicates,
        [
            'Contact Name',
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
    pool.map(get_duplicates_for_phone, keys_with_dupes_phone)

    # Sort the duplicates alphabetically and write them to a CSV
    phone_duplicates = sorted(phone_duplicates, key=itemgetter('Phone Number'))
    write_to_csv_file(
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
    pool.map(get_duplicates_for_url, keys_with_dupes_url)

    # Sort the duplicates alphabetically and write them to a CSV
    url_duplicates = sorted(url_duplicates, key=itemgetter('URL Hostname'))
    write_to_csv_file(
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
