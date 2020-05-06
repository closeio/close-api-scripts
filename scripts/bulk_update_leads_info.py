#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import logging
import re
import sys

from closeio_api import Client as CloseIO_API
from dateutil.parser import parse as parse_date

CUSTOM_FIELD_MULTIPLE_SELECT_VALUE_SEPARATOR = ';'
OPPORTUNITY_FIELDS = ['opportunity%s_note',
                      'opportunity%s_value',
                      'opportunity%s_value_period',
                      'opportunity%s_confidence',
                      'opportunity%s_status',
                      'opportunity%s_date_won']


def get_contact_info(contact_no, csv_row, what, contact_type):
    columns = [x for x in csv_row.keys()
               if re.match(r'contact%s_%s[0-9]' % (contact_no, what), x) and csv_row[x]]
    contact_info = []
    for col in columns:
        contact_info.append({what: csv_row[col], 'type': contact_type})
    return contact_info


def get_custom_fields(type):
    fields = []
    has_more = True
    offset = 0
    params = {}
    while has_more:
        resp = api.get(f'custom_fields/{type}', params=params)
        fields.extend(resp['data'])
        offset += len(resp['data'])
        has_more = resp['has_more']
    return fields


parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description="""
Imports leads and related data from a csv file with header.
Header's columns may be declared in any order. Detects csv dialect (delimeter and quotechar).

Any indexed attribute such as notes, addresses, opportunities, or contacts does NOT need to start from 0.
""", epilog="""
key columns:
    * lead_id                                   - If exists and not empty, update using lead_id.
    * company                                   - If lead_id is empty or does not exist, imports to
                                                  first lead from found company. If the company was
                                                  not found, loads as new lead.
    * email_address                             - If lead_id is empty or does not exist and company is empty, imports to
                                                  first lead from found email address. If the email address was
                                                  not found, loads as new lead.
    * unique.custom.[custom_field_name]         - If lead_is is empty or does not exist, imports to
                                                  first lead from found custom field.
lead columns:
    * url                                       - lead url
    * description                               - lead description
    * status                                    - lead status
    * note[0-9]                                 - lead notes
    * address[0-9]_country                      - ISO 3166-1 alpha-2 country code
    * address[0-9]_city                         - city
    * address[0-9]_zipcode                      - zipcode
    * address[0-9]_label                        - label (business, mailing, other)
    * address[0-9]_state                        - state
    * address[0-9]_address_1                    - text part 1
    * address[0-9]_address_2                    - text part 2
opportunity columns (new items will be added if all values filled):
    * opportunity[0-9]_note                     - opportunity note
    * opportunity[0-9]_value                    - opportunity value in cents
    * opportunity[0-9]_value_period             - will have a value like one_time or monthly
    * opportunity[0-9]_confidence               - opportunity confidence
    * opportunity[0-9]_status                   - opportunity status
    * opportunity[0-9]_date_won                 - opportunity date won
contact columns (new contacts wil be added):
    * contact[0-9]_name                         - contact name
    * contact[0-9]_title                        - contact title
    * contact[0-9]_phone[0-9]                   - contact phones
    * contact[0-9]_email[0-9]                   - contact emails
    * contact[0-9]_url[0-9]                     - contact urls
    * contact[0-9]_custom.[custom_field_name]   - value of custom_field_name; if multiple choice separate values with ;
lead custom columns (new custom field with type `text` will be created if it does not exist):
    * custom.[custom_field_name]                - value of custom_field_name; if multiple choice separate values with ;
""")

parser.add_argument('csvfile', type=argparse.FileType('rU'), help='csv file')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--confirmed', '-c', action='store_true', help='Without this flag, the script will do a dry run without actually updating any data.')
parser.add_argument('--create-custom-fields', '-f', action='store_true', help='Create new custom fields, if not exists.')
parser.add_argument('--disable-create', '-e', action='store_true', help='Prevent new lead creation. Update only exists leads.')
parser.add_argument('--continue-on-error', '-s', action='store_true', help='Do not abort import after first error')
args = parser.parse_args()

# Set up logging configuration
log_format = "[%(asctime)s] %(levelname)s %(message)s"
if not args.confirmed:
    log_format = f'DRY RUN: {log_format}'
logging.basicConfig(level=logging.INFO, format=log_format)
logging.debug(f'parameters: {vars(args)}')

# Sniff the dialect and get the CSV reader
sniffer = csv.Sniffer()
dialect = sniffer.sniff(args.csvfile.read(1000000))
args.csvfile.seek(0)
error_array = []
csv_reader = csv.DictReader(args.csvfile, dialect=dialect)

assert any(x in ('company', 'lead_id', 'email_address') or x.startswith('unique.custom.') for x in csv_reader.fieldnames), \
    'ERROR: column "company" or "lead_id" or "email_address" or a field starting with "unique.custom." is not found'

unique_field_name = next(iter([i for i in csv_reader.fieldnames if i.startswith('unique.custom.')]), None)

api = CloseIO_API(args.api_key)
org_id = api.get('me')['organizations'][0]['id']
org = api.get(f'organization/{org_id}')
org_name = org['name']

lead_custom_fields = get_custom_fields('lead')

csv_lead_custom_fieldnames = [y.split('.', 1)[1] for y in csv_reader.fieldnames if y.startswith('custom.')]
lead_new_custom_fieldnames = [x for x in csv_lead_custom_fieldnames if not any([y for y in lead_custom_fields if y['name'] == x])]
if lead_new_custom_fieldnames:
    if args.create_custom_fields:
        for field in lead_new_custom_fieldnames:
            if args.confirmed:
                resp = api.post('custom_fields/lead', data={'name': field, 'type': 'text'})
                lead_custom_fields.append(resp)
            else:
                lead_custom_fields.append({'name': field, 'type': 'text'})
            logging.info(f'added new custom field "{field}"')
    else:
        logging.error(f'unknown custom fieldnames: {lead_new_custom_fieldnames}')
        sys.exit(1)

logging.debug(f'available lead custom fields: {[x["name"] for x in lead_custom_fields]}')

updated_leads = 0
new_leads = 0
skipped_leads = 0

for row in csv_reader:
    # Skip all-empty rows
    if not any(row.values()):
        continue

    payload = {}
    if row.get('company'):
        payload['name'] = row['company']

    if row.get('url'):
        payload['url'] = row['url']

    if row.get('description'):
        payload['description'] = row['description']

    if row.get('status'):
        payload['status'] = row['status']

    # Contacts
    # Create them only if contact name is defined
    contact_indexes = [y[len('contact')] for y in row.keys() if re.match(r'contact[0-9]_name', y)]  # Extract the contact indexes if we have contact1_name, but missing contact0_name
    contacts = []
    for idx in contact_indexes:
        contact = {}
        if row.get(f'contact{idx}_name'):
            contact['name'] = row[f'contact{idx}_name']
        if row.get(f'contact{idx}_title'):
            contact['title'] = row[f'contact{idx}_title']
        phones = get_contact_info(idx, row, 'phone', 'office')
        if phones:
            contact['phones'] = phones
        emails = get_contact_info(idx, row, 'email', 'office')
        if emails:
            contact['emails'] = emails
        urls = get_contact_info(idx, row, 'url', 'url')
        if urls:
            contact['urls'] = urls

        if contact:
            contacts.append(contact)
    if contacts:
        payload['contacts'] = contacts

    # Addresses
    addresses_indexes = set([y[len('address')] for y in row.keys() if re.match(r'address[0-9]_*', y)])  # Extract the address indexes if we have address1_city, but missing address0_city
    addresses = []
    for idx in addresses_indexes:
        address = {}
        for z in ['country', 'city', 'zipcode', 'label', 'state', 'address_1', 'address_2']:
            if row.get(f'address{idx}_{z}'):
                address[z] = row[f'address{idx}_{z}']
        if address:
            addresses.append(address)
    if addresses:
        payload['addresses'] = addresses

    # Custom fields
    row_custom_fieldnames = [x.split('.', 1)[1] for x in row if x.startswith('custom.') and row[x]]  # Get the non-emtpy fields and remove the custom. prefix
    custom_fields = [x for x in lead_custom_fields if x['name'] in row_custom_fieldnames]  # Get the real custom field objects based on their names
    custom_patches = {}
    for custom_field in custom_fields:
        key = f'custom.{custom_field["name"]}'
        if custom_field.get('accepts_multiple_values'):
            custom_patches[key] = [i.strip() for i in row[key].split(CUSTOM_FIELD_MULTIPLE_SELECT_VALUE_SEPARATOR)]
        else:
            custom_patches[key] = row[key]
    payload.update(custom_patches)

    try:
        lead = None

        # Get the existing lead
        if row.get('lead_id') is not None:
            resp = api.get(f'lead/{row["lead_id"]}')
            logging.debug(f'received: {resp}')
            lead = resp
        elif row.get(unique_field_name) is not None:
            field = unique_field_name.replace("unique.custom.", "custom.")
            resp = api.get('lead', params={
                'query': f'"{field}":"{row[unique_field_name]}" sort:created',
                '_fields': 'id,display_name,name,contacts,custom',
                'limit': 1
            })
            logging.debug(f'received: {resp}')
            if resp['total_results']:
                lead = resp['data'][0]
        elif row.get('email_address') is not None:
            resp = api.get('lead', params={
                'query': f'email_address:"{row["email_address"]}" sort:created',
                '_fields': 'id,display_name,name,contacts,custom',
                'limit': 1
            })
            logging.debug(f'received: {resp}')
            if resp['total_results']:
                lead = resp['data'][0]
        else:
            resp = api.get('lead', params={
                'query': f'company:"{row["company"]}" sort:created',
                '_fields': 'id,display_name,name,contacts,custom',
                'limit': 1
            })
            logging.debug(f'received: {resp}')
            if resp['total_results']:
                lead = resp['data'][0]

        # If it exist, update it
        if lead:
            logging.debug(f'to sent: {payload}')
            if args.confirmed:
                # Append multi-select custom field values to existing lead values
                for custom_field in lead_custom_fields:
                    key = custom_field["name"]
                    if custom_field.get('accepts_multiple_values') and payload.get(f'custom.{key}') and lead['custom'].get(key):
                        payload[f'custom.{key}'] = lead['custom'][key] + payload[f'custom.{key}']

                api.put(f'lead/{lead["id"]}', data=payload)
            logging.info(f'line {csv_reader.line_num} updated: {lead["id"]} {lead.get("name") if lead.get("name") else ""}')
            updated_leads += 1

        # If it doesn't exist, create it if disable_create is false
        elif lead is None and not args.disable_create:
            logging.debug(f'to sent: {payload}')
            if args.confirmed:
                lead = api.post('lead', data=payload)
                logging.info(f'line {csv_reader.line_num} new: {lead["id"] if args.confirmed else "X"} {lead["display_name"]}')
            else:
                company_name = row["company"] if row.get("company") else row.get("email_address") or row.get(unique_field_name)
                logging.info(f'line {csv_reader.line_num} new lead for: {company_name}')
            new_leads += 1

        # If it doesn't exist and disable_create it true - skip it
        elif lead is None and args.disable_create:
            row['Validation Error'] = 'Lead does not exist in Close'
            skipped_leads += 1

            company_name = row["company"] if row.get("company") else row.get("email_address") or row.get(unique_field_name)
            logging.info(f'line {csv_reader.line_num} skipped: {company_name} does not exist in Close.io')
            error_array.append(row)
            continue

        # Notes
        notes = [row[x] for x in row.keys() if re.match(r'note[0-9]', x) and row[x]]
        for note in notes:
            if args.confirmed:
                resp = api.post('activity/note', data={'note': note, 'lead_id': lead['id']})
            logging.debug(f'{lead["id"] if args.confirmed else "X"} new note: {note}')

        # Opportunities
        opportunity_ids = {x[len('opportunity')] for x in csv_reader.fieldnames if re.match(r'opportunity[0-9]', x)}  # Extract the opportunity indexes if we have opportunity1_note, but missing opportunity0_note
        for i in opportunity_ids:
            if all([row.get(x % i) for x in OPPORTUNITY_FIELDS]):
                if row[f'opportunity{i}_value_period'] not in ('one_time', 'monthly', 'annual'):
                    value_period = row[f"opportunity{i}_value_period"]
                    logging.error(f'line {csv_reader.line_num} invalid value_period "{value_period}" for opportunity {i}')
                    continue

                opp_payload = {
                    'lead_id': lead['id'],
                    'note': row.get(f'opportunity{i}_note'),
                    'value': int(row[f'opportunity{i}_value']),  # assumes cents are given
                    'value_period': row.get(f'opportunity{i}_value_period'),
                    'confidence': int(row[f'opportunity{i}_confidence']),
                    'status': row.get(f'opportunity{i}_status'),
                    'date_won': str(parse_date(row[f'opportunity{i}_date_won']))
                }
                if args.confirmed:
                    api.post('opportunity', data=opp_payload)
            else:
                logging.error(f'line {csv_reader.line_num} is not a fully filled opportunity {i}, skipped')
    except Exception as e:
        logging.error(f'line {csv_reader.line_num} skipped with error {e}')
        skipped_leads += 1
        row['Validation Error'] = e
        error_array.append(row)
        if not args.continue_on_error:
            logging.info('stopped on error')
            sys.exit(1)

logging.info(f'summary: updated[{updated_leads}], new[{new_leads}], skipped[{skipped_leads}]')

if error_array:
    f = open(f'{org_name} Bulk Update Errored Rows.csv', 'wt', encoding='utf-8')
    try:
        keys = ['Validation Error'] + csv_reader.fieldnames
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(error_array)
    finally:
        f.close()
