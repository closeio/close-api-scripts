# -*- coding: utf-8 -*-

import re
import argparse
import csv
import logging
from closeio_api import Client as CloseIO_API, APIError


def get_contact_info(contact_no, csv_row, what, contact_type):
    columns = [x for x in csv_row.keys()
               if re.match(r'contact%s_%s[0-9]' % (contact_no, what), x) and csv_row[x]]
    contact_info = []
    for col in columns:
        contact_info.append({what: csv_row[col], 'type': contact_type})
    return contact_info

parser = argparse.ArgumentParser(description='')
parser.add_argument('csvfile', type=argparse.FileType('rU'), help='csv file')
parser.add_argument('--api_key', '-k', required=True, help='API Key')
parser.add_argument('--development', '-d', action='store_true',
                    help='Use a development (testing) server rather than production.')
parser.add_argument('--confirmed', '-c', action='store_true',
                    help='Without this flag, the script will do a dry run without actually updating any data.')
parser.add_argument('--create-custom-fields', '-C', action='store_true',
                    help='Create new custom fields, if not exists.')
parser.add_argument('--disable-create', '-e', action='store_true',
                    help='Prevent new lead creation. Update only exists leads.')
args = parser.parse_args()

log_format = "[%(asctime)s] %(levelname)s %(message)s"
if not args.confirmed:
    log_format = 'DRY RUN: '+log_format
logging.basicConfig(level=logging.DEBUG, format=log_format)
logging.debug('parameters: %s' % vars(args))

sniffer = csv.Sniffer()
dialect = sniffer.sniff(args.csvfile.read(1024))
args.csvfile.seek(0)
c = csv.DictReader(args.csvfile, dialect=dialect)
assert any(x in ('company', 'lead_id') for x in c.fieldnames), \
    'ERROR: column "company" or "lead_id" is not found'


api = CloseIO_API(args.api_key, development=args.development)

resp = api.get('custom_fields/lead')
available_custom_fieldnames = [x['name'] for x in resp['data']]
new_custom_fieldnames = [x for x in [y.split('.')[1] for y in c.fieldnames if y.startswith('custom.')]
                         if x not in available_custom_fieldnames]

if args.create_custom_fields:
    for field in new_custom_fieldnames:
        if args.confirmed:
            api.post('custom_fields/lead', data={'name': field, 'type': 'text'})
        available_custom_fieldnames.append(field)
        logging.info('added new custom field "%s"' % field)

logging.debug('avaliable custom fields: %s' % available_custom_fieldnames)

for r in c:
    payload = {}

    if r.get('company'):
        payload['name'] = r['company']

    if r.get('url'):
        payload['url'] = r['url']

    contacts = []
    for x in [y[7] for y in r.keys() if re.match(r'contact[0-9]_name', y) and r[y]]:
        contact = {'name': r['contact%s_name' % x]}
        if r.get('contact%s_title' % x):
            contact['title'] = r['contact%s_title' % x]
        phones = get_contact_info(x, r, 'phone', 'office')
        if phones:
            contact['phones'] = phones
        emails = get_contact_info(x, r, 'email', 'office')
        if emails:
            contact['emails'] = emails
        urls = get_contact_info(x, r, 'url', 'url')
        if urls:
            contact['url'] = urls
        contacts.append(contact)
    if contacts:
        payload['contacts'] = contacts

    custom = {x.split('.')[1]: r[x] for x in r.keys() if x.startswith('custom.')
              and x.split('.')[1] in available_custom_fieldnames and r[x]}
    if custom:
        payload['custom'] = custom

    lead = None
    try:
        if r.get('lead_id') is not None:
            # exists lead
            resp = api.get('lead/%s' % r['lead_id'], data={
                'fields': 'id'
            })
            lead = resp['data']
        else:
            # first lead in the company
            resp = api.get('lead', data={
                'query': 'company:"%s" sort:created' % r['company'],
                '_fields': 'id,display_name,name,contacts,custom',
                'limit': 1
            })
            if resp['total_results']:
                lead = resp['data'][0]
    except APIError as e:
        logging.error('line: %d : %s' % (c.line_num, e))
        continue

    if lead:
        logging.debug(payload)
        if args.confirmed:
            api.put('lead/' + lead['id'], data=payload)
        logging.info('line: %d updated: %s %s' % (c.line_num, lead['id'], lead['name']))
        continue

    # new lead
    if lead is None and not args.disable_create:
        try:
            logging.debug(payload)
            if args.confirmed:
                resp = api.post('lead', data=payload)
            logging.info('line %d new: %s %s' % (c.line_num, resp['id'] if args.confirmed else 'X', payload['name']))
        except APIError as e:
            logging.error('line: %d skipped with error %s' % (c.line_num, e))
