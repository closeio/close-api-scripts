import re
import argparse
import csv
import logging
from closeio_api import Client as CloseIO_API, APIError

get_contact_info = lambda key, row, what, typ: [{what: row[x], 'type': typ} for x in row.keys()
                                                if re.match(r'contact%s_%s[0-9]' % (key, what), x) and row[x]]

parser = argparse.ArgumentParser(description='')
parser.add_argument('csvfile', type=argparse.FileType('rU'), help='csv file')
parser.add_argument('--api_key', '-k', required=True, help='API Key')
parser.add_argument('--development', '-d', action='store_true',
                    help='Use a development (testing) server rather than production.')
parser.add_argument('--confirmed', '-c', action='store_true',
                    help='Without this flag, the script will do a dry run without actually updating any data.')
parser.add_argument('--disable-create', '-x', action='store_true',
                    help='Prevent new lead creation. Update only exists leads.')
args = parser.parse_args()

log_format = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
if not args.confirmed:
    log_format = 'DRY RUN: '+log_format
logging.basicConfig(level=logging.INFO, format=log_format)
logging.debug('parameters: %s' % vars(args))

sniffer = csv.Sniffer()
dialect = sniffer.sniff(args.csvfile.read(1024))
args.csvfile.seek(0)
c = csv.DictReader(args.csvfile, dialect=dialect)

api = CloseIO_API(args.api_key, development=args.development)

for r in c:
    assert any(x in ('company', 'lead_id') for x in r.keys()), \
        'error: column company or lead_id not found at line %d' % (c.line_num,)

    payload = {'name': r['company'],
               'url': r.get('url'),
               'contacts': [{'name': r['contact%s_name' % x],
                            'title': r['contact%s_title' % x],
                            'phones': get_contact_info(x, r, 'phone', 'office'),
                            'emails': get_contact_info(x, r, 'email', 'office'),
                            'urls': get_contact_info(x, r, 'url', 'url')}
                            for x in [y[7] for y in r.keys()
                                      if re.match(r'contact[0-9]_name', y) and r[y]]],
               }

    custom = {x.split('.')[1]: r[x] for x in [y for y in r.keys() if y.startswith('custom.')]}

    lead = None
    # exists lead
    if r.get('lead_id') is not None:
        try:
            resp = api.get('lead/%s' % r['lead_id'], data={
                'fields': 'id'
            })

            lead = resp['data']
            if resp['total_results']:
                if args.confirmed:
                    api.put('lead/' + lead['id'], data=payload)
                logging.info('line: %d updated: %s' % lead['id'])
                continue
        except APIError as e:
            logging.error('line: %d : %s' % (c.line_num, e))
            continue

    # first lead in the company
    if lead is None:
        try:
            resp = api.get('lead', data={
                'query': 'company: "%s" sort:created' % r['company'],
                '_fields': 'id,display_name,name,contacts,custom',
                'limit': 1
            })
            if resp['total_results']:
                lead = resp['data'][0]
                if args.confirmed:
                    api.put('lead/' + lead['id'], data=payload)
                logging.info('line: %d updated: %s' % (c.line_num, lead['id']))
                continue
        except APIError as e:
            logging.error('line: %d : %s' % (c.line_num, e))
            continue

    # new lead
    if lead is None and not args.disable_create:
        try:
            if args.confirmed:
                resp = api.post('lead', data=payload)
            logging.info('line %d new: %s' % (c.line_num, resp['id'] if args.confirmed else 'X'))
        except APIError as e:
            logging.error('line: %d : %s' % (c.line_num, e))


