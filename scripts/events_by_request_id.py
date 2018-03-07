#!/usr/bin/env python

import logging
import sys
import json
import argparse
from closeio_api import Client as CloseIO_API

parser = argparse.ArgumentParser(description='Get Events By Request ID')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--request-id', '-r',  required=True, help='request_id from event log.')
parser.add_argument('--output', '-o', required=True, help='json output file of events')
parser.add_argument('--verbose', '-v', action='store_true', help='Increase logging verbosity.')
args = parser.parse_args()

api = CloseIO_API(args.api_key)


def setup_logger():
    logger = logging.getLogger('closeio.api.events_by_request_id')
    logger.setLevel(logging.INFO)
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = setup_logger()

output = open(args.output,"w") 
output.write('{"events": [')

has_more = True
cursor = None
first_iter = True
while has_more:
    resp = api.get('event', params={'_cursor': cursor, 'request_id':args.request_id})
    cursor = resp['cursor_next']
    has_more = bool(cursor)

    for event in resp['data']:
        if not first_iter:
            output.write(",")
        json.dump(event, output, indent=4)
        first_iter = False

output.write("]}")
output.close()
