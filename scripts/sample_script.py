import argparse

from closeio_api import Client as CloseIO_API

parser = argparse.ArgumentParser(description='Sample script used to test out whether the environment is set up correctly. Script will print out the organization name associated with the provided API key.')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
args = parser.parse_args()

api = CloseIO_API(args.api_key)
org_id = api.get('api_key/' + args.api_key)['organization_id']
org_name = api.get('organization/' + org_id, params={'_fields': 'name'})['name'].replace('/', '')

print(f"Organization associated with this API key is '{org_name}'.")
