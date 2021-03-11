import argparse

from closeio_api import APIError, Client as CloseIO_API

parser = argparse.ArgumentParser(
    description='Change sequence sender for specific user'
)

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument(
    '--from-email',
    '-f',
    required=True,
    help='Current email address being used to send sequence',
)
parser.add_argument(
    '--to-email',
    '-t',
    required=True,
    help='Email address you want to use to send sequence',
)
parser.add_argument(
    '--sender-account-id',
    '-s',
    required=True,
    help='Email account id you want to use to send sequence',
)
parser.add_argument(
    '--sender-name',
    '-n',
    required=True,
    help='Sender name you want to use to send sequence',
)

args = parser.parse_args()
api = CloseIO_API(args.api_key)

has_more = True
offset = 0
from_subs = []
count = 0

print("Getting sequence subscriptions")

while has_more:
    sub_results = api.get(
        'sequence_subscription',
        params={
            '_skip': offset,
            'fields': 'id,sender_email,sender_name,sender_account_id,status',
        },
    )
    from_subs += [
        i
        for i in sub_results['data']
        if i['sender_email'] == args.from_email
        and i['status'] in ['active', 'paused']
    ]
    offset += len(sub_results['data'])
    print(offset)
    has_more = sub_results['has_more']

print(f"Total subscriptions: {len(from_subs)}")
print("Updating subscriptions")

for sub in from_subs:
    try:
        api.put(
            'sequence_subscription/' + sub['id'],
            data={
                'sender_name': args.sender_name,
                'sender_account_id': args.sender_account_id,
                'sender_email': args.to_email,
            },
        )
        count += 1
        print(f"{count}: {sub['id']}")
    except APIError as e:
        print(f"Can't update sequence {sub['id']} because {str(e)}")
