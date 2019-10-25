import argparse
import csv
import time
from datetime import datetime, timedelta

from closeio_api import Client as CloseIO_API
from dateutil import tz

parser = argparse.ArgumentParser(description='Get Time To Respond Metrics From Org')

parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--past-days', '-p', required=True, help='How many days in the past should we start the calculation?')
parser.add_argument('--org-count', '-o', action='store_true', help='Use this field if you also want org totals, not just active user totals. Note: Only use this field with short date ranges (i.e. 2 weeks maximum)')
parser.add_argument('--user-counts', '-u', action='store_true', help='Get stats per individual user')

args = parser.parse_args()

api = CloseIO_API(args.api_key)

org_id = api.get('api_key/' + args.api_key)['organization_id']
org_name = api.get('organization/' + org_id)['name']
org_memberships = api.get('organization/' + org_id)['memberships']

assert args.org_count or args.user_counts, \
    'ERROR: Please include the org count parameter, the user counts parameter, or both'

assert (args.org_count and int(args.past_days) < 15) or not args.org_count, \
    'ERROR: When using the org-count parameter, make sure that the past days parameter is less than 15'


def pretty_time_delta(seconds):
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%dd %dh %dm %ds' % (days, hours, minutes, seconds)
    elif hours > 0:
        return '%dh %dm %ds' % (hours, minutes, seconds)
    elif minutes > 0:
        return '%dm %ds' % (minutes, seconds)
    else:
        return '%ds' % (seconds)


tz_off = (-time.timezone / 60 / 60)

today = datetime.utcnow().date()
start = datetime(today.year, today.month, today.day, tzinfo=tz.tzutc()) - timedelta(days=int(args.past_days)) - timedelta(hours=tz_off)
end = datetime(today.year, today.month, today.day, tzinfo=tz.tzutc()) + timedelta(days=1)

start = start.strftime("%Y-%m-%dT%H:%M:%S")
end = end.strftime("%Y-%m-%dT%H:%M:%S")

user_stats = []


def getTTR(user):
    if user != None:
        print(f"Getting all activities in the last {args.past_days} days for {user['user_full_name']}...")
    else:
        print(f"Getting all activities in the last {args.past_days} days for {'All Users'}...")

    has_more = True
    offset = 0
    seconds = 0
    seconds_inc = 0
    resp = None
    activities = []

    while has_more:
        if user != None:
            resp = api.get('activity', params={'_skip': offset, 'date_created__gte': start, 'date_created__lte': end, '_fields': '_type,id,date_created,lead_id,direction,user_id,duration', 'user_id': user['user_id']})
        else:
            resp = api.get('activity', params={'_skip': offset, 'date_created__gte': start, 'date_created__lte': end, '_fields': '_type,id,date_created,lead_id,direction,user_id,duration'})
        for activity in resp['data']:
            if activity['_type'] in ['Call', 'Email', 'SMS'] and activity['lead_id'] != None:
                activity['date_created'] = activity['date_created'].split('+')[0].split('.')[0]
                activities.append(activity)
        print(offset)
        offset += len(resp['data'])
        has_more = resp['has_more']
    if user == None:
        user = {}
        user['user_full_name'] = 'All Users'
    print(f"Getting TTR for {user['user_full_name']}...")

    responded_count = 0
    responded_count_with_not_responded_to_yet = 0
    total_time_to_respond_with_not_responded_to_yet = 0
    total_time_to_respond = 0

    inbound_activities = [i for i in activities if ((i['direction'] == 'incoming' or i['direction'] == 'inbound') and (i['_type'] in ['SMS', 'Email'] or (i['_type'] == 'Call' and i['duration'] == 0)))]

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    for i in range(0, len(inbound_activities)):
        activities_for_this_lead = [a for a in activities if a['lead_id'] == inbound_activities[i]['lead_id']]
        outbound_activities_for_this_lead = [a for a in activities_for_this_lead if
                                             datetime.strptime(a['date_created'].split('.')[0], "%Y-%m-%dT%H:%M:%S") > datetime.strptime(inbound_activities[i]['date_created'].split('.')[0], "%Y-%m-%dT%H:%M:%S") and (
                                                     a['direction'] == 'outbound' or a['direction'] == 'outgoing')]
        if len(outbound_activities_for_this_lead) != 0:
            activity_after = outbound_activities_for_this_lead[len(outbound_activities_for_this_lead) - 1]
            diff = (datetime.strptime(activity_after['date_created'].split('.')[0], "%Y-%m-%dT%H:%M:%S") - datetime.strptime(inbound_activities[i]['date_created'].split('.')[0], "%Y-%m-%dT%H:%M:%S")).total_seconds()
            total_time_to_respond += diff
            total_time_to_respond_with_not_responded_to_yet += diff
            responded_count += 1
            responded_count_with_not_responded_to_yet += 1

        else:
            diff = (datetime.strptime(now, "%Y-%m-%dT%H:%M:%S") - datetime.strptime(inbound_activities[i]['date_created'].split('.')[0], "%Y-%m-%dT%H:%M:%S")).total_seconds()
            total_time_to_respond_with_not_responded_to_yet += diff
            responded_count_with_not_responded_to_yet += 1

    if responded_count != 0:
        seconds = int(float(total_time_to_respond) / float(responded_count))

    if total_time_to_respond_with_not_responded_to_yet != 0:
        seconds_inc = int(float(total_time_to_respond_with_not_responded_to_yet) / float(responded_count_with_not_responded_to_yet))

    print(f"Average Time to Respond To Leads (Only Leads Alredy Responded To): {pretty_time_delta(seconds)}")
    print(f"Average Time to Respond To Leads (Including Leads Not Responded To Yet): {pretty_time_delta(seconds_inc)}")

    user_stat = {
        'Total # of SMS': len([i for i in activities if i['_type'] == 'SMS']),
        'Total # of Emails': len([i for i in activities if i['_type'] == 'Email']),
        'Total # of Calls': len([i for i in activities if i['_type'] == 'Call']),
        'Total # of Inbound Communications': len([i for i in activities if (i['_type'] in ['SMS', 'Call', 'Email'] and i['direction'] in ['inbound', 'incoming'])]),
        'Total # of Outbound Communications': len([i for i in activities if (i['_type'] in ['SMS', 'Call', 'Email'] and i['direction'] in ['outbound', 'outgoing'])]),
        'Average Time to Respond To Leads (Only Leads Alredy Responded To)': seconds,
        'Average Time to Respond To Leads (Only Leads Alredy Responded To) Formatted': pretty_time_delta(seconds),
        'Average Time to Respond To Leads (Including Leads Not Responded To Yet)': seconds_inc,
        'Average Time to Respond To Leads (Including Leads Not Responded To Yet) Formatted': pretty_time_delta(seconds_inc),
        'User Name': user['user_full_name']
    }

    user_stats.append(user_stat)


if args.user_counts:
    for membership in org_memberships:
        getTTR(membership)

if args.org_count:
    getTTR(None)

f = open(f'{org_name} Time to Respond Data Per User For The Past {args.past_days} days.csv', 'wt', encoding='utf-8')
try:
    keys = user_stats[0].keys()
    ordered_keys = ['User Name', 'Average Time to Respond To Leads (Only Leads Alredy Responded To) Formatted', 'Average Time to Respond To Leads (Including Leads Not Responded To Yet) Formatted'] + [i for i in keys if i not in [
        'Average Time to Respond To Leads (Including Leads Not Responded To Yet) Formatted', 'User Name', 'Average Time to Respond To Leads (Only Leads Alredy Responded To) Formatted']]
    writer = csv.DictWriter(f, ordered_keys)
    writer.writeheader()
    writer.writerows(user_stats)
finally:
    f.close()
