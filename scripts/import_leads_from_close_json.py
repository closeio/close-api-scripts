import argparse
import sys
import json
from closeio_api import Client as CloseIO_API, APIError
from gevent.pool import Pool
import gevent.monkey
import copy
gevent.monkey.patch_all()

parser = argparse.ArgumentParser(description='Import Close Leads from a Close JSON file into a New Org')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--jsonfile', '-j', required=True, help='JSON File Path')
args = parser.parse_args()
api = CloseIO_API(args.api_key)



## Create a list of active users for the sake of posting opps and activities.
me = api.get('me')
org = api.get('organization/' + me['organizations'][0]['id'], params={ '_fields': 'memberships,inactive_memberships,name' })
org_name = org['name']
active_users = [i['user_id'] for i in org['memberships']]
all_users = active_users + [i['user_id'] for i in org['inactive_memberships']]
apikey_user_id = me['id']

## Create a list of lead and opportunity statuses currently in the org
lead_statuses = api.get('status/lead')['data']
lead_status_labels = [i['label'] for i in lead_statuses]
opportunity_statuses = api.get('status/opportunity')['data']
opportunity_status_labels = [i['label'] for i in opportunity_statuses]

## Array to keep track of number of leads restored. Because we use gevent, we can't have a standard counter variable.
total_leads_imported = []

## Array to keep track of leads that could not be posted.
errored_leads = []

## Read in data file taken from args
with open(args.jsonfile) as data_file:
    data = json.load(data_file)

## Make sure all statuses in the JSON file exist in Close before continuing
def postStatus(lead_or_opp, label, status_type):
	status_data = { 'label': label }
	if lead_or_opp == 'opportunity':
		status_data['type'] = status_type
	try:
		api.post('status/' + lead_or_opp, data=status_data)
	except APIError as e:
		print "Cannot add status %s to org because %s" % (label, str(e))

## Make sure all lead and opp statuses are in Close
lead_statuses_labels_in_json = [i['status_label'] for i in data if i['status_label'] not in lead_status_labels]
lead_statuses_labels_in_json = list(set(lead_statuses_labels_in_json))
for label in lead_statuses_labels_in_json:
	postStatus('lead', label, None)
	lead_status_labels.append(label)

for d in data:
	for opp in d['opportunities']:
		if opp['status_label'] not in opportunity_status_labels:
			postStatus('opportunity', opp['status_label'], opp['status_type'])
			opportunity_status_labels.append(opp['status_label'])


## This is a dictionary that stores a mapping between old contact ids and new contact ids for restoration purposes.
contact_id_mapping = {}

## Import opps to the new lead
def importOpportunities(opp_data, new_lead_id):
	for opp in opp_data:
		del opp['id']
		if 'organization_id' in opp:
			del opp['organization_id']
		if opp['user_id'] not in active_users:
			opp['user_id'] = apikey_user_id
		if 'contact_id' in opp and opp['contact_id'] != None and opp['contact_id'] in contact_id_mapping:
			opp['contact_id'] = contact_id_mapping[opp['contact_id']]
		opp['status'] = opp['status_label']
		del opp['status_id']
		del opp['status_label']
		opp['lead_id'] = new_lead_id
		try:
			api.post('opportunity', data=opp)
		except APIError as e:
			print "Could not post opp to %s because %s" % (new_lead_id, str(e))

## Import tasks to the new lead
def importTasks(task_data, new_lead_id):
	for task in task_data:
		del task['id']
		if 'organization_id' in task:
			del task['organization_id']
		if task['assigned_to'] not in active_users:
			task['assigned_to'] = apikey_user_id
		task['lead_id'] = new_lead_id
		try:
			api.post('task', data=task)
		except APIError as e:
			print "Could not post task to %s because %s" % (new_lead_id, str(e))

## Import call, note, and SMS data to new lead. Assume that emails will be brought over via email sync.
def importActivities(activity_data, new_lead_id):
	types = { 'Call': 'activity/call', 'SMS': 'activity/sms', 'Note': 'activity/note' }
	for activity in activity_data:
		if 'organization_id' in activity:
			del activity['organization_id']
		activity['lead_id'] = new_lead_id
		if 'contact_id' in activity and activity['contact_id'] != None and activity['contact_id'] in contact_id_mapping:
			activity['contact_id'] = contact_id_mapping[activity['contact_id']]
		if activity['_type'] == 'Call':
			if 'quality_info' in activity:
				del activity['quality_info']
			activity['source'] = 'External'
		if activity['_type'] == 'SMS' and activity['status'] in ['outbox', 'scheduled']:
			activity['status'] = 'draft'
		try:
			api.post(types[activity['_type']], data=activity)
		except APIError as e:
			print "Could not post %s activity to %s because %s" % (activity['_type'], new_lead_id, str(e))

## Remove task completed activities from top of lead.
def removeTaskCompletedActivities(new_lead_id):
	has_more = True
	offset = 0
	task_completed_ids = []
	while has_more:
		resp_task_completed = api.get('activity/task_completed', params={ '_skip': offset, 'lead_id': new_lead_id, '_fields': 'id' })
		task_completed_ids = [i['id'] for i in resp_task_completed['data']]
		offset += len(resp_task_completed['data'])
		has_more = resp_task_completed['has_more']

	for completed_id in task_completed_ids:
		try:
			api.delete('activity/task_completed/' + completed_id)
		except APIError as e:
			print "Cannot delete completed task activity %s because %s" % (completed_id, str(e))

def restoreLead(lead):
	lead_data = {}
	lead_data['status'] = lead['status_label']
	lead_data['name'] = lead['display_name']
	lead_data['date_created'] = lead['date_created']
	lead_data['created_by'] = lead['created_by']
	lead_data['url'] = lead['url']

	## Clear users ids that have never been in the new Close org from user type custom fields:
	custom_data = copy.deepcopy(lead['custom'])
	for custom in lead['custom']:
		if lead['custom'].get(custom) and str(lead['custom'][custom]).startswith('user_') and lead['custom'][custom] not in all_users:
			del custom_data[custom]
	lead_data['custom'] = custom_data
	lead_data['custom']['Original Lead ID'] = lead['id']


	## Remove lead references from old contacts before posting to new leads
	contacts = copy.deepcopy(lead['contacts'])
	for contact in contacts:
		del contact['id']
		del contact['lead_id']
	lead_data['contacts'] = contacts

	## Post New Lead.
	try:
		post_lead = api.post('lead', data=lead_data)
		if 'id' in post_lead:
			new_lead_id = post_lead['id']
			## Create contact mapping dictionary
			for i in range(0, len(lead['contacts'])):
				contact_id_mapping[lead['contacts'][i]['id']] = post_lead['contacts'][i]['id']
			## Import Opportunities
			if 'opportunities' in lead and len(lead['opportunities']) > 0:
				importOpportunities(lead['opportunities'], new_lead_id)
			if 'tasks' in lead and len(lead['tasks']) > 0:
				importTasks(lead['tasks'], new_lead_id)
				## We want to remove task completed activities on the new lead because they will be posted at the top of the activity timeline
				## regardless of when they were actually completed.
				removeTaskCompletedActivities(new_lead_id)
			## Import Call, SMS, and Note data. We assume email data will be transfered over automatically
			if 'activities' in lead and len(lead['activities']) > 0:
				activity_array = [i for i in lead['activities'] if i['_type'] in ['Call', 'Note', 'SMS']]
				importActivities(activity_array, new_lead_id)
			total_leads_imported.append(new_lead_id)
			print "%s: Imported %s" % (len(total_leads_imported), lead['id'])
	
	except Exception as e:
		print "%s: Lead could not be posted because %s" % (lead['id'], str(e))
		errored_leads.append(lead)

print "Total leads being restored: %s" % len(data)
pool = Pool(5)
pool.map(restoreLead, data)
print "Total leads restored %s" % len(total_leads_imported)
print "Total leads not restored %s" %  (len(data) - len(total_leads_imported))

## Write errored lead_ids to JSON File
if len(errored_leads) > 0:
	with open('%s Errored Leads from JSON Import.json' % (org_name), 'w') as outfile:
		json.dump(errored_leads, outfile, indent=4)