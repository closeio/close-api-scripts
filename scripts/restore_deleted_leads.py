import argparse
import logging
from closeio_api import Client as CloseIO_API, APIError
from gevent.pool import Pool
import gevent.monkey
gevent.monkey.patch_all()


parser = argparse.ArgumentParser(description='FOR INTERNAL USE ONLY. Restore an array of deleted leads by ID. This CANNOT restore status changes or call recordings.')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
args = parser.parse_args()
api = CloseIO_API(args.api_key)

## Array of Lead IDs. Add the IDs you want to restore here.
lead_ids = []

## Create a list of active users for the sake of posting opps.
org_id = api.get('me')['organizations'][0]['id']
memberships = api.get('organization/' + org_id, params={ '_fields': 'memberships' })['memberships']
active_users = [i['user_id'] for i in memberships]

## Array to keep track of number of leads restored. Because we use gevent, we can't have a standard counter variable.
total_leads_restored = []

## This is a list of object types you want to restore on the lead. We can also add activity.email, but in this script
## it's assumed that email sync will take care of all of the emails that were deleted, assuming the same email accounts
## are connected to Close.
object_types = ['contact', 'opportunity', 'task.lead', 'activity.call', 'activity.note', 'activity.sms']

## This is a dictionary that stores a mapping between old contact ids and new contact ids for restoration purposes.
contact_id_mapping = {}

def restoreObjects(object_type, old_lead_id, new_lead_id):
	has_more = True
	cursor = ''
	while has_more:
		resp_objects = api.get('event', params={ 'object_type': object_type, 'action': 'deleted', '_cursor': cursor, 'lead_id': old_lead_id })
		for event in resp_objects['data']:
			old_contact_id = None
			if 'previous_data' in event:
				prev = event['previous_data']
				if 'id' in prev:
					del prev['id']

				## Map old contact ID to new contact ID
				if 'contact_id' in prev:
					if prev['contact_id'] in contact_id_mapping:
						prev['contact_id'] = contact_id_mapping[prev['contact_id']]
					else:
						del prev['contact_id']

				## Delete quality_info when posting a call
				if 'quality_info' in prev:
					del prev['quality_info']

				## Set call source to External
				if object_type == 'activity.call':
					prev['source'] = 'External'

				## If the user assigned to the opp is no longer in the organization, we still want to post the opp, we just
				## can't have it assigned to that user_id.
				if object_type == 'opportunity' and 'user_id' in prev and prev['user_id'] not in active_users:
					del prev['user_id']

				## If anything was in outbox or scheduled, switch it to draft so it doesn't send accidentally at the wrong time.
				if object_type in ['activity.sms', 'activity.call'] and prev['status'] in ['outbox', 'scheduled']:
					prev['status'] == 'draft'

				## Set endpoint for posting. We need to change the activity and task object types to match the post endpoint
				## for their respective types.
				endpoint = object_type
				if 'activity' in endpoint:
					endpoint = endpoint.replace('.', '/')
				elif '.lead' in endpoint:
					endpoint = endpoint.replace('.lead', '')

				prev['lead_id'] = new_lead_id

				## Post the object to the new lead.
				try:
					post_request = api.post(endpoint, data=prev)

					## If we posted a contact, add the new contact id to the dictionary.
					if object_type == 'contact':
						contact_id_mapping[event['object_id']] = post_request['id']
				except APIError as e:
					print(f"ERROR: Could not post {object_type} {event['object_id']} because {str(e)}")
		cursor = resp_objects['cursor_next']
		has_more = bool(resp_objects['cursor_next'])

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
			print(f"Cannot delete completed task activity {completed_id} because {str(e)}")

def restoreLead(old_lead_id):
	resp_lead = api.get('event', params={ 'object_type': 'lead', 'action': 'deleted', 'lead_id': old_lead_id })
	if len(resp_lead['data']) > 0 and resp_lead['data'][0].get('previous_data'):
		prev = resp_lead['data'][0]['previous_data']
		if 'id' in prev:
			del prev['id']
		## Post New Lead.
		try:
			post_lead = api.post('lead', data=prev)
			if 'id' in post_lead:
				new_lead_id = post_lead['id']
				## Restore all objects on the lead.
				for object_type in object_types:
					restoreObjects(object_type, old_lead_id, new_lead_id)

				## We want to remove task completed activities on the new lead because they will be posted at the top of the activity timeline
				## regardless of when they were actually completed.
				removeTaskCompletedActivities(new_lead_id)

				total_leads_restored.append(1)
				print(f"{len(total_leads_restored)}: Restored {old_lead_id}")
		except APIError as e:
			print(f"{old_lead_id}: Lead could not be posted because {str(e)}")
	else:
		print(f"{old_lead_id} could not be restored because there is no data to restore")

print(f"Total leads being restored: {len(lead_ids)}")
pool = Pool(5)
pool.map(restoreLead, lead_ids)
print(f"Total leads restored {len(total_leads_restored)}")
print(f"Total leads not restored {(len(lead_ids) - len(total_leads_restored))}")
