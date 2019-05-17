import sys
import argparse
from closeio_api import Client as CloseIO_API, APIError
from operator import itemgetter
import csv
import math
import gevent
import gevent.monkey
from gevent.pool import Pool
reload(sys)
sys.setdefaultencoding('utf-8')
gevent.monkey.patch_all()
pool = Pool(7)

parser = argparse.ArgumentParser(description='Find duplicate contacts on a lead in your Close org via contact_name, email address, or phone number')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--field', '-f', default='all', choices=['contact_name', 'email', 'phone', 'all'], required=False, help="Specify a field to compare uniqueness")
args = parser.parse_args()

## Initialize Close API Wrapper
api = CloseIO_API(args.api_key)
org_id = api.get('api_key/' + args.api_key)['organization_id']
org_name = api.get('organization/' + org_id, params={ '_fields': 'name' })['name'].replace ('/', '')

## Calculate number of slices necessary to get all leads
total_leads = api.get('lead', params={ '_limit': 0, 'query': 'sort:created contacts > 1' })['total_results']
total_slices = int(math.ceil(float(total_leads)/1000))
slices = range(1, total_slices + 1)
leads = []

## Write data to a CSV
def writeCSV(type_name, items, ordered_keys):
	print "Writing %s data to CSV..." % type_name
	f = open('%s %s Duplicates on a Single Lead.csv' % (org_name, type_name), 'wt')
	try:
		writer = csv.DictWriter(f, ordered_keys)
		writer.writeheader()
		writer.writerows(items)
	finally:
		f.close() 

## Get leads for each slice
def getLeadsSlice(slice_num):
	print "Getting lead slice %s of %s..." % (slice_num, total_slices)
	has_more = True
	offset = 0 
	while has_more:
		resp = api.get('lead', params={ '_skip': offset, 'query': 'sort:created slice:%s/%s contacts > 1' % (slice_num, total_slices), '_fields':'id,display_name,contacts,date_created' })
		for lead in resp['data']:
			leads.append(lead)
		offset += len(resp['data'])
		has_more = resp['has_more']

## Add to a list of duplicates for contact names
def getDuplicatesForContactName(contact_name):
	for dupe in contact_names[contact_name]:
		contact_name_duplicates.append({ 'Contact Name': dupe['display_name'], 'Lead Name': dupe['lead_name'], 'Contact ID': dupe['id'], 'Lead ID': dupe['lead_id'], 'Close URL': 'https://app.close.com/lead/%s/' % dupe['lead_id'] })

## Add to a list of duplicates for contact emails
def getDuplicatesForEmail(email):
	for dupe in emails[email]:
		email_duplicates.append({ 'Email Address': email, 'Contact Name': dupe['display_name'], 'Lead Name': dupe['lead_name'], 'Contact ID': dupe['id'], 'Lead ID': dupe['lead_id'], 'Close URL': 'https://app.close.com/lead/%s/' % dupe['lead_id'] })

## Add to a list of duplicates for contact phones
def getDuplicatesForPhone(phone):
	for dupe in phones[phone]:
		phone_duplicates.append({ 'Phone Number': phone, 'Contact Name': dupe['display_name'], 'Lead Name': dupe['lead_name'], 'Contact ID': dupe['id'], 'Lead ID': dupe['lead_id'], 'Close URL': 'https://app.close.com/lead/%s/' % dupe['lead_id'] })

print "Getting Leads..."
pool.map(getLeadsSlice, slices)
leads = sorted(leads, key=itemgetter('date_created'))

## Process duplicates
contact_name_duplicates = []
email_duplicates = []
phone_duplicates = []
print "Processing contacts on each lead..."

for lead in leads:
	contact_names = {}
	emails = {}
	phones = {}
	keys_with_dupes_contact_name = []
	keys_with_dupes_email = []
	keys_with_dupes_phone = []
	for contact in lead['contacts']:
		contact['lead_name'] = lead['display_name']
		## Pouplate a dictionary of duplicate contact names, and keep track of those that appear more than once
		if args.field in ['all', 'contact_name']:
			lower_name = contact['display_name'].strip().lower()
			if contact_names.get(lower_name) and contact not in contact_names[lower_name]:
				contact_names[lower_name].append(contact)
				keys_with_dupes_contact_name.append(lower_name)
			elif not contact_names.get(lower_name):
				contact_names[lower_name] = [contact]
		
		## Populate a dictionary of emails, and keep track of those that appear more than once
		if args.field in ['all', 'email']:
			for email in contact['emails']:
				if emails.get(email['email']) and contact not in emails[email['email']]:
					emails[email['email']].append(contact)
					keys_with_dupes_email.append(email['email'])
				elif not emails.get(email['email']):
					emails[email['email']] = [contact]
		
		## Populate a dictionary of phones, and keep track of those that appear more than once
		if args.field in ['all', 'phone']:
			for phone in contact['phones']:
				if phones.get(phone['phone']) and contact not in phones[phone['phone']]:
					phones[phone['phone']].append(contact)
					keys_with_dupes_phone.append(phone['phone'])
				elif not phones.get(phone['phone']):
					phones[phone['phone']] = [contact]

	## Write data to appropriate arrays				
	if args.field in ['all', 'contact_name']:
		if len(keys_with_dupes_contact_name) > 0:
			keys_with_dupes_contact_name = list(set(keys_with_dupes_contact_name))
			pool.map(getDuplicatesForContactName, keys_with_dupes_contact_name)		

	if args.field in ['all', 'email']:
		if len(keys_with_dupes_email) > 0:
			keys_with_dupes_email = list(set(keys_with_dupes_email))
			pool.map(getDuplicatesForEmail, keys_with_dupes_email)

	if args.field in ['all', 'phone']:
		if len(keys_with_dupes_phone) > 0:
			keys_with_dupes_phone = list(set(keys_with_dupes_phone)) 
			pool.map(getDuplicatesForPhone, keys_with_dupes_phone)
	
	print "%s of %s: %s" % (leads.index(lead) + 1, len(leads), lead['id'])

if args.field in ['all', 'contact_name']:
	## Sort the duplicates alphabetically by lead name and then contact name and write them to a CSV
	contact_name_duplicates = sorted(contact_name_duplicates, key=itemgetter('Lead ID', 'Contact Name'))
	writeCSV("Contact Name", contact_name_duplicates, ['Contact Name', 'Lead Name', 'Contact ID', 'Lead ID', 'Close URL'])

if args.field in ['all', 'email']:
	email_duplicates = sorted(email_duplicates, key=itemgetter('Lead ID', 'Email Address'))
	writeCSV("Email", email_duplicates, ['Email Address', 'Contact Name', 'Lead Name', 'Contact ID', 'Lead ID', 'Close URL'])

if args.field in ['all', 'phone']:
	phone_duplicates = sorted(phone_duplicates, key=itemgetter('Lead ID', 'Phone Number'))
	writeCSV("Phone", phone_duplicates, ['Phone Number', 'Contact Name', 'Lead Name', 'Contact ID', 'Lead ID', 'Close URL'])