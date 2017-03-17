# closeio-api-scripts
Example useful scripts using the [Close.io API](http://developer.close.io/)

Install basic dependencies
-----
Before you start, you should already have `git`, `python-2.7` and `virtualenv` installed. For OS X users, we recommend [MacPorts](http://www.macports.org/).

Next you have to xinstall the API Client.

### Installation (of API client)

`pip install closeio`

### Sample Usage (of API client)

```python
from closeio_api import Client
import urllib

api = Client('YOUR_API_KEY')

# post a lead
lead = api.post('lead', data={'name': 'New Lead'})

# get 5 most recently updated opportunities
opportunities = api.get('opportunity', params={'_order_by': '-date_updated', '_limit': 5})

# fetch multiple leads (using search syntax)
lead_results = api.get('lead', params={
    '_limit': 10,
    '_fields': 'id,display_name,status_label',
    'query': 'custom.my_custom_field:"some_value" status:"Potential" sort:updated'
})
```

### Running a script
```bash
$ git clone https://github.com/closeio-api-scripts.git
$ cd closeio-api-scripts
$ virtualenv venv
$ source venv/bin/activate
$ pip -U -r requirements.txt
$ python scripts/merge_leads.py -k MYAPIKEY 
...

```

Check out `scripts/` for more detailed examples.

If you have any questions, please contact support@close.io 