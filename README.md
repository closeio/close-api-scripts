## Close.io API

[![PyPI version](https://badge.fury.io/py/closeio.svg)](https://badge.fury.io/py/closeio)

A convenient Python wrapper for the [Close.io](https://close.io/) API.

See the developer docs at http://developer.close.io. For any questions or issues, please contact support(at)close(dot)io.

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

Check out `scripts/` for more detailed examples.

### Running a script
```bash
$ git clone https://github.com/closeio/closeio-api-scripts.git
$ cd closeio-api
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ python setup.py install
$ python scripts/merge_leads.py -k MYAPIKEY 
...

```