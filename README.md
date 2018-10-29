# closeio-api-scripts

Example Python scripts for interacting with [Close.io](http://close.io/) through its [API](http://developer.close.io/)
using the [closeio_api Python client](https://github.com/closeio/closeio-api).

## Install basic dependencies

Before you start, you should already have `git`, `python` (2.7) and `virtualenv` installed. For OS X users, we recommend [Homebrew](https://brew.sh/).

## Setup

1. `git clone https://github.com/closeio/closeio-api-scripts.git`
1. `cd closeio-api-scripts`
1. `virtualenv venv`
1. `. venv/bin/activate`
1. `pip install -r requirements.txt`


## Running a script

Example:

```bash
python scripts/run_leads_deleted_report.py -k MYAPIKEY 
...

```

If you have any questions, please contact [support@close.io](mailto:support@close.io?Subject=API%20Scripts).

---

## Documentation for individual scripts

#### How to run the CSV importing script

The script will look for your CSV to have specific column names (case insensitive). All columns are optional. All columns not listed below will be imported as custom fields.

- `company` (multiple contacts will be grouped if rows have the same company
- `url` (must start with http:// or https://)
- `status` (defaults to "potential")
- `contact` (full name of contact)
- `title` (job title of contact)
- `email` (must be a valid email address)
- `phone` (must be a valid phone number, and must start with a "+" if it's a non-US number)
- `mobile_phone`
- `fax`
- `address` (street address)
- `city`
- `state` (2 letter abbreviation)
- `zip`
- `country` (2 letter abbreviation)
- (any additional fields will be added as custom fields)

Multiple contacts will be grouped in the same lead if multiple rows have the same value in the "company" column.

2. Make sure (if you haven't already in Setup) you're in the `closeio-api` directory and you have activated your virtual environment by running `. venv/bin/activate`.

3. Run the import script: `./scripts/csv_to_cio.py --api_key YOUR_API_KEY_HERE ~/path/to/your/leads.csv`

You can generate an API Key from Settings in Close.io.
