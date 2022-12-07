# Close API scripts

Example Python scripts for interacting with [Close](http://close.com/) through its [API](http://developer.close.com/)
using the [closeio_api Python client](https://github.com/closeio/closeio-api).

## Install basic dependencies

Before you start, you should already have `git`, `python 3` and `virtualenv` installed.
For OS X users, we recommend [Homebrew](https://brew.sh/).

## Setup

1. `git clone https://github.com/closeio/close-api-scripts.git`
2. `cd close-api-scripts`
3. `virtualenv venv`
4. `. venv/bin/activate`
5. `pip install -r requirements.txt`

## Running a script

Example:

```bash
python scripts/run_leads_deleted_report.py -k MYAPIKEY 
...
```

If you have any questions, please contact [support@close.com](mailto:support@close.com?Subject=Close%20API%20Scripts).