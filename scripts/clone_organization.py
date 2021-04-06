import argparse
import csv

from closeio_api import APIError, Client as CloseIO_API

arg_parser = argparse.ArgumentParser(
    description="Clone one organization to another"
)
arg_parser.add_argument(
    "--from-api-key",
    "-f",
    required=True,
    help="API Key for source organization",
)
arg_parser.add_argument(
    "--to-api-key",
    "-t",
    required=True,
    help="API Key for destination organization",
)

arg_parser.add_argument(
    "--lead-statuses",
    action="store_true",
    help="Copy lead statuses",
)
arg_parser.add_argument(
    "--opportunity-statuses",
    action="store_true",
    help="Copy opportunity statuses",
)

arg_parser.add_argument(
    "--lead-custom-fields",
    action="store_true",
    help="Copy lead custom fields",
)
arg_parser.add_argument(
    "--contact-custom-fields",
    action="store_true",
    help="Copy contact custom fields",
)

arg_parser.add_argument(
    "--custom-activities",
    action="store_true",
    help="Copy custom activities",
)

arg_parser.add_argument(
    "--smart-views", action="store_true", help="Copy smart views"
)
arg_parser.add_argument(
    "--templates", action="store_true", help="Copy templates"
)
arg_parser.add_argument(
    "--sequences", action="store_true", help="Copy sequences"
)
arg_parser.add_argument(
    "--integration-links",
    action="store_true",
    help="Copy integration links",
)
arg_parser.add_argument("--roles", action="store_true", help="Copy roles")

arg_parser.add_argument(
    "--all", "-a", action="store_true", help="Copy all settings"
)
args = arg_parser.parse_args()

from_api = CloseIO_API(args.from_api_key)
to_api = CloseIO_API(args.to_api_key)

from_organization = from_api.get("me")["organizations"][0]
to_organization = to_api.get("me")["organizations"][0]

print(
    f"Copying items from `{from_organization['name']}` to `{to_organization['name']}`..."
)


def write_results_to_csv(csv_data, objects, keys):
    f = open(
        f'{to_organization["name"]} {objects}.csv', 'wt', encoding='utf-8'
    )
    try:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(csv_data)
    finally:
        f.close()


if args.lead_statuses or args.all:
    print("Copying Lead Statuses")
    csv_data = []

    lead_status_list = from_api.get(
        f"organization/{from_organization['id']}",
        params={"_fields": "lead_statuses"},
    )["lead_statuses"]

    for index, status in enumerate(lead_status_list):
        del status["id"]

        error = ''
        try:
            to_api.post("status/lead", data=status)
        except APIError as e:
            error = str(e)

        csv_data.append({'name': status['label'], 'error': error})

    write_results_to_csv(csv_data, 'Lead Statuses', ['name', 'error'])

if args.opportunity_statuses or args.all:
    print("Copying Opportunity Statuses")
    csv_data = []

    to_pipelines = to_api.get("pipeline")["data"]

    from_pipelines = from_api.get(
        f"organization/{from_organization['id']}",
        params={"_fields": "pipelines"},
    )["pipelines"]

    for from_pipeline in from_pipelines:
        # Try to find an existing pipeline by name
        new_pipeline = next(
            iter(
                [x for x in to_pipelines if x["name"] == from_pipeline["name"]]
            ),
            None,
        )

        if not new_pipeline:
            # If the pipeline doesn't exist, create the pipeline alongside the statuses
            del from_pipeline["id"]
            del from_pipeline["organization_id"]

            try:
                new_pipeline = to_api.post("pipeline", data=from_pipeline)
            except APIError as e:
                error = str(e)
                csv_data.append(
                    {
                        'pipeline_name': from_pipeline['name'],
                        'status_name': '',
                        'error': error,
                    }
                )
                continue
        else:
            # Otherwise append the statuses to an existing pipeline
            for opp_status in from_pipeline["statuses"]:
                opp_status["pipeline_id"] = new_pipeline["id"]
                del opp_status["id"]

                error = ''
                try:
                    to_api.post("status/opportunity", data=opp_status)
                except APIError as e:
                    error = str(e)

                csv_data.append(
                    {
                        'pipeline_name': from_pipeline['name'],
                        'status_name': opp_status['label'],
                        'error': error,
                    }
                )

    write_results_to_csv(
        csv_data,
        'Opportunity Statuses',
        ['pipeline_name', 'status_name', 'error'],
    )


def copy_custom_fields(custom_field_type):
    csv_data = []

    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get(
            f"custom_fields/{custom_field_type}", params={"_skip": offset}
        )
        for custom in resp["data"]:
            del custom["id"]
            del custom["organization_id"]

            error = ''
            try:
                to_api.post(f"custom_fields/{custom_field_type}", data=custom)
            except APIError as e:
                error = str(e)

            csv_data.append({'name': custom['name'], 'error': error})

        offset += len(resp["data"])
        has_more = resp["has_more"]

    write_results_to_csv(
        csv_data,
        f'{custom_field_type.title()} Custom Fields',
        ['name', 'error'],
    )


if args.lead_custom_fields or args.all:
    print("Copying Lead Custom Fields")
    copy_custom_fields('lead')

if args.contact_custom_fields or args.all:
    print("Copying Contact Custom Fields")
    copy_custom_fields('contact')

if args.integration_links or args.all:
    print("Copying Integration Links")
    csv_data = []

    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("integration_link", params={"_skip": offset})
        for link in resp["data"]:
            del link["id"]
            del link["organization_id"]

            error = ''
            try:
                to_api.post("integration_link", data=link)
            except APIError as e:
                error = str(e)

            csv_data.append({'name': link['name'], 'error': error})

        offset += len(resp["data"])
        has_more = resp["has_more"]

    write_results_to_csv(
        csv_data,
        'Integration Links',
        ['name', 'error'],
    )

if args.smart_views or args.all:
    print("Copying Smart Views")
    csv_data = []

    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("saved_search", params={"_skip": offset})
        for saved_search in resp["data"]:
            del saved_search["id"]
            del saved_search["organization_id"]

            error = ''
            try:
                to_api.post("saved_search", data=saved_search)
            except APIError as e:
                error = str(e)

            csv_data.append(
                {
                    'name': saved_search['name'],
                    'query': saved_search['query'],
                    'error': error,
                }
            )

        offset += len(resp["data"])
        has_more = resp["has_more"]

    write_results_to_csv(
        csv_data,
        'Smart Views',
        ['name', 'error'],
    )

if args.roles or args.all:
    BUILT_IN_ROLES = [
        "Admin",
        "Restricted User",
        "Super User",
        "User",
    ]

    print("Copying Roles")
    csv_data = []

    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("role", params={"_skip": offset})
        for role in resp["data"]:
            if role["name"] in BUILT_IN_ROLES:
                continue

            del role["id"]
            del role["organization_id"]

            error = ''
            try:
                to_api.post("role", data=role)
            except APIError as e:
                error = str(e)

            csv_data.append({'name': role['name'], 'error': error})

        offset += len(resp["data"])
        has_more = resp["has_more"]

    write_results_to_csv(
        csv_data,
        'Roles',
        ['name', 'error'],
    )

if args.templates or args.all:
    print("Copying Templates")
    csv_data = []

    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("email_template", params={"_skip": offset})
        for source_step_template in resp["data"]:
            del source_step_template["id"]
            del source_step_template["organization_id"]

            error = ''
            try:
                to_api.post("email_template", data=source_step_template)
            except APIError as e:
                error = str(e)

            csv_data.append(
                {'name': source_step_template['name'], 'error': error}
            )

        offset += len(resp["data"])
        has_more = resp["has_more"]

    write_results_to_csv(
        csv_data,
        'Email Templates',
        ['name', 'error'],
    )

# Assumes all the sequence steps (templates) were already transferred over
if args.sequences or args.all:
    print("Copying Sequences")
    csv_data = []

    to_templates = []
    has_more = True
    offset = 0
    while has_more:
        resp = to_api.get("email_template", params={"_skip": offset})
        to_templates.extend(resp['data'])
        offset += len(resp["data"])
        has_more = resp["has_more"]

    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("sequence", params={"_skip": offset})
        for sequence in resp["data"]:
            del sequence["id"]
            del sequence["organization_id"]
            for step in sequence["steps"]:
                del step["id"]
                source_step_template = from_api.get(
                    f"email_template/{step['email_template_id']}",
                    params={'_fields': 'name'},
                )
                for template in to_templates:
                    if (
                        source_step_template["name"] == template["name"]
                        and template["is_shared"]
                    ):
                        step["email_template_id"] = template["id"]

            error = ''
            try:
                to_api.post("sequence", data=sequence)
            except APIError as e:
                error = str(e)

            csv_data.append({'name': sequence['name'], 'error': error})

        offset += len(resp["data"])
        has_more = resp["has_more"]

    write_results_to_csv(
        csv_data,
        'Email Sequences',
        ['name', 'error'],
    )
