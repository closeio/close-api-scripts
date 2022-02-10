import argparse

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
    "--opp-custom-fields",
    action="store_true",
    help="Copy opportunity custom fields",
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
    "--webhooks", action="store_true", help="Copy webhooks"
)

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


if args.lead_statuses or args.all:
    print("\nCopying Lead Statuses")
    lead_status_list = from_api.get(
        f"organization/{from_organization['id']}",
        params={"_fields": "lead_statuses"},
    )["lead_statuses"]

    for index, status in enumerate(lead_status_list):
        del status["id"]

        try:
            to_api.post("status/lead", data=status)
            print(f'Added `{status["label"]}`')
        except APIError as e:
            print(f"Couldn't add `{status['label']}` because {str(e)}")


if args.opportunity_statuses or args.all:
    print("\nCopying Opportunity Statuses")
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
                print(f'Added `{from_pipeline["name"]}` and its statuses')
            except APIError as e:
                print(
                    f"Couldn't add `{from_pipeline['name']}` because {str(e)}"
                )
                continue
        else:
            # Otherwise append the statuses to an existing pipeline
            for opp_status in from_pipeline["statuses"]:
                opp_status["pipeline_id"] = new_pipeline["id"]
                del opp_status["id"]

                try:
                    to_api.post("status/opportunity", data=opp_status)
                    print(f'Added `{opp_status["label"]}`')
                except APIError as e:
                    print(
                        f"Couldn't add `{opp_status['label']}` because {str(e)}"
                    )


def copy_custom_fields(custom_field_type):
    # Get the existing shared custom fields in case the new org already has them
    existing_shared_custom_fields = []
    has_more = True
    offset = 0
    while has_more:
        resp = to_api.get("custom_field/shared", params={"_skip": offset})
        existing_shared_custom_fields.extend(resp['data'])
        offset += len(resp["data"])
        has_more = resp["has_more"]

    from_custom_fields = from_api.get(
        f"custom_field_schema/{custom_field_type}"
    )["fields"]

    for from_cf in from_custom_fields:
        del from_cf["id"]
        del from_cf["organization_id"]

        try:
            if from_cf['is_shared']:
                to_cf = next(
                    iter(
                        [
                            x
                            for x in existing_shared_custom_fields
                            if x['name'] == from_cf['name']
                        ]
                    ),
                    None,
                )

                if not to_cf:
                    to_cf = to_api.post(f"custom_field/shared", data=from_cf)
                    print(f'Created `{from_cf["name"]}` shared custom field')

                # Only add association to a custom field type that's being copied.
                #
                # For example, if you have a shared field for leads and contacts, and you're copying only lead custom fields,
                # we would add only `lead` association to that shared field.
                to_api.post(
                    f"custom_field/shared/{to_cf['id']}/association",
                    data={'object_type': custom_field_type},
                )
                print(
                    f"Added `{custom_field_type}` association to shared `{from_cf['name']}` custom field"
                )
            else:
                to_api.post(f"custom_field/{custom_field_type}", data=from_cf)
                print(
                    f'Created `{from_cf["name"]}` {custom_field_type} custom field'
                )
        except APIError as e:
            print(f"Couldn't add `{from_cf['name']}` because {str(e)}")


if args.lead_custom_fields or args.all:
    print("\nCopying Lead Custom Fields")
    copy_custom_fields('lead')

if args.opp_custom_fields or args.all:
    print("\nCopying Opportunity Custom Fields")
    copy_custom_fields('opportunity')

if args.contact_custom_fields or args.all:
    print("\nCopying Contact Custom Fields")
    copy_custom_fields('contact')

if args.integration_links or args.all:
    print("\nCopying Integration Links")
    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("integration_link", params={"_skip": offset})
        for link in resp["data"]:
            del link["id"]
            del link["organization_id"]

            try:
                to_api.post("integration_link", data=link)
                print(f'Added `{link["name"]}`')
            except APIError as e:
                print(f"Couldn't add `{link['name']}` because {str(e)}")

        offset += len(resp["data"])
        has_more = resp["has_more"]

if args.smart_views or args.all:
    print("\nCopying Smart Views")
    has_more = True
    offset = 0
    saved_search_array = []
    while has_more:
        resp = from_api.get("saved_search", params={"_skip": offset})
        for saved_search in resp["data"]:
            del saved_search["id"]
            del saved_search["organization_id"]
            del saved_search["user_id"]
            saved_search_array.append(saved_search)
        offset += len(resp["data"])
        has_more = resp["has_more"]

    reverse = list(reversed(saved_search_array))
    for saved_search in reverse:
        error = ''
        try:
            to_api.post("saved_search", data=saved_search)
            print(f'Added `{saved_search["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{saved_search['name']}` because {str(e)}")

if args.roles or args.all:
    BUILT_IN_ROLES = [
        "Admin",
        "Restricted User",
        "Super User",
        "User",
    ]

    print("\nCopying Roles")
    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("role", params={"_skip": offset})
        for role in resp["data"]:
            if role["name"] in BUILT_IN_ROLES:
                continue

            del role["id"]
            del role["organization_id"]

            try:
                to_api.post("role", data=role)
                print(f'Added `{role["name"]}`')
            except APIError as e:
                print(f"Couldn't add `{role['name']}` because {str(e)}")

        offset += len(resp["data"])
        has_more = resp["has_more"]

if args.templates or args.all:
    print("\nCopying Templates")
    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("email_template", params={"_skip": offset})
        for template in resp["data"]:
            del template["id"]
            del template["organization_id"]

            try:
                to_api.post("email_template", data=template)
                print(f'Added `{template["name"]}`')
            except APIError as e:
                print(f"Couldn't add `{template['name']}` because {str(e)}")

        offset += len(resp["data"])
        has_more = resp["has_more"]

# Assumes all the sequence steps (templates) were already transferred over
if args.sequences or args.all:
    print("\nCopying Sequences")

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
                from_template = from_api.get(
                    f"email_template/{step['email_template_id']}",
                    params={'_fields': 'name'},
                )
                for template in to_templates:
                    if (
                        template["name"] == from_template["name"]
                        and template["is_shared"]
                    ):
                        step["email_template_id"] = template["id"]

            try:
                to_api.post("sequence", data=sequence)
                print(f'Added `{sequence["name"]}`')
            except APIError as e:
                print(f"Couldn't add `{sequence['name']}` because {str(e)}")

        offset += len(resp["data"])
        has_more = resp["has_more"]

if args.webhooks or args.all:
    print("\nCopying Webhooks")
    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("webhook", params={"_skip": offset})
        for webhook in resp["data"]:
            del webhook["id"]

            try:
                to_api.post("webhook", data=webhook)
                print(f'Added `{webhook["url"]}`')
            except APIError as e:
                print(f"Couldn't add `{webhook['url']}` because {str(e)}")

        offset += len(resp["data"])
        has_more = resp["has_more"]

if args.custom_activities or args.all:
    print("\nCopying Custom Activities")

    # Fetch both shared and non-shared activity custom fields
    source_custom_fields = []

    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("custom_field/activity", params={"_skip": offset})
        source_custom_fields.extend(resp['data'])
        offset += len(resp["data"])
        has_more = resp["has_more"]
    has_more = True
    offset = 0
    while has_more:
        resp = from_api.get("custom_field/shared", params={"_skip": offset})
        source_custom_fields.extend(resp['data'])
        offset += len(resp["data"])
        has_more = resp["has_more"]

    # Get the existing shared custom fields in case the new org already has them
    existing_shared_custom_fields = []
    has_more = True
    offset = 0
    while has_more:
        resp = to_api.get("custom_field/shared", params={"_skip": offset})
        existing_shared_custom_fields.extend(resp['data'])
        offset += len(resp["data"])
        has_more = resp["has_more"]

    custom_activities = from_api.get("custom_activity")["data"]
    for activity_type in custom_activities:
        # Create the activity type first, then add the fields to it below
        del activity_type["organization_id"]

        try:
            new_activity_type = to_api.post(
                "custom_activity", data=activity_type
            )
            print(f"Added `{activity_type['name']}` custom activity")
        except APIError as e:
            print(
                f"Couldn't add `{activity_type['name']}` custom activity because {str(e)}"
            )
            continue

        for field in activity_type["fields"]:
            # Get the object directly because some fields like `choices` aren't exposed in activity type `fields` array
            source_field = next(
                iter(
                    [x for x in source_custom_fields if x["id"] == field["id"]]
                ),
                None,
            )
            source_field.pop('organization_id', None)

            if field["is_shared"]:
                destination_field = next(
                    iter(
                        [
                            x
                            for x in existing_shared_custom_fields
                            if x['name'] == field['name']
                        ]
                    ),
                    None,
                )

                if destination_field:
                    new_cf = destination_field
                else:
                    # Create new shared field because it doesn't exist yet
                    try:
                        # Delete `associations` field as that references old (source) activities
                        del source_field['associations']

                        new_cf = to_api.post(
                            f"custom_field/shared/",
                            data=source_field,
                        )
                        existing_shared_custom_fields.append(new_cf)
                        print(f"Added `{field['name']}` shared field")
                    except APIError as e:
                        print(
                            f"Couldn't add `{field['name']}` shared field because {str(e)}"
                        )
                        continue

                to_api.post(
                    f"custom_field/shared/{new_cf['id']}/association",
                    data={
                        'object_type': 'custom_activity_type',
                        "custom_activity_type_id": new_activity_type["id"],
                        "required": field['required'],
                    },
                )
            else:
                # Non-shared (regular) field, just create it
                source_field["custom_activity_type_id"] = new_activity_type[
                    "id"
                ]
                to_api.post("custom_field/activity/", data=source_field)
