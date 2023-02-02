import argparse

from closeio_api import APIError

from scripts.CloseApiWrapper import CloseApiWrapper

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
    "--statuses",
    action="store_true",
    help="Copy lead & opportunity statuses",
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
    "--custom-fields",
    action="store_true",
    help="Copy lead, contact, and opportunity custom fields",
)
arg_parser.add_argument(
    "--lead-custom-fields",
    action="store_true",
    help="Copy lead custom fields",
)
arg_parser.add_argument(
    "--opportunity-custom-fields",
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
    "--templates", action="store_true", help="Copy email & SMS templates"
)
arg_parser.add_argument(
    "--email-templates", action="store_true", help="Copy email templates"
)
arg_parser.add_argument(
    "--sms-templates", action="store_true", help="Copy SMS templates"
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
    "--groups", action="store_true", help="Copy groups without members."
)
arg_parser.add_argument(
    "--groups-with-members", action="store_true", help="Copy groups including members. Any member that hasn't been "
                                                       "added to the destination organization will be skipped."
)
arg_parser.add_argument(
    "--all", "-a", action="store_true", help="Copy all settings"
)
args = arg_parser.parse_args()

from_api = CloseApiWrapper(args.from_api_key)
to_api = CloseApiWrapper(args.to_api_key)

from_organization = from_api.get("me")["organizations"][0]
to_organization = to_api.get("me")["organizations"][0]

message = f"Cloning `{from_organization['name']}` ({from_organization['id']}) organization to `{to_organization['name']}` ({to_organization['id']})..."
message += '\nData from source organization will be added to the destination organization. No data will be deleted.\n\nContinue?'

confirmed = input(f"{message} (y/n)\n")
if confirmed not in ["yes", "y"]:
    exit()

if args.lead_statuses or args.statuses or args.all:
    print("\nCopying Lead Statuses")

    from_lead_statuses = from_api.get_lead_statuses()
    for status in from_lead_statuses:
        del status["id"]

        try:
            to_api.post("status/lead", data=status)
            print(f'Added lead status `{status["label"]}`')
        except APIError as e:
            print(f"Couldn't add `{status['label']}` because {str(e)}")

if args.opportunity_statuses or args.statuses or args.all:
    print("\nCopying Opportunity Statuses")
    to_pipelines = to_api.get_opportunity_pipelines()
    from_pipelines = from_api.get_opportunity_pipelines()

    for from_pipeline in from_pipelines:
        # Try to find an existing pipeline by name
        to_pipeline = next(
            (x for x in to_pipelines if x["name"] == from_pipeline["name"]),
            None,
        )

        if not to_pipeline:
            # If the pipeline doesn't exist, create the pipeline alongside the statuses
            del from_pipeline["id"]
            del from_pipeline["organization_id"]

            try:
                to_pipeline = to_api.post("pipeline", data=from_pipeline)
                print(f'Added `{from_pipeline["name"]}` and its statuses')
            except APIError as e:
                print(
                    f"Couldn't add `{from_pipeline['name']}` because {str(e)}"
                )
                continue
        else:
            # Otherwise append the statuses to an existing pipeline
            for opp_status in from_pipeline["statuses"]:
                opp_status["pipeline_id"] = to_pipeline["id"]
                del opp_status["id"]

                try:
                    to_api.post("status/opportunity", data=opp_status)
                    print(f'Added opportunity status `{opp_status["label"]}`')
                except APIError as e:
                    print(
                        f"Couldn't add `{opp_status['label']}` because {str(e)}"
                    )


def copy_custom_fields(custom_field_type):
    # Get the existing shared custom fields in case the new org already has them
    to_shared_custom_fields = to_api.get_all_items('custom_field/shared')

    from_custom_fields = from_api.get(
        f"custom_field_schema/{custom_field_type}"
    )["fields"]

    for from_cf in from_custom_fields:
        del from_cf["id"]
        del from_cf["organization_id"]

        try:
            if from_cf['is_shared']:
                to_cf = next(
                    (
                        x
                        for x in to_shared_custom_fields
                        if x['name'] == from_cf['name']
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


if args.lead_custom_fields or args.custom_fields or args.all:
    print("\nCopying Lead Custom Fields")
    copy_custom_fields('lead')

if args.opportunity_custom_fields or args.custom_fields or args.all:
    print("\nCopying Opportunity Custom Fields")
    copy_custom_fields('opportunity')

if args.contact_custom_fields or args.custom_fields or args.all:
    print("\nCopying Contact Custom Fields")
    copy_custom_fields('contact')

if args.integration_links or args.all:
    print("\nCopying Integration Links")
    integration_links = from_api.get_all_items('integration_link')
    for link in integration_links:
        del link["id"]
        del link["organization_id"]

        try:
            to_api.post("integration_link", data=link)
            print(f'Added `{link["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{link['name']}` because {str(e)}")


def get_id_mappings():
    map_from_to_id = {}

    # Custom Activity Types
    from_custom_activities = from_api.get("custom_activity")["data"]
    to_custom_activities = to_api.get("custom_activity")["data"]
    for from_ca in from_custom_activities:
        to_ca = next(
            (x for x in to_custom_activities if x['name'] == from_ca['name']),
            None,
        )
        if to_ca:
            map_from_to_id[from_ca['id']] = to_ca['id']

    # Custom fields
    def get_custom_fields(api):
        BUILT_IN_SCHEMES = [
            'lead',
            'contact',
            'opportunity',
        ]
        custom_activity_type_ids = [
            x['id'] for x in api.get("custom_activity")["data"]
        ]

        custom_fields = []
        for schema in BUILT_IN_SCHEMES + custom_activity_type_ids:
            if schema.startswith('actitype_'):
                schema_fields = api.get_custom_fields(f"activity/{schema}")
            else:
                schema_fields = api.get_custom_fields(schema)

            # Add `object_type` field so we can use it to match/map IDs later on in case there are 2 custom fields
            # with the same name - one Lead Custom Field, and another Custom Activity Custom Field
            schema_fields = [
                {**x, **{'object_type': schema}} for x in schema_fields
            ]
            custom_fields.extend(schema_fields)

        return custom_fields

    from_custom_fields = get_custom_fields(from_api)
    to_custom_fields = get_custom_fields(to_api)
    for from_cf in from_custom_fields:
        to_cf = next(
            (
                x
                for x in to_custom_fields
                if x['name'] == from_cf['name']
                   and (
                           x['object_type'] == from_cf['object_type']
                           or x['object_type']
                           == map_from_to_id.get(from_cf['object_type'])
                   )
            ),
            None,
        )
        if to_cf:
            map_from_to_id[from_cf['id']] = to_cf['id']

    # Lead & opportunity statuses
    from_statuses = (
            from_api.get_lead_statuses() + from_api.get_opportunity_statuses()
    )
    to_statuses = (
            to_api.get_lead_statuses() + to_api.get_opportunity_statuses()
    )
    for from_status in from_statuses:
        to_status = next(
            (x for x in to_statuses if x['label'] == from_status['label']),
            None,
        )
        if to_status:
            map_from_to_id[from_status['id']] = to_status['id']

    # Email templates
    from_templates = from_api.get_all_items('email_template')
    to_templates = to_api.get_all_items('email_template')
    for from_template in from_templates:
        to_template = next(
            (x for x in to_templates if x['name'] == from_template['name']),
            None,
        )
        if to_template:
            map_from_to_id[from_template['id']] = to_template['id']

    # SMS templates
    from_templates = from_api.get_all_items('sms_template')
    to_templates = to_api.get_all_items('sms_template')
    for from_template in from_templates:
        to_template = next(
            (x for x in to_templates if x['name'] == from_template['name']),
            None,
        )
        if to_template:
            map_from_to_id[from_template['id']] = to_template['id']

    # Sequences
    from_sequences = from_api.get_all_items('sequence')
    to_sequences = to_api.get_all_items('sequence')
    for from_sequence in from_sequences:
        to_sequence = next(
            (x for x in to_sequences if x['name'] == from_sequence['name']),
            None,
        )
        if to_sequence:
            map_from_to_id[from_sequence['id']] = to_sequence['id']

    return map_from_to_id


if args.roles or args.all:
    BUILT_IN_ROLES = [
        "Admin",
        "Restricted User",
        "Super User",
        "User",
    ]

    print("\nCopying Roles")
    roles = from_api.get_all_items('role')
    for role in roles:
        if role["name"] in BUILT_IN_ROLES:
            continue

        del role["id"]
        del role["organization_id"]

        try:
            to_api.post("role", data=role)
            print(f'Added `{role["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{role['name']}` because {str(e)}")

if args.templates or args.email_templates or args.all:
    print("\nCopying Email Templates")
    templates = from_api.get_all_items('email_template')
    for template in templates:
        del template["id"]
        del template["organization_id"]

        try:
            to_api.post("email_template", data=template)
            print(f'Added `{template["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{template['name']}` because {str(e)}")

if args.templates or args.sms_templates or args.all:
    print("\nCopying SMS Templates")
    templates = from_api.get_all_items('sms_template')
    for template in templates:
        del template["id"]
        del template["organization_id"]

        try:
            to_api.post("sms_template", data=template)
            print(f'Added `{template["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{template['name']}` because {str(e)}")

# Assumes all the sequence steps (templates) were already transferred over
if args.sequences or args.all:
    print("\nCopying Sequences")

    to_email_templates = to_api.get_all_items('email_template')
    to_sms_templates = to_api.get_all_items('sms_template')
    from_sequences = from_api.get_all_items('sequence')
    for sequence in from_sequences:
        del sequence["id"]
        del sequence["organization_id"]
        for step in sequence["steps"]:
            del step["id"]

            # Replace Email Template ID (if it exists ie. it's an Email step)
            if step.get('email_template_id'):
                from_template = from_api.get(
                    f"email_template/{step['email_template_id']}",
                    params={'_fields': 'name'},
                )
                for template in to_email_templates:
                    if (
                            template["name"] == from_template["name"]
                            and template["is_shared"]
                    ):
                        step["email_template_id"] = template["id"]

            # Replace SMS Template ID (if it exists ie. it's a SMS step)
            if step.get('sms_template_id'):
                from_template = from_api.get(
                    f"sms_template/{step['sms_template_id']}",
                    params={'_fields': 'name'},
                )
                for template in to_sms_templates:
                    if (
                            template["name"] == from_template["name"]
                            and template["is_shared"]
                    ):
                        step["sms_template_id"] = template["id"]

        try:
            to_api.post("sequence", data=sequence)
            print(f'Added `{sequence["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{sequence['name']}` because {str(e)}")

if args.custom_activities or args.all:
    print("\nCopying Custom Activities")

    # Fetch both shared and non-shared activity custom fields
    from_custom_fields = from_api.get_all_items(
        'custom_field/activity'
    ) + from_api.get_all_items('custom_field/shared')

    # Get the existing shared custom fields in case the new org already has them
    to_shared_custom_fields = to_api.get_all_items('custom_field/shared')

    custom_activity_types = from_api.get("custom_activity")["data"]
    for activity_type in custom_activity_types:
        # Re-map old role IDs to new role IDs (by name)
        if activity_type['editable_with_roles']:
            new_roles = to_api.get('role')['data']
            new_editable_with_roles = []
            for old_role_id in activity_type['editable_with_roles']:
                if old_role_id.startswith('role_'):
                    old_role_name = from_api.get(f'role/{old_role_id}')['name']
                    new_role = next(
                        (x for x in new_roles if x['name'] == old_role_name),
                        None,
                    )
                    if new_role:
                        new_editable_with_roles.append(new_role['id'])
                else:
                    # Built-in roles such as `admin`
                    new_editable_with_roles.append(old_role_id)

            activity_type['editable_with_roles'] = new_editable_with_roles

        try:
            del activity_type["organization_id"]
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
            from_field = next(
                (x for x in from_custom_fields if x["id"] == field["id"]),
                None,
            )
            from_field.pop('organization_id', None)

            if field["is_shared"]:
                to_field = next(
                    (
                        x
                        for x in to_shared_custom_fields
                        if x['name'] == field['name']
                    ),
                    None,
                )

                if not to_field:
                    # Create new shared field because it doesn't exist yet
                    try:
                        # Delete `associations` field as that references old (source) activities
                        del from_field['associations']

                        to_field = to_api.post(
                            f"custom_field/shared/",
                            data=from_field,
                        )
                        to_shared_custom_fields.append(to_field)
                        print(f"Added `{field['name']}` shared field")
                    except APIError as e:
                        print(
                            f"Couldn't add `{field['name']}` shared field because {str(e)}"
                        )
                        continue

                to_api.post(
                    f"custom_field/shared/{to_field['id']}/association",
                    data={
                        'object_type': 'custom_activity_type',
                        "custom_activity_type_id": new_activity_type["id"],
                        "required": field['required'],
                        'editable_with_roles': field['editable_with_roles'],
                    },
                )
            else:
                # Non-shared (regular) field, just create it
                from_field["custom_activity_type_id"] = new_activity_type["id"]
                to_api.post("custom_field/activity/", data=from_field)

if args.smart_views or args.all:

    def structured_replace(value, replacement_dictionary):
        '''
        Recursively replace values in a dictionary with values from a replacement dictionary.
        This is used to replace IDs in source Smart Views with the new IDs in the destination account.

        IDs can be lead status IDs, opportunity status IDs, email templates, sequences, custom fields, etc. - pretty
        much anything apart from Smart View IDs which are handled separately as they are not known until the Smart View
        is created.
        '''
        if type(value) == list:
            return [structured_replace(item, replacement_dictionary) for item in value]

        if type(value) == dict:
            return {
                key: structured_replace(value, replacement_dictionary)
                for key, value in value.items()
            }

        return replacement_dictionary.get(value, value)


    def textual_replace(value, replacement_dictionary):
        '''
        Simple global & replace of IDs in source Smart Views with the new IDs in the destination account.
        Used only for deprecated (textual) queries.
        '''
        for from_id, to_id in replacement_dictionary.items():
            value = value.replace(from_id, to_id)

        return value


    print("\nCopying Smart Views")
    from_smart_views = from_api.get_all_items('saved_search')

    # Used to map old to new IDs (custom fields, custom activity types, lead & opportunity statuses, email templates...)
    # that will be used in global search & replace within each Smart View query
    map_from_to_id = None

    # Used to map old to new Smart View IDs for Smart Views that use `in:SMART_VIEW_ID` in their queries
    map_from_to_smart_view_id = {}
    created_smart_views = []

    # Sort Smart Views as they appear in the original organization
    # (when you add a new Smart View, it will show up at the top of the list)
    reverse = list(reversed(from_smart_views))

    # Create Smart Views in the destination organization
    for smart_view in reverse:
        # Replace IDs
        s_query = smart_view.get('s_query')
        query = smart_view.get('query')

        if s_query:
            if not map_from_to_id:
                map_from_to_id = get_id_mappings()

            smart_view['s_query'] = structured_replace(s_query, map_from_to_id)
        elif query:
            smart_view['query'] = textual_replace(query, map_from_to_id)

        try:
            old_id = smart_view.pop('id')
            del smart_view["organization_id"]
            del smart_view["user_id"]

            new_smart_view = to_api.post("saved_search", data=smart_view)
            map_from_to_smart_view_id[old_id] = new_smart_view['id']

            created_smart_views.append(new_smart_view)
            print(f'Added `{smart_view["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{smart_view['name']}` because {str(e)}")

    # Replace any Smart View IDs in case one Smart View is nested within the other
    for smart_view in created_smart_views:
        # Replace Smart View IDs
        s_query = smart_view.get('s_query')
        query = smart_view.get('query')

        if s_query:
            smart_view['s_query'] = structured_replace(s_query, map_from_to_smart_view_id)
        elif query:
            smart_view['query'] = textual_replace(query, map_from_to_smart_view_id)

        # Update the Smart View if necessary
        if smart_view['s_query'] != s_query or smart_view['query'] != query:
            to_api.put(f"saved_search/{smart_view['id']}", data=smart_view)

if args.groups or args.groups_with_members or args.all:
    print("\nCopying Groups")
    groups = from_api.get('group')['data']
    for group in groups:
        group = from_api.get(f'group/{group["id"]}', params={'_fields': 'name,members'})

        try:
            new_group = to_api.post('group', data={'name': group['name']})

            if args.groups_with_members:
                for member in group['members']:
                    try:
                        to_api.post(f'group/{new_group["id"]}/member', data={'user_id': member['user_id']})
                    except APIError as e:
                        if 'Invalid organization members' in str(e):
                            pass

            print(f'Added `{group["name"]}`')
        except APIError as e:
            print(f"Couldn't add `{group['name']}` because {str(e)}")

if args.webhooks:
    print("\nCopying Webhooks")
    webhooks = from_api.get_all_items('webhook')
    for webhook in webhooks:
        del webhook["id"]

        try:
            to_api.post("webhook", data=webhook)
            print(f'Added `{webhook["url"]}`')
        except APIError as e:
            print(f"Couldn't add `{webhook['url']}` because {str(e)}")
