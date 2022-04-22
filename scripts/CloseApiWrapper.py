from closeio_api import Client


class CloseApiWrapper(Client):
    """
    Close API wrapper that makes it easier to paginate through resources and get all items
    with a single function call alongside some convenience functions (e.g. getting all lead statuses).
    """

    def __init__(
        self, api_key=None, tz_offset=None, max_retries=5, development=False
    ):
        super().__init__(
            api_key=api_key,
            tz_offset=tz_offset,
            max_retries=max_retries,
            development=development,
        )

    def get_lead_statuses(self):
        organization_id = self.get('me')['organizations'][0]['id']
        return self.get(
            f"organization/{organization_id}",
            params={"_fields": "lead_statuses"},
        )["lead_statuses"]

    def get_opportunity_pipelines(self):
        organization_id = self.get('me')['organizations'][0]['id']
        return self.get(
            f"organization/{organization_id}",
            params={"_fields": "pipelines"},
        )["pipelines"]

    def get_custom_fields(self, type):
        return self.get(f"custom_field_schema/{type}")["fields"]

    def get_opportunity_statuses(self):
        organization_id = self.get('me')['organizations'][0]['id']
        pipelines = self.get(
            f"organization/{organization_id}",
            params={"_fields": "pipelines"},
        )["pipelines"]

        opportunity_statuses = []
        for pipeline in pipelines:
            opportunity_statuses.extend(pipeline['statuses'])

        return opportunity_statuses

    def get_all_items(self, url, params=None):
        if params is None:
            params = {}

        items = []
        has_more = True
        offset = 0
        while has_more:
            params["_skip"] = offset
            resp = self.get(url, params=params)
            items.extend(resp['data'])
            offset += len(resp["data"])
            has_more = resp["has_more"]

        return items
