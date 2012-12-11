import requests
import json
import time
import urllib

class APIError(Exception):
    pass

class API(object):
    def __init__(self, base_url, api_key, async=False, config={}):
        assert base_url
        self.base_url = base_url
        self.api_key = api_key
        self.async = async
        self.config = config
        if async:
            import grequests
            self.requests = grequests
        else:
            self.requests = requests.session()

    def dispatch(self, method_name, endpoint, data=None):
        method = getattr(requests, method_name)

        print self.api_key
        print self.base_url

        response = method(
            self.base_url+endpoint,
            data=data != None and json.dumps(data),
            auth=(self.api_key, ''),
            headers={'Content-Type': 'application/json'},
            config=self.config
        )

        if self.async:
            return response
        else:
            if response.ok:
                return json.loads(response.content)
            else:
                print response, response.content
                raise APIError()

    def get(self, endpoint, data=None):
        if data:
            endpoint += '/?'+urllib.urlencode(data)
        else:
            endpoint += '/'
        return self.dispatch('get', endpoint)

    def post(self, endpoint, data):
        return self.dispatch('post', endpoint+'/', data)

    def put(self, endpoint, data):
        return self.dispatch('put', endpoint+'/', data)

    # Only for async requests
    def map(self, reqs, max_retries=5):
        # TODO
        # There is no good way of catching or dealing with exceptions that are raised
        # during the request sending process when using map or imap.
        # When this issue is closed: https://github.com/kennethreitz/grequests/pull/15
        # modify this method to repeat only the requests that failed because of
        # connection errors
        if self.async:
            import grequests
            responses = [(
                json.loads(response.content) if response.ok and response.content is not None
                else APIError()
            ) for response in grequests.map(reqs)]
            # retry the api calls that failed until they succeed or the max_retries limit is reached
            retries = 0
            while True and retries < max_retries:
                n_errors = sum([int(isinstance(response, APIError)) for response in responses])
                if not n_errors:
                    break
                # sleep 2 seconds before retrying requests
                time.sleep(2)
                error_ids = [i for i, resp in enumerate(responses) if isinstance(responses[i], APIError)]
                new_reqs = [reqs[i] for i in range(len(responses)) if i in error_ids]
                new_resps = [(
                    json.loads(response.content) if response.ok and response.content is not None
                    else APIError()
                ) for response in grequests.map(new_reqs)]
                # update the responses that previously finished with errors
                for i in range(len(error_ids)):
                    responses[error_ids[i]] = new_resps[i]
                retries += 1
            return responses


class CloseIO_API(API):
    def __init__(self, api_key, async=False, config={}, development=False):
        if development:
            base_url = 'http://local.close.io:5001/api/v1/'
        else:
            base_url = 'https://app.close.io/api/v1/'
        super(CloseIO_API, self).__init__(base_url, api_key, async, config)


