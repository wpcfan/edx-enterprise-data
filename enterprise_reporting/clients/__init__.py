# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

import os
from time import time
from functools import wraps
from six.moves.urllib.parse import parse_qs, urlparse

from edx_rest_api_client.client import EdxRestApiClient


class EdxOAuth2APIClient(object):
    """
    Base API Client for accessing edX IDA API endpoints.
    """

    LMS_ROOT_URL = os.environ.get('LMS_ROOT_URL')
    LMS_OAUTH_HOST = os.environ.get('LMS_OAUTH_HOST')
    API_BASE_URL = LMS_ROOT_URL + '/api/'
    APPEND_SLASH = False

    def __init__(self, client_id, client_secret):
        """
        Connect to the REST API.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.expires_at = 0
        self.client = None

    def connect(self):
        """
        Connect to the REST API, authenticating with an access token retrieved with our client credentials.
        """

        access_token, expires_at = EdxRestApiClient.get_oauth_access_token(
            self.LMS_OAUTH_HOST + '/oauth2/access_token',
            self.client_id,
            self.client_secret,
            'jwt'
        )
        self.client = EdxRestApiClient(
            self.API_BASE_URL, append_slash=self.APPEND_SLASH, jwt=access_token,
        )
        self.expires_at = expires_at

    def token_expired(self):
        """
        Return True if the JWT token has expired, False if not.
        """
        return int(time()) > self.expires_at

    @staticmethod
    def refresh_token(func):
        """
        Use this method decorator to ensure the JWT token is refreshed when needed.
        """
        @wraps(func)
        def inner(self, *args, **kwargs):
            """
            Before calling the wrapped function, we check if the JWT token is expired, and if so, re-connect.
            """
            if self.token_expired():
                self.connect()
            return func(self, *args, **kwargs)
        return inner


def traverse_pagination(response, endpoint):
    """
    Traverse a paginated API response.

    Extracts and concatenates "results" (list of dict) returned by DRF-powered
    APIs.

    Arguments:
        response (Dict): Current response dict from service API
        endpoint (slumber Resource object): slumber Resource object from edx-rest-api-client

    Returns:
        list of dict.

    """
    results = response.get('results', [])

    next_page = response.get('next')
    while next_page:
        querystring = parse_qs(urlparse(next_page).query, keep_blank_values=True)
        response = endpoint.get(**querystring)
        results += response.get('results', [])
        next_page = response.get('next')

    return results
