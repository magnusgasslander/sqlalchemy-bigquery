# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os

import httplib2
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials

"""
Copied from airflow integration
"""

class GoogleCloudBaseHook(object):
    """
    A base hook for Google cloud-related hooks. Google cloud has a shared REST
    API client that is built in the same way no matter which service you use.
    This class helps construct and authorize the credentials needed to then
    call apiclient.discovery.build() to actually discover and build a client
    for a Google cloud service.

    The class also contains some miscellaneous helper functions.

    All hook derived from this base hook use the 'Google Cloud Platform' connection
    type. Two ways of authentication are supported:

    Default credentials: Only specify 'Project Id'. Then you need to have executed
    ``gcloud auth`` on the Airflow worker machine.

    JSON key file: Specify 'Project Id', 'Key Path' and 'Scope'.

    Legacy P12 key files are not supported.
    """

    def __init__(self, conn_id, delegate_to=None):
        """
        :param conn_id: The connection ID to use when fetching connection info.
        :type conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: string
        """
        self.conn_id = conn_id
        self.delegate_to = delegate_to
        self.extras = self.get_connection(conn_id).extra_dejson

    def _authorize(self, scope = 'https://www.googleapis.com/auth/bigquery', key_path = None):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """

        kwargs = {}

        if not key_path:
            logging.info('Getting connection using `gcloud auth` user, since no key file '
                         'is defined for hook.')
            credentials = GoogleCredentials.get_application_default()
        else:
            if not scope:
                raise Exception('Scope should be defined when using a key file.')
            scopes = [s.strip() for s in scope.split(',')]
            print scopes
            if key_path.endswith('.json'):
                logging.info('Getting connection using a JSON key file.')
                credentials = ServiceAccountCredentials\
                    .from_json_keyfile_name(key_path, scopes)
                credentials = GoogleCredentials.get_application_default()
            elif key_path.endswith('.p12'):
                raise Exception('Legacy P12 key file are not supported, '
                                       'use a JSON key file.')
            else:
                raise Exception('Unrecognised extension for key file.')

        http = httplib2.Http()
        return credentials.authorize(http)

