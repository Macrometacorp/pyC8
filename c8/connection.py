from __future__ import absolute_import, unicode_literals

import json

import requests

import c8.constants as constants
from c8.exceptions import (
    C8AuthenticationError,
    C8TenantNotFoundError,
    C8TokenNotFoundError,
)
from c8.http import DefaultHTTPClient

__all__ = ["Connection"]


class Connection(object):
    """HTTP connection to specific C8 tenant.

    :param url: C8Db base URL.
    :type url: str | unicode
    :param fabric: Fabric name.
    :type fabric: str | unicode
    :param username: Username.
    :type username: str | unicode
    :param password: Password.
    :type password: str | unicode
    :param http_client: User-defined HTTP client.
    :type http_client: c8.http.HTTPClient
    :param is_fabric: Whether this a DB or streams call.
                      Anything other than streams is a DB call.
    :type is_fabric: bool
    """

    def __init__(self, url, email, password, token, apikey, http_client, skip_tenant=False):
        self.url = url
        self._tenant_name = ""
        self._fabric_name = constants.FABRIC_DEFAULT
        self._email = email
        self._password = password
        self._http_client = http_client or DefaultHTTPClient()
        self._token = token
        self._apikey = apikey
        self._header = ""

        if self._token is not None:
            self._auth_token = self._token

        if self._apikey is not None:
            self._auth_token = self._apikey

        if self._email != "" and password != "":
            body = self._get_auth_token()
            self._auth_token = body.get("jwt")
            self._tenant_name = body.get("tenant")
            self._header = {"Authorization": "Bearer " + self._auth_token}

        if self._tenant_name == "" and self._token is not None:
            headers = {"Authorization": "Bearer " + self._auth_token}
            self._header = headers

            if not skip_tenant:
                tenurl = self.url + "/_api/user"
                response = requests.get(url=tenurl, headers=headers)
                if response.status_code == 200:
                    body = json.loads(response.text)
                    self._tenant_name = body["result"][0]["tenant"]

        if self._tenant_name == "" and self._apikey is not None:
            headers = {"Authorization": "apikey " + self._auth_token}
            self._header = headers
            if not skip_tenant:
                tenurl = self.url + "/_api/user"
                response = requests.get(url=tenurl, headers=headers)
                if response.status_code == 200:
                    body = json.loads(response.text)
                    self._tenant_name = body["result"][0]["tenant"]

        self._url_prefix = "{}/_fabric/{}/_api".format(url, self._fabric_name)

        # TODO : Handle the functions side of things

    def _get_auth_token(self, email=None, password=None, tenant=None, username=None):
        data = {}
        if password is not None:
            data.update({"password": password})
            if email is not None:
                data.update({"email": email})
            elif tenant is not None and username is not None:
                data.update({"tenant": tenant, "username": username})
            else:
                raise C8AuthenticationError(
                    "Failed to Authenticate the C8DB user, Error: Invalid JSON"
                )
        else:
            data = {"email": self._email, "password": self._password}

        data = json.dumps(data)
        url = self.url + "/_open/auth"
        response = requests.post(url, data=data)

        if response.status_code == 200:
            body = json.loads(response.text)
            tenant = body.get("tenant")
            token = body.get("jwt")
            if not tenant:
                raise C8TenantNotFoundError(
                    "Failed to get Tenant Name for URL: {} and Email: {}".format(
                        self.url, self._email
                    )
                )
            if not token:
                raise C8TokenNotFoundError(
                    "Failed to get Authentication Token for URL: {} Tenant: {} and Email: {}".format(
                        self.url, self.tenant_name, self._email
                    )
                )
        else:
            raise C8AuthenticationError(
                "Failed to Authenticate the C8DB user for URL: {} and Email: {}. Error: {}".format(
                    self.url, self._email, response.text
                )
            )
        return body

    @property
    def headers(self):
        return self._header

    @property
    def url_prefix(self):
        """Return the C8 URL prefix (base URL + tenant name).

        :returns: C8 URL prefix.
        :rtype: str | unicode
        """
        return self._url_prefix

    @property
    def username(self):
        """Return the username.

        :returns: Username.
        :rtype: str | unicode
        """
        return self._username

    @property
    def tenant_name(self):
        """Return the tenant name.

        :returns: tenant name.
        :rtype: str | unicode
        """
        return self._tenant_name

    @property
    def fabric_name(self):
        """Return the DB name if it was called from the DB class

        :returns:  DB name.
        :rtype: str | unicode
        """
        return self._fabric_name

    def set_fabric_name(self, new_fabric_name):
        self._fabric_name = new_fabric_name

    def set_url_prefix(self, new_prefix):
        """
        Set the URL prefix to the new prefix,
        returns (old_prefix,new_prefix) so it can be tracked.
        """
        self._url_prefix = new_prefix
        # return old_prefix, self._url_prefix

    def send_request(self, request, custom_prefix=None):
        """Send an HTTP request to C8 server.

        :param request: HTTP request.
        :type request: c8.request.Request
        :param custom_prefix: Custom url-path value
        :type custom_prefix: str
        :return: HTTP response.
        :rtype: c8.response.Response
        """
        # Below line is a debug to show what the full request URL is.
        # Useful in testing multitenancy API calls
        # if '_tenant' in request.endpoint and '_fabric' in request.endpoint:
        if "/_fabric" in request.endpoint:
            find_url = self._url_prefix.find("/_fabric")
            url = self._url_prefix[0:find_url]
            final_url = url + request.endpoint
        else:
            if custom_prefix is not None:
                final_url = self.url + custom_prefix + request.endpoint
            else:
                final_url = self._url_prefix + request.endpoint

        headers = request.headers

        if self._token is not None:
            headers["Authorization"] = "bearer " + self._auth_token

        elif self._apikey is not None:
            headers["Authorization"] = "apikey " + self._auth_token

        elif self._token is None and self._apikey is None:
            headers["Authorization"] = "bearer " + self._auth_token

        self._header = headers
        return self._http_client.send_request(
            method=request.method,
            url=final_url,
            params=request.params,
            data=request.data,
            headers=headers,
        )


class TenantConnection(Connection):
    """Tenant Connection wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, url, email, password, token, apikey, http_client, skip_tenant=False):
        super(TenantConnection, self).__init__(
            url=url,
            email=email,
            password=password,
            token=token,
            apikey=apikey,
            http_client=http_client,
            skip_tenant=skip_tenant,
        )
        self._fqfabric_name = self._tenant_name + "." + self._fabric_name

    def __repr__(self):
        return "<TenantConnection {}>".format(self._fqfabric_name)

    @property
    def fqfabric_name(self):
        """Return the fully qualified (i.e with tenant) fabric name.

        :returns: Tenant name.
        :rtype: str | unicode
        """
        return self._fqfabric_name


# class FabricConnection(Connection):
#     """Fabric Connection wrapper.

#     :param connection: HTTP connection.
#     :type connection: c8.connection.Connection
#     """

#     def __init__(self, url, stream_port, tenant, fabric, email, password,
#                  http_client):
#         super(FabricConnection, self).__init__(url, tenant, fabric, email,
#                                                password, http_client, True)
#         self._fabric_name = fabric
#         self.stream_port = stream_port

#     def __repr__(self):
#         return '<FabricConnection {}>'.format(self._fabric_name)

#     @property
#     def fabric_name(self):
#         """Return the fabric name.

#         :returns: Fabric name.
#         :rtype: str | unicode
#         """
#         return self._fabric_name
