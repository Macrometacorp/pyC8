from __future__ import absolute_import, unicode_literals

import socket
import time

from abc import ABCMeta, abstractmethod
from urllib3.connection import HTTPConnection

import requests

from c8.response import Response

__all__ = ['HTTPClient', 'DefaultHTTPClient']


class HTTPClient(object):  # pragma: no cover
    """Abstract base class for HTTP clients."""

    __metaclass__ = ABCMeta

    @abstractmethod
    def send_request(self,
                     method,
                     url,
                     headers=None,
                     params=None,
                     data=None,
                     auth=None):
        """Send an HTTP request.

        This method must be overridden by the user.

        :param method: HTTP method in lowercase (e.g. "post").
        :type method: str | unicode
        :param url: Request URL.
        :type url: str | unicode
        :param headers: Request headers.
        :type headers: dict
        :param params: URL (query) parameters.
        :type params: dict
        :param data: Request payload.
        :type data: str | unicode | bool | int | list | dict
        :param auth: Username and password.
        :type auth: tuple
        :returns: HTTP response.
        :rtype: c8.response.Response
        """
        raise NotImplementedError

# KARTIK : 20181211 : C8Platform#166 : Remote disconnect issue.
# https://github.com/joowani/python-arango/issues/30#issuecomment-333771027
# Also see: https://github.com/requests/requests/issues/3808


class KeepaliveAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        kwargs['socket_options'] = HTTPConnection.default_socket_options + [
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ]
        super(KeepaliveAdapter, self).init_poolmanager(*args, **kwargs)


class DefaultHTTPClient(HTTPClient):
    """Default HTTP client implementation."""

    def __init__(self):
        self._session = requests.Session()
        # KARTIK : 20181211 : C8Platform#166 : Implement keepalive adapter
        adapter = KeepaliveAdapter()
        adapter.max_retries = 5
        self._session.mount('https://', adapter)
        self._session.mount('http://', adapter)

    def send_request(self,
                     method,
                     url,
                     params=None,
                     data=None,
                     headers=None,
                     auth=None):
        """Send an HTTP request.

        :param method: HTTP method in lowercase (e.g. "post").
        :type method: str | unicode
        :param url: Request URL.
        :type url: str | unicode
        :param headers: Request headers.
        :type headers: dict
        :param params: URL (query) parameters.
        :type params: dict
        :param data: Request payload.
        :type data: str | unicode | bool | int | list | dict
        :param auth: Username and password.
        :type auth: tuple
        :returns: HTTP response.
        :rtype: c8.response.Response
        """
        # KARTIK : C8Platform#166 : explicitly set a keep-alive header
        if not headers:
            headers = {"Connection": "keep-alive"}
        else:
            if 'Connection' in headers:
                del headers["Connection"]
            headers["Connection"] = "keep-alive"

        retry = 5
        time_sleep = 5
        while(True):
            try:
                raw_resp = self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=headers,
                    auth=auth,
                    verify=False,
                    timeout=260
                )
                break
            except requests.ConnectionError:
                if retry == 0:
                    raise Exception(
                        "requests.ConnectionError: Not able to connect to "
                        "url: %s. Please make sure the federation is up and "
                        "running." % url)
                print("Error in connecting the url. Retring...")
                retry -= 1
                time.sleep(time_sleep)

        return Response(
            method=raw_resp.request.method,
            url=raw_resp.url,
            headers=raw_resp.headers,
            status_code=raw_resp.status_code,
            status_text=raw_resp.reason,
            raw_body=raw_resp.text,
        )
