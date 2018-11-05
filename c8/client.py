from __future__ import absolute_import, unicode_literals

__all__ = ['C8Client']

from c8.connection import FabricConnection
from c8.connection import StreamConnection
from c8.connection import TenantConnection
from c8.fabric import StandardFabric
from c8.tenant import Tenant
from c8.exceptions import ServerConnectionError
from c8.version import __version__
from c8 import constants


class C8Client(object):
    """C8Db client.

    :param protocol: Internet transfer protocol (default: "http").
    :type protocol: str | unicode
    :param host: C8Db host (default: "127.0.0.1").
    :type host: str | unicode
    :param port: C8Db port (default: 8529).
    :type port: int
    :param http_client: User-defined HTTP client.
    :type http_client: c8.http.HTTPClient
    """

    def __init__(self,
                 protocol='http',
                 host='127.0.0.1',
                 port=80,
                 stream_port=constants.STREAM_PORT,
                 http_client=None):
        self._protocol = protocol.strip('/')
        self._host = host.strip('/')
        self._port = int(port)
        if self._protocol == 'https':
            self._port = 443
        self._stream_port=int(stream_port)
        self._url = '{}://{}:{}'.format(self._protocol, self.host, self.port)
        self._http_client = http_client

    def __repr__(self):
        return '<C8Client {}>'.format(self._url)

    @property
    def version(self):
        """Return the client version.

        :return: Client version.
        :rtype: str | unicode
        """
        return __version__

    @property
    def protocol(self):
        """Return the internet transfer protocol (e.g. "http").

        :return: Internet transfer protocol.
        :rtype: str | unicode
        """
        return self._protocol

    @property
    def host(self):
        """Return the C8Db host.

        :return: C8Db host.
        :rtype: str | unicode
        """
        return self._host

    @property
    def port(self):
        """Return the C8Db port.

        :return: C8Db port.
        :rtype: int
        """
        return self._port

    @property
    def base_url(self):
        """Return the C8Db base URL.

        :return: C8Db base URL.
        :rtype: str | unicode
        """
        return self._url

    def tenant(self, name="guest", fabricname='_system', username='root', password='', verify=False):
        """Connect to a fabric and return the fabric API wrapper.

        :param name: Tenant name.
        :type name: str | unicode
        :param fabricname: Tenant fabric name.
        :type name: str | unicode
        :param username: Username for basic authentication.
        :type username: str | unicode
        :param password: Password for basic authentication.
        :type password: str | unicode
        :param verify: Verify the connection by sending a test request.
        :type verify: bool
        :return: Standard fabric API wrapper.
        :rtype: c8.fabric.StandardFabric
        :raise c8.exceptions.ServerConnectionError: If **verify** was set
            to True and the connection to C8Db fails.
        """
        connection = TenantConnection(
            url=self._url,
            tenant=name,
            fabric=fabricname,
            username=username,
            password=password,
            http_client=self._http_client
        )
        tenant = Tenant(connection)

        # TODO : handle verify

        return tenant



    def fabric(self, tenant="guest", name='_system', username='root', password='', verify=False):
        """Connect to a fabric and return the fabric API wrapper.

        :param name: Fabric name.
        :type name: str | unicode
        :param username: Username for basic authentication.
        :type username: str | unicode
        :param password: Password for basic authentication.
        :type password: str | unicode
        :param verify: Verify the connection by sending a test request.
        :type verify: bool
        :return: Standard fabric API wrapper.
        :rtype: c8.fabric.StandardFabric
        :raise c8.exceptions.ServerConnectionError: If **verify** was set
            to True and the connection to C8Db fails.
        """
        connection = FabricConnection(
            url=self._url,
            stream_port=self._stream_port,
            tenant=tenant,
            fabric=name,
            username=username,
            password=password,
            http_client=self._http_client
        )
        fabric = StandardFabric(connection)

        if verify:  # Check the server connection by making a read API call
            try:
                fabric.ping()
            except ServerConnectionError as err:
                raise err
            except Exception as err:
                raise ServerConnectionError('bad connection: {}'.format(err))

        return fabric