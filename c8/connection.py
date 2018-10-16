from __future__ import absolute_import, unicode_literals

from c8.http import DefaultHTTPClient

import c8.constants as constants

__all__ = ['Connection']

### TODO : Functions API

class Connection(object):
    """HTTP connection to specific C8 tenant.

    :param url: C8Db base URL.
    :type url: str | unicode
    :param db: Database name.
    :type db: str | unicode
    :param username: Username.
    :type username: str | unicode
    :param password: Password.
    :type password: str | unicode
    :param http_client: User-defined HTTP client.
    :type http_client: c8.http.HTTPClient
    :param is_db: Whether this a DB or streams call. Anything other than streams is a DB call.
    :type is_db: bool
    """

    def __init__(self, url, tenantname, db_or_stream_name, username, password, http_client, is_db=True):
        self.url=url
        self._tenant_name = ""
        self._db_name = ""
        self._stream_name = ""
        self._username = username
        self._http_client = http_client or DefaultHTTPClient()

        # Multitenancy
        # TODO : Streams permissions?
        if not tenantname or self._tenant_name.isspace():
            self._tenant_name = constants.TENANT_DEFAULT 
        else:
            self._tenant_name=tenantname

        # Set the auth credentials depending on tenant name
        if self._tenant_name == '_mm':
            self._auth = (username, password)
        else:
            self._auth = (self._tenant_name+'.'+username, password)

        # Construct the URL prefix in the required format.
        # E.g. C8DB Connections: GET /_db/_tenant/{tenant-name}/_database/{db_name}/_admin/user/{user}/database/
        # E.g C8Streams Conns..:  /_streams/_tenant/<tenant-name>/_stream/<stream-name>/topics
        if(is_db):
            # Handle the DB side of things
            if not db_or_stream_name:
                self._db_name = constants.DB_DEFAULT
            else:
                self._db_name = db_or_stream_name

            # self._url_prefix = '{}/_db/{}'.format(url, self._db_name) # ORIG CODE
            #self._url_prefix = '{}/_db/tenant/{}/_database/{}'.format(url, self._tenant_name, self._db_name)
            self._url_prefix = '{}/_database/_tenant/{}/_db/{}'.format(url, self._tenant_name, self._db_name )

        else:
            # Handle the streams side of things
            if not db_or_stream_name:
                self._stream_name = constants.STREAMNAME_GLOBAL_SYSTEM_DEFAULT
            else:
                self._stream_name = db_or_stream_name

            self._url_prefix = '{}/_streams/tenant/{}'.format(url, self._tenant_name, self._stream_name)

        # TODO : Handle the functions side of things


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
    def db_name(self):
        """Return the DB name if it was called from the DB class

        :returns:  DB name.
        :rtype: str | unicode
        """
        return self._db_name

    @property
    def stream_name(self):
        """Return the Stream name if it was called from the Stream class

        :returns: Stream name.
        :rtype: str | unicode
        """
        return self._stream_name

    def set_url_prefix(self, new_prefix):
        """
        Set the URL prefix to the new prefix,
        returns (old_prefix,new_prefix) so it can be tracked.
        """
        old_prefix = self._url_prefix
        self._url_prefix = new_prefix
        return old_prefix,self._url_prefix

    def send_request(self, request):
        """Send an HTTP request to C8 server.

        :param request: HTTP request.
        :type request: c8.request.Request
        :return: HTTP response.
        :rtype: c8.response.Response
        """
        # Below line is a debug to show what the full request URL is. Useful in testing multitenancy API calls
        #print("KARTIK : CONN OBJECT : send_request called with URL: '"+self._url_prefix + request.endpoint+"'")
        return self._http_client.send_request(
            method=request.method,
            url=self._url_prefix + request.endpoint,
            params=request.params,
            data=request.data,
            headers=request.headers,
            auth=self._auth,
        )

class TenantConnection(Connection):
    """Tenant Connection wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, url, tenant, db, username, password, http_client):
        super(TenantConnection, self).__init__(url, tenant, db, username, password, http_client, is_db=True)
        self._fqdb_name = tenant+"."+db

    def __repr__(self):
        return '<TenantConnection {}>'.format(self._fqdb_name)

    @property
    def fqdb_name(self):
        """Return the fully qualified (i.e with tenant) database name.

        :returns: Tenant name.
        :rtype: str | unicode
        """
        return self._fqdb_name


class DatabaseConnection(Connection):
    """Database Connection wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, url, stream_port, tenant, db, username, password, http_client):
        super(DatabaseConnection, self).__init__(url, tenant, db, username, password, http_client, is_db=True)
        self._db_name = db
        self.stream_port = stream_port
    def __repr__(self):
        return '<DatabaseConnection {}>'.format(self._db_name)

    @property
    def db_name(self):
        """Return the database name.

        :returns: Database name.
        :rtype: str | unicode
        """
        return self._db_name

class StreamConnection(Connection):
    """Stream Connection wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, url, tenant, stream, username, password, http_client):
        super(StreamConnection, self).__init__(url, tenant, stream, username, password, http_client, is_db=False)
        self._stream_name = stream

    def __repr__(self):
        return '<StreamConnection {}>'.format(self._stream_name)

    @property
    def stream_name(self):
        """Return the stream name.

        :returns: Stream name.
        :rtype: str | unicode
        """
        return self._stream_name


class RealtimeConnection(Connection):
    """
    Realtime connection wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """
    # TODO : Topic persistence settings

    def __init__(self, url, tenant, db, username, password, http_client, topic):
        super(RealtimeConnection, self).__init__(url, tenant, db, username, password, http_client, is_db=False)
        self._topic_name = topic
        self._realtime_url = url
        # super's _url_prefix is useless in this case so override it here
        self._url_prefix = url


    def __repr__(self):
        return '<RealtimeConnection tenant={}, db={}, topic={}>'.format(self._tenant_name, self._db_name, self._topic_name)

    @property
    def topic_name(self):
        return self._topic_name

    @property
    def realtime_url(self):
        return self._realtime_url


# TODO : class FunctionConnection(Connection)
