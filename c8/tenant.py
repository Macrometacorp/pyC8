from __future__ import absolute_import, unicode_literals
import json

from c8.api import APIWrapper
from c8.request import Request
from c8.fabric import StandardFabric
from c8.executor import (
    DefaultExecutor,
)

from c8.exceptions import (
    TenantDcListError,
    TenantUpdateError,
    TenantListError,
    TenantCreateError,
    TenantDeleteError,
    TenantUpdateError,
    PermissionListError,
    PermissionGetError,
    PermissionResetError,
    PermissionUpdateError,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserListError,
    UserReplaceError,
    UserUpdateError,
    SpotRegionAssignError,
    DataBaseError,
    GetDataBaseAccessLevel,
    SetDataBaseAccessLevel,
    SetCollectionAccessLevel,
    CollectionAccessLevel,
    ClearCollectionAccessLevel,
    ListStreams,
    StreamAccessLevel,
    SetStreamAccessLevel,
    ClearStreamAccessLevel,
    SetBillingAccessLevel,
    BillingAcessLevel,
    ClearBillingAccessLevel
)

__all__ = ['Tenant']


class Tenant(APIWrapper):
    """Base class for Tenant API wrappers.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection):
        
        super(Tenant, self).__init__(connection,
                                     executor=DefaultExecutor(connection))
        # self.get_auth_token_from_server()

    @property
    def name(self):
        """Return tenant name.

        :return: tenant name.
        :rtype: str | unicode
        """
        return self.tenant_name

    def get_auth_token_from_server(self):

        """
        Returns the JWT auth token which can be used in subsequent requests
        The login for the auth token is done using the username and password
        for the current tenant object.
        """
        # Do an auth login. We need to rewrite the connection's standard URL
        # prefix because the /_open/auth call does not expect anything past
        # the '_tenant/{tenantname}' part of the URL. So we rewrite it here
        # temporarily to do the auth call, then set it back
        oldconnprefix = self._conn.url_prefix
        proto = oldconnprefix.split('//')[0]
        rema = oldconnprefix.split('//')[1].split('/')[0]

        # We set the temp URL prefix here for the auth call. It is restored
        # below
        self._conn.set_url_prefix(proto + '//' + rema )
        data = {"tenant": self.tenant_name}
        data['email'] = self._conn._email
        data['password'] = self._conn._auth[1]
        request = Request(
            method='post',
            endpoint='/_open/auth',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise "Authentication Error"
            if 'jwt' not in resp.body:
                raise "Failed to fetch jwt"
            return resp.body['jwt']

        tok = self._execute(request, response_handler)
        # NOTE : Set tok as _self.auth_tok so other functions can pass
        # self._auth.tok to the request object as the Request.auth_tok field.
        self._conn._auth_token = tok

        # Set the connection object's url prefix back to what it was.
        self._conn.set_url_prefix(oldconnprefix)

    @property
    def auth_token(self):
        """Return the stored JWT auth token for this tenant & user, if stored.
        This will be stored after calling get_auth_token_from_server() above.

        :return: JWT auth token stored for this tenant user
        :rtype: str | unicode
        """
        return self._conn._auth_token

    def useFabric(self, fabric_name):
        conn = self._conn
        conn.set_fabric_name(fabric_name)
        url_prefix = '{}/_fabric/{}/_api'.format(conn.url, conn.fabric_name)
        conn.set_url_prefix(url_prefix)
        fabric = StandardFabric(conn)
        return fabric

    #######################
    # Tenant Management #
    #######################

    def tenants(self):
        """Return the names all tenants.
        :return: Tenant names.
        :rtype: [str | unicode]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        self.auth_token
        request = Request(
            method='get',
            endpoint='/tenants',
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantListError(resp, request)
            retval = []
            for item in resp.body['result']:
                retval.append(item['tenant'])
            return retval

        return self._execute(request, response_handler)

    def has_tenant(self, name):
        """Check if a tenant exists.
        :param name: Tenant name.
        :type name: str | unicode
        :return: True if tenant exists, False otherwise.
        :rtype: bool
        """
        return name in self.tenants()

    def create_tenant(self, email, passwd='', dclist=[], extra={}):
        """Create a new tenant.
        :param name: Tenant name.
        :type name: str | unicode
        :param passwd: What I presume is the tenant admin user password.
        :type passwd: str
        :param dclist: comma separated list of region where tenant will be
                       created. If no value passed tenant will be created
                       globally.
        :type dclist: list
        :param extra: Extra config info.
        :type extra: dict
        :return: True if tenant was created successfully.
        :rtype: bool
        :raise c8.exceptions.TenantCreateError: If create fails.
        Here is an example entry for parameter **users**:
        .. code-block:: python
            {
                'email': 'email'
                'passwd': 'password',
                'extra': {'Department': 'IT'}
            }
        """
        name = email.replace('@', "")
        name = name.replace('.', "")
        print(name)
        data = {'name': name}
        data['email'] = email
        data['passwd'] = passwd
        data['extra'] = extra
        if dclist != '':
            data['dcList'] = dclist

        request = Request(
            method='post',
            endpoint='/tenant',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantCreateError(resp, request)
            return True

        return self._execute(request, response_handler)

    def update_tenant(self, name, passwd='', extra={}):
        """Update a existing tenant.
        :param name: Tenant name.
        :type name: str | unicode
        :param passwd: What I presume is the tenant admin user password.
        :param extra: Extra config info.
        :type extra: [dict]
        :return: True if tenant was created successfully.
        :rtype: bool
        :raise c8.exceptions.TenantCreateError: If create fails.
        Here is an example entry for parameter **users**:
        .. code-block:: python
            {
                'username': 'john',
                'password': 'password',
                'active': True,
                'extra': {'Department': 'IT'}
            }
        """
        data = {'name': name}
        data['passwd'] = passwd
        data['extra'] = extra

        request = Request(
            method='patch',
            endpoint='/tenant/{tenantname}'.format(tenantname=name),
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantUpdateError
            return True

        return self._execute(request, response_handler)

    def delete_tenant(self, name, ignore_missing=False):
        """Delete the tenant.
        :param name: Tenant name.
        :type name: str | unicode
        :param ignore_missing: Do not raise an exception on missing tenant.
        :type ignore_missing: bool
        :return: True if tenant was deleted successfully, False if tenant
            was not found and **ignore_missing** was set to True.
        :rtype: bool
        :raise c8.exceptions.TenantDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/tenant/{tenantname}'.format(tenantname=name)
        )

        def response_handler(resp):
            if resp.error_code == 1228 and ignore_missing:
                return False
            if not resp.is_success:
                raise TenantDeleteError(resp, request)
            return resp.body['result']

        return self._execute(request, response_handler)

    def dclist(self, detail=False):
        """Return the list of names of Datacenters
        :param detail: detail list of DCs if set to true else only DC names
        :type: boolean
        :return: DC List.
        :rtype: [str | unicode ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/datacenter/_tenant/%s' % self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            if detail:
                return resp.body[0]["dcInfo"]
            dc_list = []
            for dc in resp.body[0]["dcInfo"]:
                dc_list.append(dc['name'])
            return dc_list

        return self._execute(request, response_handler)

    def localdc(self, detail=True):
        """Return the list of local Datacenters
        :param detail: detail list of DCs if set to true else only DC names
        :type: boolean
        :return: DC List.
        :rtype: [str | dict ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/datacenter/local'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            if detail:
                return resp.body
            return resp.body["name"]

        return self._execute(request, response_handler)

    def assign_dc_spot(self, dc, spot_region=False):
        """Assigns spot region of a fed
        :param: dc: dc name
        :type: str
        :param: spot_region: If True, makes the region a spot region
        :type: bool
        :return: True if request successful,false otherwise
        :rtype: bool
        :raise c8.exceptions.SpotRegionAssignError: If assignment fails.
        """
        data = json.dumps(spot_region)
        request = Request(
            method='put',
            endpoint='/datacenter/{}/{}'.format(dc, data)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SpotRegionAssignError(resp, request)
            return True

        return self._execute(request, response_handler)
        
    ###################
    # User Management #
    ###################

    def has_user(self, username):
        """Check if user exists.

        :param username: Username.
        :type username: str | unicode
        :return: True if user exists, False otherwise.
        :rtype: bool
        """
        return any(user['username'] == username for user in self.users())

    def users(self):
        """Return all user details.

        :return: List of user details.
        :rtype: [dict]
        :raise c8.exceptions.UserListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/user'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserListError(resp, request)
            return [{
                'username': record['user'],
                'active': record['active'],
                'extra': record['extra'],
            } for record in resp.body['result']]

        return self._execute(request, response_handler)

    def user(self, username):
        """Return user details.

        :param username: Username.
        :type username: str | unicode
        :return: User details.
        :rtype: dict
        :raise c8.exceptions.UserGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/user/{}'.format(username)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserGetError(resp, request)
            return {
                'username': resp.body['user'],
                'active': resp.body['active'],
                'extra': resp.body['extra']
            }

        return self._execute(request, response_handler)

    def create_user(self, username, email, password, active=True, extra=None):
        """Create a new user.

        :param username: Username.
        :type username: str | unicode
        :param password: Password.
        :type password: str | unicode
        :param active: True if user is active, False otherwise.
        :type active: bool
        :param extra: Additional data for the user.
        :type extra: dict
        :return: New user details.
        :rtype: dict
        :raise c8.exceptions.UserCreateError: If create fails.
        """
        data = {'user': username, 'email': email, 'passwd': password, 'active': active}
        if extra is not None:
            data['extra'] = extra

        request = Request(
            method='post',
            endpoint='/user',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserCreateError(resp, request)
            return {
                'username': resp.body['user'],
                'active': resp.body['active'],
                'extra': resp.body['extra'],
            }

        return self._execute(request, response_handler)

    def update_user(self, username, password=None, active=None, extra=None):
        """Update a user.

        :param username: Username.
        :type username: str | unicode
        :param password: New password.
        :type password: str | unicode
        :param active: Whether the user is active.
        :type active: bool
        :param extra: Additional data for the user.
        :type extra: dict
        :return: New user details.
        :rtype: dict
        :raise c8.exceptions.UserUpdateError: If update fails.
        """
        data = {}
        if password is not None:
            data['passwd'] = password
        if active is not None:
            data['active'] = active
        if extra is not None and "queries" not in data["extra"]:
            data['extra'] = extra

        request = Request(
            method='patch',
            endpoint='/user/{user}'.format(user=username),
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserUpdateError(resp, request)
            return {
                'username': resp.body['user'],
                'active': resp.body['active'],
                'extra': resp.body['extra'],
            }

        return self._execute(request, response_handler)

    def replace_user(self, username, password, active=None, extra=None):
        """Replace a user.

        :param username: Username.
        :type username: str | unicode
        :param password: New password.
        :type password: str | unicode
        :param active: Whether the user is active.
        :type active: bool
        :param extra: Additional data for the user.
        :type extra: dict
        :return: New user details.
        :rtype: dict
        :raise c8.exceptions.UserReplaceError: If replace fails.
        """
        data = {'user': username, 'passwd': password}
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra

        request = Request(
            method='put',
            endpoint='/user/{user}'.format(user=username),
            data=data
        )

        def response_handler(resp):
            if resp.is_success:
                return {
                    'username': resp.body['user'],
                    'active': resp.body['active'],
                    'extra': resp.body['extra'],
                }
            raise UserReplaceError(resp, request)

        return self._execute(request, response_handler)

    def delete_user(self, username, ignore_missing=False):
        """Delete a user.

        :param username: Username.
        :type username: str | unicode
        :param ignore_missing: Do not raise an exception on missing user.
        :type ignore_missing: bool
        :return: True if user was deleted successfully, False if user was not
            found and **ignore_missing** was set to True.
        :rtype: bool
        :raise c8.exceptions.UserDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/user/{user}'.format(user=username)
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            elif resp.status_code == 404 and ignore_missing:
                return False
            raise UserDeleteError(resp, request)

        return self._execute(request, response_handler)


    def list_accessible_databases_user(self, username, full=False):
        """Lists accessible databases for a user.

        :param username: Username.
        :type username: str | unicode
        :param full: Return the full set of access levels for all databases 
                and all collections if set to true.
        :type full: bool
        :return:Object containing database details
        :rtype: list | object
        :raise c8.exceptions.DataBaseError: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/user/{}/database?full={}'.format(username, full)
        )

        def response_handler(resp):
            if resp.is_success:
               return resp.body['result']
            raise DataBaseError(resp, request)

        return self._execute(request, response_handler)


    def get_database_access_level_user(self, username, databasename=""):
        """Lists accessible databases for a user.

        :param username: Username.
        :type username: str | unicode
        :param databasename: Database name.
        :type databasename: str | unicode
        :return: Access Details
        :rtype:string
        :raise c8.exceptions.DataBaseError: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/user/{}/database/{}'.format(username, databasename)
        )

        def response_handler(resp):
            if resp.is_success:
               return resp.body['result']
            raise DataBaseError(resp, request)

        return self._execute(request, response_handler)


    def remove_database_access_level_user(self, username, databasename=""):
        """Lists accessible databases for a user.

        :param username: Username.
        :type username: str | unicode
        :param databasename: Database name.
        :type databasename: str | unicode
        :return:Object containing database details
        :rtype: object
        :raise c8.exceptions.DataBaseError: If request fails.
        """
        request = Request(
            method='delete',
            endpoint='/user/{}/database/{}'.format(username, databasename),
        )

        def response_handler(resp):
            if resp.is_success:
               return resp.body
            raise DataBaseError(resp, request)

        return self._execute(request, response_handler)

    def set_database_access_level_user(self, username, databasename="", grant='ro'):
        """Lists accessible databases for a user.

        :param username: Username.
        :type username: str | unicode
        :param databasename: Database name.
        :type databasename: str | unicode
        :param grant: Grant accesslevel.
                    Use "rw" to set the database access level to Administrate .
                    Use "ro" to set the database access level to Access.
                    Use "none" to set the database access level to No access.
        :type grant: string
        :return:Object containing database details
        :rtype: object
        :raise c8.exceptions.DataBaseError: If request fails.
        """
        request = Request(
            method='put',
            endpoint='/user/{}/database/{}'.format(username, databasename),
            data={
                "grant": grant
            }
        )

        def response_handler(resp):
            if resp.is_success:
               return resp.body
            raise DataBaseError(resp, request)

        return self._execute(request, response_handler)


    def get_collection_access_level_user(self, username, collection_name, databasename='_system'):
        """Fetch the collection access level for a specific collection in a database.

        :param collection_name: Name of the collection
        :type collection_name: string
        :param databasename: Name of the database
        :type databasename: string
        :return: AccessLevel of a db.
        :rtype: string
        :raise c8.exceptions.CollectionAccessLevel: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/user/{}/database/{}/collection/{}'.format(username,
                                                                     databasename,
                                                                     collection_name),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionAccessLevel(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def set_collection_access_level_user(self, username, collection_name, databasename='_system',
                                     grant='ro'):
       
        """Set the collection access level for a specific collection in a database.

        :param collection_name: Name of the collection
        :type collection_name: string
        :param databasename: Name of the database
        :type databasename: string
        :param grant   : Use "rw" to set the database access level to Administrate .
                         Use "ro" to set the database access level to Access.
                         Use "none" to set the database access level to No access.
        :type grant: string
        :return: Accesslevel of a particular db.
        :rtype: Object
        :raise c8.exceptions.SetCollectionAccessLevel: If request fails.
        """
        request = Request(
            method='put',
            endpoint='/user/{}/database/{}/collection/{}'.format(username,
                                                                     databasename,
                                                                     collection_name),
            data={
                "grant": grant
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SetCollectionAccessLevel(resp, request)
            else:
                return resp.body
                
        return self._execute(request, response_handler)

    
    def clear_collection_access_level_user(self, username, collection_name, databasename='_system'):
       
        """Clear the collection access level for a specific collection in a database.

        :param collection_name: Name of the collection
        :type collection_name: string
        :param databasename: Name of the database
        :type databasename: string
        :return: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearCollectionAccessLevel: If request fails.
        """
        request = Request(
            method='delete',
            endpoint='/user/{}/database/{}/collection/{}'.format(username,
                                                                 databasename,
                                                                 collection_name),
           
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ClearCollectionAccessLevel(resp, request)
            else:
                if resp.body['error'] is False:
                    return True
                elif resp.body['error'] is True:
                    return False
                
        return self._execute(request, response_handler)



    def list_accessible_streams_user(self, username, databasename='_system', full=False):
        """Fetch the list of streams available to the specified keyid.
 
        :param databasename: Name of the database
        :type databasename: string
        :param full: Return the full set of access levels for all streams.
        :type full: boolean
        :return: List of available databases.
        :rtype: list
        :raise c8.exceptions.ListStreams: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/user/{}/database/{}/stream?full={}'.format(username,
                                                                      databasename,
                                                                      full),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ListStreams(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def get_stream_access_level_user(self, username, streamname, databasename='_system', local=False):
        """Fetch the database access level for a specific stream.

        :param streamname: Name of the stream
        :type streamname: string
        :param databasename: Name of the database
        :type databasename: string
        :return: AccessLevel of a db.
        :rtype: string
        :raise c8.exceptions.StreamAccessLevel: If request fails.
        """
        if local is False:
            url = '/user/{}/database/{}/stream/{}?global=True'.format(username,
                                                                 databasename,
                                                                 streamname)
        elif local is True:
            url = '/user/{}/database/{}/stream/{}?global=False'.format(username,
                                                                 databasename,
                                                                 streamname)
        request = Request(
            method='get',
            endpoint=url
        )

        def response_handler(resp):
            if not resp.is_success:
                raise StreamAccessLevel(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def set_stream_access_level_user(self, username, streamname, databasename='_system', grant='ro', local=False):
       
        """Set the database access level for a specific stream.

        :param streamname: Name of the stream
        :type streamname: string
        :param databasename: Name of the database
        :type databasename: string
        :param grant   : Use "rw" to set the database access level to Administrate .
                         Use "ro" to set the database access level to Access.
                         Use "none" to set the database access level to No access.
        :type grant: string
        :return: Accesslevel of a particular db.
        :rtype: Object
        :raise c8.exceptions.SetStreamAccessLevel: If request fails.
        """
        if local is False:
            url = '/user/{}/database/{}/stream/{}?global=True'.format(username,
                                                                 databasename,
                                                                 streamname)
        elif local is True:
            url = '/user/{}/database/{}/stream/{}?global=False'.format(username,
                                                                 databasename,
                                                                 streamname)

        request = Request(
            method='put',
            endpoint=url,
            data={
                "grant": grant
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SetStreamAccessLevel(resp, request)
            else:
                return resp.body
                
        return self._execute(request, response_handler)

    
    def clear_stream_access_level_user(self, username, streamname, databasename='_system', local=False):
       
        """Clear the database access level for a specific stream.

        :param streamname: Name of the stream
        :type streamname: string
        :param databasename: Name of the database
        :type databasename: string
        :return: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearStreamAccessLevel: If request fails.
        """

        if local is False:
            url = '/user/{}/database/{}/stream/{}?global=True'.format(username,
                                                                 databasename,
                                                                 streamname)
        elif local is True:
            url = '/user/{}/database/{}/stream/{}?global=False'.format(username,
                                                                 databasename,
                                                                 streamname)

        request = Request(
            method='delete',
            endpoint=url
           
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ClearStreamAccessLevel(resp, request)
            else:
                if resp.body['error'] is False:
                    return True
                elif resp.body['error'] is True:
                    return False
                
        return self._execute(request, response_handler)


    def get_billing_access_level_user(self, username):
        """Fetch the billing access level.

        :return: AccessLevel of billing.
        :rtype: string
        :raise c8.exceptions.BillingAccessLevel: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/user/{}/billing'.format(username),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise BillingAcessLevel(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def set_billing_access_level(self, username, grant='ro'):
       
        """Set the collection access level for billing.

        :param grant   : Use "rw" to set the database access level to Administrate .
                         Use "ro" to set the database access level to Access.
                         Use "none" to set the database access level to No access.
        :type grant: string
        :return: Accesslevel of a particular db.
        :rtype: Object
        :raise c8.exceptions.SetBillingAccessLevel: If request fails.
        """
        request = Request(
            method='put',
            endpoint='/user/{}/billing'.format(username),
            data={
                "grant": grant
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SetBillingAccessLevel(resp, request)
            else:
                return resp.body
                
        return self._execute(request, response_handler)

    
    def clear_billing_access_level(self, username):
       
        """Clear the billing access level.

        :return: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearBillingAccessLevel: If request fails.
        """
        request = Request(
            method='delete',
            endpoint='/user/{}/billing'.format(username),
           
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ClearBillingAccessLevel(resp, request)
            else:
                return resp.body
                
        return self._execute(request, response_handler)


    #########################
    # Permission Management #
    #########################

    def permissions(self, username):
        """Return user permissions for all fabrics and collections.

        :param username: Username.
        :type username: str | unicode
        :return: User permissions for all fabrics and collections.
        :rtype: dict
        :raise: c8.exceptions.PermissionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/user/{}/database'.format(username),
            params={'full': True}
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body['result']
            raise PermissionListError(resp, request)

        return self._execute(request, response_handler)

    def permission(self, username, fabric, collection=None):
        """Return user permission for a specific fabric or collection.

        :param username: Username.
        :type username: str | unicode
        :param fabric: fabric name.
        :type fabric: str | unicode
        :param collection: Collection name.
        :type collection: str | unicode
        :return: Permission for given fabric or collection.
        :rtype: str | unicode
        :raise: c8.exceptions.PermissionGetError: If retrieval fails.
        """
        endpoint = '/user/{}/database/{}'.format(username, fabric)
        if collection is not None:
            endpoint += '/' + collection
        request = Request(method='get', endpoint=endpoint)

        def response_handler(resp):
            if not resp.is_success:
                raise PermissionGetError(resp, request)
            return resp.body['result']

        return self._execute(request, response_handler)

    def update_permission(self,
                          username,
                          permission,
                          fabric,
                          collection=None):
        """Update user permission for a specific fabric or collection.

        :param username: Username.
        :type username: str | unicode
        :param fabric: fabric name.
        :type fabric: str | unicode
        :param collection: Collection name.
        :type collection: str | unicode
        :param permission: Allowed values are "rw" (read and write), "ro"
            (read only) or "none" (no access).
        :type permission: str | unicode
        :return: True if access was granted successfully.
        :rtype: bool
        :raise c8.exceptions.PermissionUpdateError: If update fails.
        """
        endpoint = '/user/{}/database/{}'.format(username, fabric)
        if collection is not None:
            endpoint += '/' + collection

        request = Request(
            method='put',
            endpoint=endpoint,
            data={'grant': permission}
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            raise PermissionUpdateError(resp, request)

        return self._execute(request, response_handler)

    def reset_permission(self, username, fabric, collection=None):
        """Reset user permission for a specific fabric or collection.

        :param username: Username.
        :type username: str | unicode
        :param fabric: fabric name.
        :type fabric: str | unicode
        :param collection: Collection name.
        :type collection: str | unicode
        :return: True if permission was reset successfully.
        :rtype: bool
        :raise c8.exceptions.PermissionRestError: If reset fails.
        """
        endpoint = '/user/{}/database/{}'.format(username, fabric)
        if collection is not None:
            endpoint += '/' + collection

        request = Request(method='delete', endpoint=endpoint)

        def response_handler(resp):
            if resp.is_success:
                return True
            raise PermissionResetError(resp, request)

        return self._execute(request, response_handler)
