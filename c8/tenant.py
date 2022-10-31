from __future__ import absolute_import, unicode_literals

import json

from c8.api import APIWrapper
from c8.exceptions import (
    BillingAccessLevel,
    ClearBillingAccessLevel,
    ClearCollectionAccessLevel,
    ClearDataBaseAccessLevel,
    ClearStreamAccessLevel,
    CollectionAccessLevel,
    DataBaseError,
    GetAttributes,
    GetDataBaseAccessLevel,
    ListStreams,
    PermissionListError,
    RemoveAllAttributes,
    RemoveAttribute,
    SetBillingAccessLevel,
    SetCollectionAccessLevel,
    SetDataBaseAccessLevel,
    SetStreamAccessLevel,
    SpotRegionAssignError,
    StreamAccessLevel,
    TenantCreateError,
    TenantDcListError,
    TenantDeleteError,
    TenantDetailsError,
    TenantListError,
    TenantUpdateError,
    UpdateAttributes,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserListError,
    UserUpdateError,
)
from c8.executor import DefaultExecutor
from c8.fabric import StandardFabric
from c8.request import Request

__all__ = ["Tenant"]


class Tenant(APIWrapper):
    """Base class for Tenant API wrappers.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection):

        super(Tenant, self).__init__(connection, executor=DefaultExecutor(connection))
        # self.get_auth_token_from_server()

    @property
    def name(self):
        """Return tenant name.

        :returns: tenant name.
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
        proto = oldconnprefix.split("//")[0]
        rema = oldconnprefix.split("//")[1].split("/")[0]

        # We set the temp URL prefix here for the auth call. It is restored
        # below
        self._conn.set_url_prefix(proto + "//" + rema)
        data = {"tenant": self.tenant_name}
        data["email"] = self._conn._email
        data["password"] = self._conn._password
        request = Request(method="post", endpoint="/_open/auth", data=data)

        def response_handler(resp):
            if not resp.is_success:
                raise "Authentication Error"
            if "jwt" not in resp.body:
                raise "Failed to fetch jwt"
            return resp.body["jwt"]

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

        :returns: JWT auth token stored for this tenant user
        :rtype: str | unicode
        """
        return self._conn._auth_token

    def useFabric(self, fabric_name):
        conn = self._conn
        conn.set_fabric_name(fabric_name)
        url_prefix = "{}/_fabric/{}/_api".format(conn.url, conn.fabric_name)
        conn.set_url_prefix(url_prefix)
        fabric = StandardFabric(conn)
        return fabric

    #######################
    # Tenant Management #
    #######################

    def tenants(self):
        """Return the names all tenants.

        :returns: Tenant names.
        :rtype: [str | unicode]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        self.auth_token
        request = Request(
            method="get",
            endpoint="/_api/tenants",
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantListError(resp, request)
            retval = []
            for item in resp.body["result"]:
                retval.append(item["tenant"])
            return retval

        return self._execute(request, response_handler, custom_prefix="")

    def has_tenant(self, name):
        """Check if a tenant exists.

        :param name: Tenant name.
        :type name: str | unicode
        :returns: True if tenant exists, False otherwise.
        :rtype: bool
        """
        return name in self.tenants()

    def create_tenant(
        self,
        email,
        display_name=None,
        passwd="",
        plan=None,
        attribution=None,
        dclist=[],
        metadata={},
        contact={},
    ):
        """Create a new tenant.

        :param email: Tenant email address.
        :type email: str | unicode
        :param display_name: Tenant display name.
        :type display_name: str | unicode
        :param passwd: Tenant password to be set.
        :type passwd: str
        :param plan: Tenant plan to be set (example: FREE, METERED, ENTERPRISE).
        :type plan: str | unicode
        :param attribution: Tenant attribution (example: Macrometa, Cox-Edge).
        :type attribution: str | unicode
        :param dclist: comma separated list of region where the tenant will be
                       created. If no value passed tenant will be created
                       globally.
        :type dclist: list
        :param metadata: Metadata info.
        :type metadata: dict
        :param contact: Contact info.
        :type contact: dict
        :returns: Created tenant details if creation was successful
        :rtype: dict
        :raise c8.exceptions.TenantCreateError: If create fails.
        """
        data = {}
        if display_name is not None:
            data["displayName"] = display_name
        data["email"] = email
        data["passwd"] = passwd
        if plan is not None:
            data["plan"] = plan.upper()
        if attribution is not None:
            data["attribution"] = attribution
        data["metadata"] = metadata
        data["contact"] = contact
        if dclist != "":
            data["dcList"] = dclist

        request = Request(method="post", endpoint="/_api/tenant", data=data)

        def response_handler(resp):
            if not resp.is_success:
                raise TenantCreateError(resp, request)
            return resp.body

        return self._execute(request, response_handler, custom_prefix="")

    def update_tenant(
        self, name, active=True, status="active", display_name=None, metadata=None
    ):
        """Update an existing tenant.

        :param name: Tenant name.
        :type name: str | unicode
        :param active: Whether the tenant is active or not.
        :type active: bool
        :param status: The current status of the tenant.
        :type status: str | unicode
        :param display_name: Display name of the tenant
        :type display_name: str | unicode
        :param metadata: Metadata info.
        :type metadata: [dict]
        :returns: True if the update was successful else False.
        :rtype: bool
        :raise c8.exceptions.TenantUpdateError: If update fails.
        """
        data = {}
        data["active"] = active
        data["status"] = status
        if display_name is not None:
            data["displayName"] = display_name
        if metadata is not None:
            data["metadata"] = metadata

        request = Request(
            method="patch",
            endpoint="/_api/tenant/{tenantname}".format(tenantname=name),
            data=data,
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantUpdateError(resp, request)
            return True

        return self._execute(request, response_handler, custom_prefix="")

    def get_tenant_details(self, name):
        """Get the details of the tenant.

        :param name: Tenant name.
        :type name: str | unicode
        :returns: Tenant Details
        :rtype: dict
        :raise c8.exceptions.TenantDetailsError: If retrieval of details fails.
        """
        request = Request(
            method="get", endpoint="/_api/tenant/{tenantname}".format(tenantname=name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantDetailsError(resp, request)
            return resp.body["result"]

        return self._execute(request, response_handler, custom_prefix="")

    def delete_tenant(self, name, ignore_missing=False):
        """Delete the tenant.

        :param name: Tenant name.
        :type name: str | unicode
        :param ignore_missing: Do not raise an exception on missing tenant.
        :type ignore_missing: bool
        :returns: True if tenant was deleted successfully, False if tenant
            was not found and **ignore_missing** was set to True.
        :rtype: bool
        :raise c8.exceptions.TenantDeleteError: If delete fails.
        """
        request = Request(
            method="delete",
            endpoint="/_api/tenant/{tenantname}".format(tenantname=name),
        )

        def response_handler(resp):
            if resp.error_code == 1228 and ignore_missing:
                return False
            if not resp.is_success:
                raise TenantDeleteError(resp, request)
            return resp.body["result"]

        return self._execute(request, response_handler, custom_prefix="")

    def dclist(self, detail=False):
        """Return the list of names of Datacenters

        :param detail: detail list of DCs if set to true else only DC names
        :type: boolean
        :returns: DC List.
        :rtype: [str | unicode ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        request = Request(method="get", endpoint="/datacenter/_tenant/%s" % self.name)

        def response_handler(resp):
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            if detail:
                return resp.body[0]["dcInfo"]
            dc_list = []
            for dc in resp.body[0]["dcInfo"]:
                dc_list.append(dc["name"])
            return dc_list

        return self._execute(request, response_handler, custom_prefix="")

    def localdc(self, detail=True):
        """Return the list of local Datacenters

        :param detail: detail list of DCs if set to true else only DC names
        :type detail: boolean
        :returns: DC List.
        :rtype: [str | dict ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        request = Request(method="get", endpoint="/datacenter/local")

        def response_handler(resp):
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            if detail:
                return resp.body
            return resp.body["name"]

        return self._execute(request, response_handler, custom_prefix="")

    def assign_dc_spot(self, dc, spot_region=False):
        """Assigns spot region of a fed

        :param dc: dc name
        :type dc: str
        :param spot_region: If True, makes the region a spot region
        :type spot_region: bool
        :returns: True if request successful, False otherwise
        :rtype: bool
        :raise c8.exceptions.SpotRegionAssignError: If assignment fails.
        """
        data = json.dumps(spot_region)
        request = Request(method="put", endpoint="/datacenter/{}/{}".format(dc, data))

        def response_handler(resp):
            if not resp.is_success:
                raise SpotRegionAssignError(resp, request)
            return True

        return self._execute(request, response_handler, custom_prefix="")

    ###################
    # User Management #
    ###################

    def has_user(self, username):
        """Check if user exists.

        :param username: Username.
        :type username: str | unicode
        :returns: True if user exists, False otherwise.
        :rtype: bool
        """
        return any(user["username"] == username for user in self.users())

    def users(self):
        """Return all user details.

        :returns: List of user details.
        :rtype: [dict]
        :raise c8.exceptions.UserListError: If retrieval fails.
        """
        request = Request(method="get", endpoint="/user")

        def response_handler(resp):
            if not resp.is_success:
                raise UserListError(resp, request)
            return [
                {
                    "display_name": record["displayName"],
                    "email": record["email"],
                    "username": record["user"],
                    "active": record["active"],
                    "extra": record["extra"],
                }
                for record in resp.body["result"]
            ]

        return self._execute(request, response_handler, custom_prefix="/_api")

    def user(self, username):
        """Return user details.

        :param username: Username.
        :type username: str | unicode
        :returns: User details.
        :rtype: dict
        :raise c8.exceptions.UserGetError: If retrieval fails.
        """
        request = Request(method="get", endpoint="/user/{}".format(username))

        def response_handler(resp):
            if not resp.is_success:
                raise UserGetError(resp, request)
            return {
                "display_name": resp.body["displayName"],
                "email": resp.body["email"],
                "username": resp.body["user"],
                "active": resp.body["active"],
                "extra": resp.body["extra"],
            }

        return self._execute(request, response_handler, custom_prefix="/_api")

    def create_user(self, email, password, display_name=None, active=True, extra=None):
        """Create a new user.

        :param email: Email address of the user.
        :type email: str | unicode
        :param password: Password to be set for the user.
        :type password: str | unicode
        :param display_name: Display name for the user.
        :type display_name: str | unicode
        :param active: True if user is active, False otherwise.
        :type active: bool
        :param extra: Additional data for the user.
        :type extra: dict
        :returns: New user details.
        :rtype: dict
        :raise c8.exceptions.UserCreateError: If create fails.
        """
        data = {"email": email, "passwd": password, "active": active}
        if display_name is not None:
            data["displayName"] = display_name
        if extra is not None:
            data["extra"] = extra

        request = Request(method="post", endpoint="/user", data=data)

        def response_handler(resp):
            if not resp.is_success:
                raise UserCreateError(resp, request)
            return {
                "display_name": resp.body["displayName"],
                "email": resp.body["email"],
                "username": resp.body["user"],
                "active": resp.body["active"],
                "extra": resp.body["extra"],
                "tenant": resp.body["tenant"],
            }

        return self._execute(request, response_handler, custom_prefix="/_api")

    def update_user(
        self,
        username,
        password=None,
        display_name=None,
        email=None,
        is_verified=None,
        active=None,
        extra=None,
    ):
        """Update a user.

        :param username: Username.
        :type username: str | unicode
        :param password: New password.
        :type password: str | unicode
        :param display_name: New display name for the user.
        :type display_name: str | unicode
        :param email: New email for the user.
        :type email: str | unicode
        :param is_verified: Whether the email is verified or not.
        :type is_verified: bool
        :param active: Whether the user is active.
        :type active: bool
        :param extra: Additional data for the user.
        :type extra: dict
        :returns: New user details.
        :rtype: dict
        :raise c8.exceptions.UserUpdateError: If update fails.
        """
        data = {}
        if password is not None:
            data["passwd"] = password
        if display_name is not None:
            data["displayName"] = display_name
        if email is not None and is_verified is not None:
            new_email = {"email": email, "isVerified": is_verified}
            data["newEmail"] = new_email
        if active is not None:
            data["active"] = active
        if extra is not None:
            data["extra"] = extra

        request = Request(
            method="patch", endpoint="/user/{user}".format(user=username), data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserUpdateError(resp, request)
            return {
                "display_name": resp.body["displayName"],
                "email": resp.body["email"],
                "username": resp.body["user"],
                "active": resp.body["active"],
                "extra": resp.body["extra"],
            }

        return self._execute(request, response_handler, custom_prefix="/_api")

    def delete_user(self, username, ignore_missing=False):
        """Delete a user.

        :param username: Username.
        :type username: str | unicode
        :param ignore_missing: Do not raise an exception on missing user.
        :type ignore_missing: bool
        :returns: True if user was deleted successfully, False if user was not
            found and **ignore_missing** was set to True.
        :rtype: bool
        :raise c8.exceptions.UserDeleteError: If delete fails.
        """
        request = Request(
            method="delete", endpoint="/user/{user}".format(user=username)
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            elif resp.status_code == 404 and ignore_missing:
                return False
            raise UserDeleteError(resp, request)

        return self._execute(request, response_handler, custom_prefix="/_api")

    def list_accessible_databases_user(self, username, full=False):
        """Lists accessible databases for a user.

        :param username: Username.
        :type username: str | unicode
        :param full: Return the full set of access levels for all databases
                and all collections if set to true.
        :type full: bool
        :returns: Object containing database details
        :rtype: list | object
        :raise c8.exceptions.DataBaseError: If request fails.
        """
        request = Request(
            method="get", endpoint="/user/{}/database?full={}".format(username, full)
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body["result"]
            raise DataBaseError(resp, request)

        return self._execute(request, response_handler, custom_prefix="/_api")

    def get_database_access_level_user(self, username, databasename=""):
        """Fetch the access level for a specific database.

        :param username: Username.
        :type username: str | unicode
        :param databasename: Database name.
        :type databasename: str | unicode
        :returns: Access Details
        :rtype: string
        :raise c8.exceptions.GetDataBaseAccessLevel: If request fails.
        """
        request = Request(
            method="get", endpoint="/user/{}/database/{}".format(username, databasename)
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body["result"]
            raise GetDataBaseAccessLevel(resp, request)

        return self._execute(request, response_handler, custom_prefix="/_api")

    def remove_database_access_level_user(self, username, databasename=""):
        """Clear the access level for the specific database of user.
        As consequence the default database access level is used.

        :param username: Username.
        :type username: str | unicode
        :param databasename: Database name.
        :type databasename: str | unicode
        :returns: Object containing database details
        :rtype: object
        :raise c8.exceptions.ClearDataBaseAccessLevel: If request fails.
        """
        request = Request(
            method="delete",
            endpoint="/user/{}/database/{}".format(username, databasename),
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            raise ClearDataBaseAccessLevel(resp, request)

        return self._execute(request, response_handler, custom_prefix="/_api")

    def set_database_access_level_user(self, username, databasename="", grant="ro"):
        """Set the access levels for the specific database of user.

        :param username: Username.
        :type username: str | unicode
        :param databasename: Database name.
        :type databasename: str | unicode
        :param grant: Grant accesslevel.
                    Use "rw" to set the database access level to Administrate .
                    Use "ro" to set the database access level to Access.
                    Use "none" to set the database access level to No access.
        :type grant: string
        :returns: Object containing database details
        :rtype: object
        :raise c8.exceptions.SetDataBaseAccessLevel: If request fails.
        """
        request = Request(
            method="put",
            endpoint="/user/{}/database/{}".format(username, databasename),
            data={"grant": grant},
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body
            raise SetDataBaseAccessLevel(resp, request)

        return self._execute(request, response_handler, custom_prefix="/_api")

    def list_accessible_collections_user(
        self, username, databasename="_system", full=False
    ):
        """Fetch the collection access level for a specific collection in a database.

        :param username: Name of the user
        :type username: string
        :param databasename: Name of the database
        :type databasename: string
        :param full: Return the full set of access levels for all collections.
        :type full: boolean
        :returns: Fetch the list of collections access level for a specific user.
        :rtype: string
        :raise c8.exceptions.CollectionAccessLevel: If request fails.
        """
        request = Request(
            method="get",
            endpoint="/user/{}/database/{}/collection?full={}".format(
                username, databasename, full
            ),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionAccessLevel(resp, request)
            else:
                return resp.body["result"]

        return self._execute(request, response_handler, custom_prefix="/_api")

    def get_collection_access_level_user(
        self, username, collection_name, databasename="_system"
    ):
        """Fetch the collection access level for a specific collection in a database.

        :param collection_name: Name of the collection
        :type collection_name: string
        :param databasename: Name of the database
        :type databasename: string
        :returns: AccessLevel of a db.
        :rtype: string
        :raise c8.exceptions.CollectionAccessLevel: If request fails.
        """
        request = Request(
            method="get",
            endpoint="/user/{}/database/{}/collection/{}".format(
                username, databasename, collection_name
            ),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionAccessLevel(resp, request)
            else:
                return resp.body["result"]

        return self._execute(request, response_handler, custom_prefix="/_api")

    def set_collection_access_level_user(
        self, username, collection_name, databasename="_system", grant="ro"
    ):

        """Set the collection access level for a specific collection in a database.

        :param collection_name: Name of the collection
        :type collection_name: string
        :param databasename: Name of the database
        :type databasename: string
        :param grant: Use "rw" to set the database access level to Administrate .
                      Use "ro" to set the database access level to Access.
                      Use "none" to set the database access level to No access.
        :type grant: string
        :returns: Accesslevel of a particular db.
        :rtype: Object
        :raise c8.exceptions.SetCollectionAccessLevel: If request fails.
        """
        request = Request(
            method="put",
            endpoint="/user/{}/database/{}/collection/{}".format(
                username, databasename, collection_name
            ),
            data={"grant": grant},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SetCollectionAccessLevel(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    def clear_collection_access_level_user(
        self, username, collection_name, databasename="_system"
    ):

        """Clear the collection access level for a specific collection in a database.

        :param collection_name: Name of the collection
        :type collection_name: string
        :param databasename: Name of the database
        :type databasename: string
        :returns: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearCollectionAccessLevel: If request fails.
        """
        request = Request(
            method="delete",
            endpoint="/user/{}/database/{}/collection/{}".format(
                username, databasename, collection_name
            ),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ClearCollectionAccessLevel(resp, request)
            else:
                if resp.body["error"] is False:
                    return True
                elif resp.body["error"] is True:
                    return False

        return self._execute(request, response_handler, custom_prefix="/_api")

    def list_accessible_streams_user(
        self, username, databasename="_system", full=False
    ):
        """Fetch the list of streams available to the specified user.

        :param databasename: Name of the database
        :type databasename: string
        :param full: Return the full set of access levels for all streams.
        :type full: boolean
        :returns: List of available databases.
        :rtype: list
        :raise c8.exceptions.ListStreams: If request fails.
        """
        request = Request(
            method="get",
            endpoint="/user/{}/database/{}/stream?full={}".format(
                username, databasename, full
            ),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ListStreams(resp, request)
            else:
                return resp.body["result"]

        return self._execute(request, response_handler, custom_prefix="/_api")

    def get_stream_access_level_user(
        self, username, streamname, databasename="_system"
    ):
        """Fetch the database access level for a specific stream.

        :param streamname: Name of the stream
        :type streamname: string
        :param databasename: Name of the database
        :type databasename: string
        :returns: AccessLevel of a db.
        :rtype: string
        :raise c8.exceptions.StreamAccessLevel: If request fails.
        """

        request = Request(
            method="get",
            endpoint="/user/{}/database/{}/stream/{}".format(
                username, databasename, streamname
            ),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise StreamAccessLevel(resp, request)
            else:
                return resp.body["result"]

        return self._execute(request, response_handler, custom_prefix="/_api")

    def set_stream_access_level_user(
        self, username, streamname, databasename="_system", grant="ro"
    ):

        """Set the database access level for a specific stream.

        :param streamname: Name of the stream
        :type streamname: string
        :param databasename: Name of the database
        :type databasename: string
        :param grant: Use "rw" to set the database access level to Administrate .
                      Use "ro" to set the database access level to Access.
                      Use "none" to set the database access level to No access.
        :type grant: string
        :returns: Accesslevel of a particular db.
        :rtype: Object
        :raise c8.exceptions.SetStreamAccessLevel: If request fails.
        """

        request = Request(
            method="put",
            endpoint="/user/{}/database/{}/stream/{}".format(
                username, databasename, streamname
            ),
            data={"grant": grant},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SetStreamAccessLevel(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    def clear_stream_access_level_user(
        self, username, streamname, databasename="_system"
    ):

        """Clear the database access level for a specific stream.

        :param streamname: Name of the stream
        :type streamname: string
        :param databasename: Name of the database
        :type databasename: string
        :returns: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearStreamAccessLevel: If request fails.
        """

        request = Request(
            method="delete",
            endpoint="/user/{}/database/{}/stream/{}".format(
                username, databasename, streamname
            ),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ClearStreamAccessLevel(resp, request)
            else:
                if resp.body["error"] is False:
                    return True
                elif resp.body["error"] is True:
                    return False

        return self._execute(request, response_handler, custom_prefix="/_api")

    def get_billing_access_level_user(self, username):
        """Fetch the billing access level.

        :returns: AccessLevel of billing.
        :rtype: string
        :raise c8.exceptions.BillingAccessLevel: If request fails.
        """
        request = Request(method="get", endpoint="/user/{}/billing".format(username))

        def response_handler(resp):
            if not resp.is_success:
                raise BillingAccessLevel(resp, request)
            else:
                return resp.body["result"]

        return self._execute(request, response_handler, custom_prefix="/_api")

    def set_billing_access_level_user(self, username, grant="ro"):

        """Set the billing access level for user.

        :param grant: Use "rw" to set the database access level to Administrate .
                      Use "ro" to set the database access level to Access.
                      Use "none" to set the database access level to No access.
        :type grant: string
        :returns: Billing Accesslevel of a particular user.
        :rtype: Object
        :raise c8.exceptions.SetBillingAccessLevel: If request fails.
        """
        request = Request(
            method="put",
            endpoint="/user/{}/billing".format(username),
            data={"grant": grant},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SetBillingAccessLevel(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    def clear_billing_access_level_user(self, username):

        """Clear the billing access level.

        :returns: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearBillingAccessLevel: If request fails.
        """
        request = Request(method="delete", endpoint="/user/{}/billing".format(username))

        def response_handler(resp):
            if not resp.is_success:
                raise ClearBillingAccessLevel(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    def get_attributes_user(self, username):

        """Fetch the list of attributes for the specified user.

        :returns: All attributes for the specified user.
        :rtype: dict
        :raise c8.exceptions.GetAttributes: If request fails.
        """
        request = Request(method="get", endpoint="/user/{}/attributes".format(username))

        def response_handler(resp):
            if not resp.is_success:
                raise GetAttributes(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    def update_attributes_user(self, username, attributes):

        """Update the list of attributes for the specified user.

        :param attributes: The key-value pairs of attributes with values that needs to be updated.
        :type attributes: dict
        :returns: The updated attributes.
        :rtype: Object
        :raise c8.exceptions.UpdateAttributes: If request fails.
        """
        request = Request(
            method="put",
            endpoint="/user/{}/attributes".format(username),
            data=attributes,
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UpdateAttributes(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    def remove_all_attributes_user(self, username):

        """Remove all attributes of the specified user.

        :returns: True if operation successful.
        :returns: All attributes for the specified user.
        :rtype: dict
        """
        request = Request(
            method="delete", endpoint="/user/{}/attributes/truncate".format(username)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise RemoveAllAttributes(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    def remove_attribute_user(self, username, attributeid):

        """Remove the specified attribute for the specified user.

        :param attributeid: Name of the attribute
        :type attributeid: string
        :returns: All attributes for the specified user.
        :rtype: dict
        :raise c8.exceptions.RemoveAttribute: If request fails.
        """
        request = Request(
            method="delete",
            endpoint="/user/{}/attributes/{}".format(username, attributeid),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise RemoveAttribute(resp, request)
            else:
                return resp.body

        return self._execute(request, response_handler, custom_prefix="/_api")

    #########################
    # Permission Management #
    #########################

    def permissions(self, username):
        """Return user permissions for all fabrics and collections.

        :param username: Username.
        :type username: str | unicode
        :returns: User permissions for all fabrics and collections.
        :rtype: dict
        :raise: c8.exceptions.PermissionListError: If retrieval fails.
        """
        request = Request(
            method="get",
            endpoint="/user/{}/database".format(username),
            params={"full": True},
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body["result"]
            raise PermissionListError(resp, request)

        return self._execute(request, response_handler, custom_prefix="/_api")
