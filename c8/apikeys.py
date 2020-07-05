from c8.api import APIWrapper
from c8.request import Request
from c8.response import Response
import json

from c8.exceptions import (
    CreateAPIKey,
    RemoveAPIKey,
    ListDataBases,
    DataBaseAccessLevel,
    SetDataBaseAccessLevel,
    ClearDataBaseAccessLevel,
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

class APIKeys(APIWrapper):
    """Base class for API keys API wrappers.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
   
    """

    def __init__(self, connection, executor, keyid):
        super(APIKeys, self).__init__(connection, executor)
        self._keyid = keyid

    
    def create_api_key(self):
        """Creates an api key.
        
        :return: Creates an api key.
        :rtype: list
        :raise c8.exceptions.CreateAPIKey: If request fails.
        """
        request = Request(
            method='post',
            endpoint='/key',
            data={
                    "keyid": self._keyid
                }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CreateAPIKey(resp, request)
            else:
                return resp.body
                
        return self._execute(request, response_handler)

    
    def remove_api_key(self):
        """Removes an api key.
        :param keyid: Id of the key to be created.
        :type name: str | unicode
        :return: True if removed.
        :rtype: boolean
        :raise c8.exceptions.RemoveAPIKey: If request fails.
        """
        request = Request(
            method='delete',
            endpoint='/key/{}'.format(self._keyid),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise RemoveAPIKey(resp, request)
            else:
                if resp.body['error'] is False:
                    return True
                else:
                    return False
                
        return self._execute(request, response_handler)

    
    def list_accessible_databases(self):
        """Fetch the list of databases available to the specified keyid.
 
        :return: List of available databases.
        :rtype: list
        :raise c8.exceptions.ListDataBases: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/key/{}/database'.format(self._keyid),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ListDataBases(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def get_database_access_level(self, databasename):
        """Fetch the database access level for a specific database.


        :param databasename: Name of the database
        :type databasename: string
        :return: AccessLevel of a db.
        :rtype: string
        :raise c8.exceptions.DataBaseAccessLevel: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/key/{}/database/{}'.format(self._keyid, databasename),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DataBaseAccessLevel(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def set_database_access_level(self, databasename, grant='ro'):
       
        """Set the database access level for a specific database.

        :param databasename: Name of the database
        :type databasename: string
        :param grant   : Use "rw" to set the database access level to Administrate .
                         Use "ro" to set the database access level to Access.
                         Use "none" to set the database access level to No access.
        :type grant: string
        :return: Accesslevel of a particular db.
        :rtype: Object
        :raise c8.exceptions.SetDataBaseAccessLevel: If request fails.
        """
        request = Request(
            method='put',
            endpoint='/key/{}/database/{}'.format(self._keyid, databasename),
            data={
                "grant": grant
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SetDataBaseAccessLevel(resp, request)
            else:
                return resp.body
                
        return self._execute(request, response_handler)

    
    def clear_database_access_level(self, databasename):
       
        """Clear the database access level for a specific database.

        :param databasename: Name of the database
        :type databasename: string
        :return: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearDataBaseAccessLevel: If request fails.
        """
        request = Request(
            method='delete',
            endpoint='/key/{}/database/{}'.format(self._keyid, databasename),
           
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ClearDataBaseAccessLevel(resp, request)
            else:
                if resp.body['error'] is False:
                    return True
                elif resp.body['error'] is True:
                    return False
                
        return self._execute(request, response_handler)

#------------------------------------------------------------------------

    def get_collection_access_level(self, collection_name, databasename='_system'):
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
            endpoint='/key/{}/database/{}/collection/{}'.format(self._keyid,
                                                                     databasename,
                                                                     collection_name),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionAccessLevel(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def set_collection_access_level(self, collection_name, databasename='_system',
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
            endpoint='/key/{}/database/{}/collection/{}'.format(self._keyid,
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

    
    def clear_collection_access_level(self, collection_name, databasename='_system'):
       
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
            endpoint='/key/{}/database/{}/collection/{}'.format(self._keyid,
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

#---------------------------------------------------------------------------

    def list_accessible_streams(self, databasename='_system', full=False):
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
            endpoint='/key/{}/database/{}/stream?full={}'.format(self._keyid,
                                                                      databasename,
                                                                      full),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ListStreams(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def get_stream_access_level(self, streamname, databasename='_system', local=False):
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
            url = '/key/{}/database/{}/stream/{}?global=True'.format(self._keyid,
                                                                 databasename,
                                                                 streamname)
        elif local is True:
            url = '/key/{}/database/{}/stream/{}?global=False'.format(self._keyid,
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

    
    def set_stream_access_level(self, streamname, databasename='_system', grant='ro', local=False):
       
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
            url = '/key/{}/database/{}/stream/{}?global=True'.format(self._keyid,
                                                                 databasename,
                                                                 streamname)
        elif local is True:
            url = '/key/{}/database/{}/stream/{}?global=False'.format(self._keyid,
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

    
    def clear_stream_access_level(self, streamname, databasename='_system', local=False):
       
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
            url = '/key/{}/database/{}/stream/{}?global=True'.format(self._keyid,
                                                                 databasename,
                                                                 streamname)
        elif local is True:
            url = '/key/{}/database/{}/stream/{}?global=False'.format(self._keyid,
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

#-----------------------------------------------------------------------------

    def get_billing_access_level(self):
        """Fetch the billing access level.

        :return: AccessLevel of billing.
        :rtype: string
        :raise c8.exceptions.BillingAccessLevel: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/key/{}/billing'.format(self._keyid),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise BillingAcessLevel(resp, request)
            else:
                return resp.body['result']
                
        return self._execute(request, response_handler)

    
    def set_billing_access_level(self, grant='ro'):
       
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
            endpoint='/key/{}/billing'.format(self._keyid),
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

    
    def clear_billing_access_level(self):
       
        """Clear the billing access level.

        :return: True if operation successful.
        :rtype: booleaan
        :raise c8.exceptions.ClearBillingAccessLevel: If request fails.
        """
        request = Request(
            method='delete',
            endpoint='/key/{}/billing'.format(self._keyid),
           
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ClearBillingAccessLevel(resp, request)
            else:
                return resp.body
                
        return self._execute(request, response_handler)
