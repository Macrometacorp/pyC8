from __future__ import absolute_import, unicode_literals
from json import dumps

from c8.api import APIWrapper
from c8.request import Request

from c8.exceptions import (
   ListCollections,
   CreateCollectionError,
   DeleteCollectionError,
   InsertKVError,
   GetValueError,
   DeleteEntryForKey,
   GetKeysError,
   GetCountError

)

class KV(APIWrapper):
    """KV (Key Value) API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection, executor):
        super(KV, self).__init__(connection, executor)


    def __repr__(self):
        return '<KV in {}>'.format(self._conn.fabric_name)


    def get_collections(self):
        """Returns the list of collections using kv.
        :return: Existing Collections.
        :rtype: list
        :raise c8.exceptions.ListCollections: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/kv'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ListCollections(resp, request)
            else:
                return resp.body["result"]
                
        return self._execute(request, response_handler)

    
    def create_collection(self, name, expiration=False):
        """Creates Collection.

        :param name: Collection name.
        :type name: str | unicode
        :param expiration:if True then the namesapce supports TTL.
        :type expiration: boolean
        :return: True if the request is successful.
        :rtype: boolean
        :raise c8.exceptions.CreateCollectionError: If creation fails.
        """
        request = Request(
            method='post',
            endpoint='/kv/{}?expiration={}'.format(name, expiration)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ListCollections(resp, request)
            else:
                if resp.body["error"] is False and resp.body["name"] == name:
                    return True
                else:
                    return False
                
        return self._execute(request, response_handler)

    def has_collection(self, name):
        """Checks if a Collection exists.

        :param name: Collection name.
        :type name: str | unicode
        :return: True if the collection exists.
        :rtype: boolean
        """
        exists = False
        collections = self.get_collections()
        for collection in collections:
            if collection['name'] == name:
                exists = True
                break
            else:
                exists = False
        return exists
            


    def delete_collection(self, name):
        """Deletes Collection.
        :param name: Collection name.
        :type name: str | unicode
        :return: True if the request is successful.
        :rtype: boolean
        :raise c8.exceptions.DeleteCollectionError: If creation fails.
        """
        request = Request(
            method='delete',
            endpoint='/kv/{}'.format(name)
        )
        def response_handler(resp):
            if not resp.is_success:
                raise DeleteCollectionError(resp, request)
            else:
                if resp.body["error"] is False and resp.body["name"] == name:
                    return True
                else:
                    return False
        return self._execute(request, response_handler)

    
    def insert_key_value_pair(self, name, data=None):
        """Set a key value pair.

        :param name: Collection name.
        :type name: str | unicode
        :param data: objects to be inserted.
        :type data: list
        :return: List of inserted objects.
        :rtype: list
        :raise c8.exceptions.InsertKVError: If insertion fails.
        """
        request = Request(
            method='put',
            endpoint='/kv/{}/value'.format(name),
            data=dumps(data)

        )
        def response_handler(resp):
            if not resp.is_success:
                raise InsertKVError(resp, request)
            else:
                return resp.body
        return self._execute(request, response_handler)

    
    def delete_entry_for_key(self, name, key):
        """Delete an entry for a key.

        :param name: Collection name.
        :type name: str | unicode
        :param key: The key for which the object is to be deleted.
        :type data: string
        :return: True if successfully deleted.
        :rtype: boolean
        :raise c8.exceptions.DeleteEntryForKey: If deletion fails.
        """
        request = Request(
            method='delete',
            endpoint='/kv/{}/value/{}'.format(name, key)
        )
        def response_handler(resp):
            if not resp.is_success:
                raise DeleteEntryForKey(resp, request)
            else:
                if resp.body["_key"] == key:
                    return True
                else:
                    return False
        return self._execute(request, response_handler)

    
    def delete_entry_for_keys(self, name, keys=[]):
        """Deletes entries for multiple keys.

        :param name: Collection name.
        :type name: str | unicode
        :param keys: The keys for which the object is to be deleted.
        :type data: list
        :return: List of deleted objects
        :rtype: List
        :raise c8.exceptions.DeleteEntryForKey: If deletion fails.
        """
        request = Request(
            method='delete',
            endpoint='/kv/{}/values'.format(name),
            data=dumps(keys)
        )
        def response_handler(resp):
            if not resp.is_success:
                raise DeleteEntryForKey(resp, request)
            else:
                return resp.body
        return self._execute(request, response_handler)

    
    def get_value_for_key(self, name, key):
        """Delete an entry for a key.

        :param name: Collection name.
        :type name: str | unicode
        :param key: The key for which the object is to be deleted.
        :type data: string
        :return: The value object.
        :rtype: object
        :raise c8.exceptions.GetValueError: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/kv/{}/value/{}'.format(name, key)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ListCollections(resp, request)
            else:
                return(resp.body)
                
        return self._execute(request, response_handler)

    
    def get_keys(self, name):
        """gets keys of a collection.

        :param name: Collection name.
        :type name: str | unicode
        :return: List of Keys.
        :rtype: list
        :raise c8.exceptions.GetKeysError: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/kv/{}/keys'.format(name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise GetKeysError(resp, request)
            else:
                return(resp.body["result"])
                
        return self._execute(request, response_handler)


    def get_kv_count(self, name):
        """gets the kv count of a collection.

        :param name: Collection name.
        :type name: str | unicode
        :return:Number of kv entries.
        :rtype: int
        :raise c8.exceptions.GetCountError: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/kv/{}/count'.format(name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise GetCountError(resp, request)
            else:
                return(resp.body["count"])
                
        return self._execute(request, response_handler)






