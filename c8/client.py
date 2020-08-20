from __future__ import absolute_import, unicode_literals

from c8.connection import TenantConnection
from c8.fabric import StandardFabric
from c8.tenant import Tenant
from c8.exceptions import ServerConnectionError
from c8.version import __version__
from c8 import constants

__all__ = ['C8Client']


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

    def __init__(self, protocol="http", host='127.0.0.1', port=80,
                 geofabric="_system",stream_port=constants.STREAM_PORT,
                 email=None, password=None,  http_client=None, token=None, apikey=None):
        
        self._protocol = protocol.strip('/')
        self._host = host.strip('/')
        self._port = int(port)
        self._email = email
        self._password = password
        self._fabricname = geofabric
        self._token = token
        self._apikey = apikey
        if self._protocol == 'https':
            self._port = 443
        self._stream_port = int(stream_port)
        if "api-" in self.host:
            self._url = '{}://{}:{}'.format(self._protocol, self.host, self.port)
        else:
            self._url = '{}://api-{}:{}'.format(self._protocol, self.host, self.port)
        self._http_client = http_client

        if self._email and self._password:  
            self._tenant = self.tenant(email=self._email, password=self._password)
            self._fabric = self._tenant.useFabric(self._fabricname)
        
        if self._token:
            self._tenant = self.tenant(token=self._token)
            self._fabric = self._tenant.useFabric(self._fabricname)
        
        if self._apikey:
            self._tenant = self.tenant(apikey=self._apikey)
            self._fabric = self._tenant.useFabric(self._fabricname)



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

    def tenant(self, email='', password='', verify=False, token=None, apikey=None):
        """Connect to a fabric and return the fabric API wrapper.

        :param email: Email for basic authentication.
        :type email: str | unicode
        :param password: Password for basic authentication.
        :type password: str | unicode
        :param verify: Verify the connection by sending a test request.
        :type verify: bool
        :return: Standard fabric API wrapper.
        :rtype: c8.fabric.StandardFabric
        :raise c8.exceptions.ServerConnectionError: If **verify** was set
            to True and the connection to C8Db fails.
        """
        connection = TenantConnection(url=self._url, 
                                      email=email,
                                      password=password,
                                      token=token,
                                      apikey=apikey,
                                      http_client=self._http_client)
        tenant = Tenant(connection)

        # TODO : handle verify

        return tenant

    # Reducing steps

    # client.get_fabric_details
    def get_fabric_details(self):
        return self._fabric.fabrics_detail()
        

    # client.collection
    def collection(self, collection_name):
        return self._fabric.collection(collection_name)

    # client.create_collection
    def create_collection(self,
                          name,
                          sync=False,
                          edge=False,
                          user_keys=True,
                          key_increment=None,
                          key_offset=None,
                          key_generator='traditional',
                          shard_fields=None,
                          index_bucket_count=None,
                          sync_replication=None,
                          enforce_replication_factor=None,
                          spot_collection=False,
                          local_collection=False,
                          is_system=False
                          ):
        """Create a new collection.

        :param name: Collection name.
        :type name: str | unicode
        :param sync: If set to True, document operations via the collection
            will block until synchronized to disk by default.
        :type sync: bool
        :param edge: If set to True, an edge collection is created.
        :type edge: bool
        :param key_generator: Used for generating document keys. Allowed values
            are "traditional" or "autoincrement".
        :type key_generator: str | unicode
        :param user_keys: If set to True, users are allowed to supply document
            keys. If set to False, the key generator is solely responsible for
            supplying the key values.
        :type user_keys: bool
        :param key_increment: Key increment value. Applies only when value of
            **key_generator** is set to "autoincrement".
        :type key_increment: int
        :param key_offset: Key offset value. Applies only when value of
            **key_generator** is set to "autoincrement".
        :type key_offset: int
        :param shard_fields: Field(s) used to determine the target shard.
        :type shard_fields: [str | unicode]
        :param index_bucket_count: Number of buckets into which indexes using
            hash tables are split. The default is 16, and this number has to be
            a power of 2 and less than or equal to 1024. For large collections,
            one should increase this to avoid long pauses when the hash table
            has to be initially built or re-sized, since buckets are re-sized
            individually and can be initially built in parallel. For instance,
            64 may be a sensible value for 100 million documents.
        :type index_bucket_count: int
        :param sync_replication: If set to True, server reports success only
            when collection is created in all replicas. You can set this to
            False for faster server response, and if full replication is not a
            concern.
        :type sync_replication: bool
        :param enforce_replication_factor: Check if there are enough replicas
            available at creation time, or halt the operation.
        :type enforce_replication_factor: bool
        :param spot_collection: If True, it is a spot collection
        :type bool
        :param is_system: If True, able to create system collections
        :type is_system: bool
        :return: Standard collection API wrapper.
        :rtype: c8.collection.StandardCollection
        """
        resp = self._fabric.create_collection(name=name,
                                              sync=sync,
                                              edge=edge,
                                              user_keys=user_keys,
                                              key_increment=key_increment,
                                              key_offset=key_offset,
                                              key_generator=key_generator,
                                              shard_fields=shard_fields,
                                              index_bucket_count=index_bucket_count,
                                              sync_replication=sync_replication,
                                              enforce_replication_factor=enforce_replication_factor,
                                              spot_collection=spot_collection,
                                              local_collection=local_collection,
                                              is_system=is_system)
        return resp

    # client.list_collection_indexes
    def list_collection_indexes(self, collection_name):
        """Delete the collection.
        :param name: Collection name.
        :type name: str | unicode
        :return: List of indexes
        :rtype: bool
        """
        _collection = self.get_collection(collection_name)
        return _collection.indexes()


    # client.add_hash_index
    def add_hash_index(self,
                       collection_name,
                       fields,
                       unique=None,
                       sparse=None,
                       deduplicate=None):
        """Create a new hash index.

        :param name: Collection name to add index on.
        :type name: str | unicode
        :param fields: Document fields to index.
        :type fields: [str | unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: If set to True, documents with None in the field
            are also indexed. If set to False, they are skipped.
        :type sparse: bool
        :param deduplicate: If set to True, inserting duplicate index values
            from the same document triggers unique constraint errors.
        :type deduplicate: bool
        :return: New index details.
        :rtype: dict
        :raise c8.exceptions.IndexCreateError: If create fails.
        """
        _collection = self.get_collection(collection_name)
        return _collection.add_hash_index(fields=fields,
                                          unique=unique,
                                          sparse=sparse,
                                          deduplicate=deduplicate)

    
    # client.add_geo_index
    def add_geo_index(self, collection_name, fields, ordered=None):
        """Create a new geo-spatial index.

        :param name: Collection name to add index on.
        :type name: str | unicode
        :param fields: A single document field or a list of document fields. If
            a single field is given, the field must have values that are lists
            with at least two floats. Documents with missing fields or invalid
            values are excluded.
        :type fields: str | unicode | list
        :param ordered: Whether the order is longitude, then latitude.
        :type ordered: bool
        :return: New index details.
        :rtype: dict
        :raise c8.exceptions.IndexCreateError: If create fails.
        """
        _collection = self.get_collection(collection_name)
        return _collection.add_geo_index(fields=fields,
                                          ordered=ordered)


    # client.add_skiplist_index
    def add_skiplist_index(self,
                           collection_name,
                           fields,
                           unique=None,
                           sparse=None,
                           deduplicate=None):
        """Create a new skiplist index.

        :param name: Collection name to add index on.
        :type name: str | unicode
        :param fields: Document fields to index.
        :type fields: [str | unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: If set to True, documents with None in the field
            are also indexed. If set to False, they are skipped.
        :type sparse: bool
        :param deduplicate: If set to True, inserting duplicate index values
            from the same document triggers unique constraint errors.
        :type deduplicate: bool
        :return: New index details.
        :rtype: dict
        :raise c8.exceptions.IndexCreateError: If create fails.
        """
        _collection = self.get_collection(collection_name)
        return _collection.add_skiplist_index(fields=fields,
                                          unique=unique,
                                          sparse=sparse,
                                          deduplicate=deduplicate)

    
    # client.add_persistent_index
    def add_persistent_index(self, collection_name,
                             fields, unique=None, sparse=None):
        """Create a new persistent index.

        Unique persistent indexes on non-sharded keys are not supported in a
        cluster.

        :param name: Collection name to add index on.
        :type name: str | unicode
        :param fields: Document fields to index.
        :type fields: [str | unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: Exclude documents that do not contain at least one of
            the indexed fields, or documents that have a value of None in any
            of the indexed fields.
        :type sparse: bool
        :return: New index details.
        :rtype: dict
        :raise c8.exceptions.IndexCreateError: If create fails.
        """
        _collection = self.get_collection(collection_name)
        return _collection.add_persistent_index(fields=fields,
                                          unique=unique,
                                          sparse=sparse)


    # client.add_fulltext_index
    def add_fulltext_index(self, collection_name, fields, min_length=None):
        """Create a new fulltext index.
        
        :param name: Collection name to add index on.
        :type name: str | unicode
        :param fields: Document fields to index.
        :type fields: [str | unicode]
        :param min_length: Minimum number of characters to index.
        :type min_length: int
        :return: New index details.
        :rtype: dict
        :raise c8.exceptions.IndexCreateError: If create fails.
        """
        _collection = self.get_collection(collection_name)
        return _collection.add_fulltext_index(fields=fields, min_length=min_length)


    # client.add_ttl_index
    def add_ttl_index(self, collection_name, fields, expireAfter=0, inBackground=False):
        """Create a new ttl index.
        
        :param name: Collection name to add index on.
        :type name: str | unicode
        :param fields: Document fields to index.
        :type fields: [str | unicode]
        :param expireAfter:  The time (in seconds) after
         a document's creation after which the documents count as "expired".
        :type expireAfter: int
        :param inBackground: Expire Documents in Background.
        :type inBackground: bool
        :return: New index details.
        :rtype: dict
        :raise c8.exceptions.IndexCreateError: If create fails.
        """
        _collection = self.get_collection(collection_name)
        return _collection.add_ttl_index(fields=fields, expireAfter=expireAfter,
                                         inBackground=inBackground)


    # client.delete_index
    def delete_index(self, collection_name, index_id, ignore_missing=False):
        """Delete an index.

        :param index_id: Index ID.
        :type index_id: str | unicode
        :param ignore_missing: Do not raise an exception on missing index.
        :type ignore_missing: bool
        :return: True if index was deleted successfully, False if index was
            not found and **ignore_missing** was set to True.
        :rtype: bool
        :raise c8.exceptions.IndexDeleteError: If delete fails.
        """
        _collection = self.get_collection(collection_name)
        return _collection.delete_index(index_id=index_id, ignore_missing=ignore_missing)


    # client.get_index
    def get_index(self, collection_name, index_name):
        _collection = self.get_collection(collection_name)
        return _collection.get_index(index_name)


    # client.delete_collection
    def delete_collection(self, name, ignore_missing=False, system=None):
        """Delete the collection.
        :param name: Collection name.
        :type name: str | unicode
        :param ignore_missing: Do not raise an exception on missing collection.
        :type ignore_missing: bool
        :param system: Whether the collection is a system collection.
        :type system: bool
        :return: True if collection was deleted successfully, False if
            collection was not found and **ignore_missing** was set to True.
        :rtype: bool
        """
        resp = self._fabric.delete_collection(name=name, ignore_missing=ignore_missing,
                                              system=system)
        return resp
    

    # client.has_collection
    def has_collection(self, name):
        """Delete the collection.
        :param name: Collection name.
        :type name: str | unicode
        :return: True if collection exists, False otherwise.
        :rtype: bool
        """
        resp = self._fabric.has_collection(name)
        return resp

    
    # client.get_collections
    def get_collections(self):
        """Return the collections in the fabric.

        :return: Collections in the fabric and their details.
        :rtype: [dict]
        :raise c8.exceptions.CollectionListError: If retrieval fails.
        """
        resp = self._fabric.collections()
        return resp
    

    # client.get_collection
    def get_collection(self, name):
        """Return the standard collection API wrapper.
        :param name: Collection name.
        :type name: str | unicode
        :return: Standard collection API wrapper.
        :rtype: c8.collection.StandardCollection
        """
        resp = self._fabric.collection(name)
        return resp
    
    def printdata(self, event):
        """Prints the event.

        :param event: real-time update.
        :type event: str | unicode
        """
        print(event)


    # client.on_change
    def on_change(self, collection, callback):
        resp = self._fabric.on_change(collection, callback)
        return resp

    # client.get_document
    def get_document(self, collection, document, rev=None, check_rev=True):
        """Return a document.

        :param collection: Collection Name
        :type document: str 
        :param document: Document ID, key or body. Document body must contain
            the "_id" or "_key" field.
        :type document: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **document** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **document** (if given)
            is compared against the revision of target document.
        :type check_rev: bool
        :return: Document, or None if not found.
        :rtype: dict | None
        :raise c8.exceptions.DocumentGetError: If retrieval fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        _collection = self.get_collection(collection)
        resp = _collection.get(document=document, rev=rev, check_rev=check_rev)
        return resp


    # client.insert_document
    def insert_document(self, collection_name="", return_new=False,
                silent=False, sync=None, document=None):
        """Insert a new document.

        :param document: Document to insert. If it contains the "_key" or "_id"
            field, the value is used as the key of the new document (otherwise
            it is auto-generated). Any "_rev" field is ignored.
        :type document: dict
        :param return_new: Include body of the new document in the returned
            metadata. Ignored if parameter **silent** is set to True.
        :type return_new: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        """
        _collection = self.get_collection(collection_name)

        if isinstance(document, dict):
           resp =  _collection.insert(document=document ,return_new=return_new, 
                                      sync=sync, silent=silent)
        elif isinstance(document, list):
            resp = _collection.insert_many(documents=document, return_new=return_new, 
                                            sync=sync, silent=silent)
        print(resp)
        return resp      

    # client.insert_document_from_file()
    def insert_document_from_file(self, collection_name, csv_filepath, return_new=False, sync=None,
                         silent=False):
        """Insert a documents from csv file.

        :param csv_filepath: CSV file path which contains documents
        :type csv_filepath: str
        :param return_new: Include body of the new document in the returned
            metadata. Ignored if parameter **silent** is set to True.
        :type return_new: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        """
        _collection = self.get_collection(collection_name)
        resp = _collection.insert_from_file(csv_filepath=csv_filepath, return_new=return_new,
                                             sync=sync, silent=silent)
        return resp
    

    # client.update_document
    def update_document(self,
               collection_name,
               document,
               check_rev=True,
               merge=True,
               keep_none=True,
               return_new=False,
               return_old=False,
               sync=None,
               silent=False):
        """Update a document.

        :param document: Partial or full document with the updated values. It
            must contain the "_id" or "_key" field.
        :type document: dict
        :param check_rev: If set to True, revision of **document** (if given)
            is compared against the revision of target document.
        :type check_rev: bool
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new one overwriting the old one.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: Include body of the new document in the result.
        :type return_new: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise c8.exceptions.DocumentUpdateError: If update fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        _collection = self.get_collection(collection_name)
        resp = _collection.update(document=document,
                                  check_rev=check_rev,
                                  merge=merge,
                                  keep_none=keep_none,
                                  return_new=return_new,
                                  return_old=return_old,
                                  sync=sync,
                                  silent=silent)
        return resp


    # client.update_document_many
    def update_document_many(self,
                    collection_name,
                    documents,
                    check_rev=True,
                    merge=True,
                    keep_none=True,
                    return_new=False,
                    return_old=False,
                    sync=None,
                    silent=False):
        """Update multiple documents.

        If updating a document fails, the exception object is placed in the
        result list instead of document metadata.

        :param documents: Partial or full documents with the updated values.
            They must contain the "_id" or "_key" fields.
        :type documents: [dict]
        :param check_rev: If set to True, revisions of **documents** (if given)
            are compared against the revisions of target documents.
        :type check_rev: bool
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new ones overwriting the old ones.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: List of document metadata (e.g. document keys, revisions) and
            any exceptions, or True if parameter **silent** was set to True.
        :rtype: [dict | C8Error] | bool
        :raise c8.exceptions.DocumentUpdateError: If update fails.
        """
        _collection = self.get_collection(collection_name)
        resp = _collection.update_document_many(documents=documents,
                                  check_rev=check_rev,
                                  merge=merge,
                                  keep_none=keep_none,
                                  return_new=return_new,
                                  return_old=return_old,
                                  sync=sync,
                                  silent=silent)
        return resp


    # client.replace_document
    def replace_document(self,
                     collection_name,
                     document,
                     check_rev=True,
                     return_new=False,
                     return_old=False,
                     sync=None,
                     silent=False):
        """Replace multiple documents.

        If replacing a document fails, the exception object is placed in the
        result list instead of document metadata.

        :param documents: New documents to replace the old ones with. They must
            contain the "_id" or "_key" fields. Edge documents must also have
            "_from" and "_to" fields.
        :type documents: [dict]
        :param check_rev: If set to True, revisions of **documents** (if given)
            are compared against the revisions of target documents.
        :type check_rev: bool
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: List of document metadata (e.g. document keys, revisions) and
            any exceptions, or True if parameter **silent** was set to True.
        :rtype: [dict | C8Error] | bool
        :raise c8.exceptions.DocumentReplaceError: If replace fails.
        """
        _collection = self.get_collection(collection_name)
        resp = _collection.replace(document=document,
                                   check_rev=check_rev,
                                   return_new=return_new,
                                   return_old=return_old,
                                   sync=sync,
                                   silent=silent)
        return resp

    
    #client.replace_document_many
    def replace_document_many(self,
                     collection_name,
                     documents,
                     check_rev=True,
                     return_new=False,
                     return_old=False,
                     sync=None,
                     silent=False):
        """Replace multiple documents.

        If replacing a document fails, the exception object is placed in the
        result list instead of document metadata.

        :param documents: New documents to replace the old ones with. They must
            contain the "_id" or "_key" fields. Edge documents must also have
            "_from" and "_to" fields.
        :type documents: [dict]
        :param check_rev: If set to True, revisions of **documents** (if given)
            are compared against the revisions of target documents.
        :type check_rev: bool
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: List of document metadata (e.g. document keys, revisions) and
            any exceptions, or True if parameter **silent** was set to True.
        :rtype: [dict | C8Error] | bool
        :raise c8.exceptions.DocumentReplaceError: If replace fails.
        """
        _collection = self.get_collection(collection_name)
        resp = _collection.replace_many(documents=documents,
                                   check_rev=check_rev,
                                   return_new=return_new,
                                   return_old=return_old,
                                   sync=sync,
                                   silent=silent)
        return resp
    
    
    # client.delete_document
    def delete_document(self,
               collection_name,
               document,
               rev=None,
               check_rev=True,
               ignore_missing=False,
               return_old=False,
               sync=None,
               silent=False):
        """Delete a document.

        :param document: Document ID, key or body. Document body must contain
            the "_id" or "_key" field.
        :type document: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **document** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **document** (if given)
            is compared against the revision of target document.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised on failures.
        :type ignore_missing: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision), or True if
            parameter **silent** was set to True, or False if document was not
            found and **ignore_missing** was set to True (does not apply in
            transactions).
        :rtype: bool | dict
        :raise c8.exceptions.DocumentDeleteError: If delete fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        _collection = self.get_collection(collection_name)
        resp = _collection.delete( document=document,
                                   rev=rev,
                                   check_rev=check_rev,
                                   ignore_missing=ignore_missing,
                                   return_old=return_old,
                                   sync=sync,
                                   silent=silent)
        return resp


    # client.delete_document_many
    def delete_document_many(self,
                    collection_name,
                    documents,
                    return_old=False,
                    check_rev=True,
                    sync=None,
                    silent=False):
        """Delete multiple documents.

        If deleting a document fails, the exception object is placed in the
        result list instead of document metadata.

        :param documents: Document IDs, keys or bodies. Document bodies must
            contain the "_id" or "_key" fields.
        :type documents: [str | unicode | dict]
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param check_rev: If set to True, revisions of **documents** (if given)
            are compared against the revisions of target documents.
        :type check_rev: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: List of document metadata (e.g. document keys, revisions) and
            any exceptions, or True if parameter **silent** was set to True.
        :rtype: [dict | C8Error] | bool
        :raise c8.exceptions.DocumentDeleteError: If delete fails.
        """
        _collection = self.get_collection(collection_name)
        resp = _collection.delete( documents=documents,
                                   check_rev=check_rev,
                                   return_old=return_old,
                                   sync=sync,
                                   silent=silent)
        return resp

    
    # client.get_collection_indexes
    def get_collection_indexes(self, collection_name):
        """Return the collection indexes.

        :return: Collection indexes.
        :rtype: [dict]
        :raise c8.exceptions.IndexListError: If retrieval fails.
        """
        _collection = self._fabric.collection(collection_name)
        resp = _collection.indexes() 
        return resp
        
    # client.get_dc_list
    def get_dc_list(self, detail=False):
        """Return the list of names of Datacenters

        :param detail: detail list of DCs if set to true else only DC names
        :type: boolean
        :return: DC List.
        :rtype: [str | unicode ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        resp = self._fabric.dclist(detail=detail)
        return resp

    
    # client.get_local_dc
    def get_local_dc(self, detail=True):
        """Return the list of local Datacenters

        :param detail: detail list of DCs if set to true else only DC names
        :type: boolean
        :return: DC List.
        :rtype: [str | dict ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        resp = self._fabric.localdc(detail=detail)
        return resp


    # client.validate_query
    def validate_query(self, query):
        """Parse and validate the query without executing it.

        :param query: Query to validate.
        :type query: str | unicode
        :return: Query details.
        :rtype: dict
        :raise c8.exceptions.C8QLQueryValidateError: If validation fails.
        """
        resp = self._fabric.c8ql.validate(query)
        return resp

    
    # client.explain_query
    def explain_query(self,query, all_plans=False, max_plans=None, opt_rules=None):
        """Inspect the query and return its metadata without executing it.

        :param query: Query to inspect.
        :type query: str | unicode
        :param all_plans: If set to True, all possible execution plans are
            returned in the result. If set to False, only the optimal plan
            is returned.
        :type all_plans: bool
        :param max_plans: Total number of plans generated by the optimizer.
        :type max_plans: int
        :param opt_rules: List of optimizer rules.
        :type opt_rules: list
        :return: Execution plan, or plans if **all_plans** was set to True.
        :rtype: dict | list
        :raise c8.exceptions.C8QLQueryExplainError: If explain fails.
        """
        resp = self._fabric.c8ql.explain(query, all_plans=all_plans, max_plans=max_plans, opt_rules=opt_rules)
        return resp

    # client.execute_query
    def execute_query(self,
                query,
                count=False,
                bind_vars=None,
                cache=None,
                profile=None):
        """Execute the query and return the result cursor.

        :param query: Query to execute.
        :type query: str | unicode
        :param count: If set to True, the total document count is included in
            the result cursor.
        :type count: bool
        :param bind_vars: Bind variables for the query.
        :type bind_vars: dict
        :param cache: If set to True, the query cache is used. The operation
            mode of the query cache must be set to "on" or "demand".
        :type cache: bool
        :param profile: Return additional profiling details in the cursor,
            unless the query cache is used.
        :type profile: bool
        :return: Result cursor.
        :rtype: c8.cursor.Cursor
        :raise c8.exceptions.C8QLQueryExecuteError: If execute fails.
        """
        resp = self._fabric.c8ql.execute(query,
                                    count=count,
                                    bind_vars=bind_vars,
                                    cache=cache,
                                    profile=profile)
        return resp

    
    # client.get_running_queries
    def get_running_queries(self):
        """Return the currently running C8QL queries.

        :return: Running C8QL queries.
        :rtype: [dict]
        :raise c8.exceptions.C8QLQueryListError: If retrieval fails.
        """
        return self._fabric.c8ql.queries()

    
    # client.kill_query
    def kill_query(self, query_id):
        """Kill a running query.

        :param query_id: Query ID.
        :type query_id: str | unicode
        :return: True if kill request was sent successfully.
        :rtype: bool
        :raise c8.exceptions.C8QLQueryKillError: If the send fails.
        """
        return self._fabric.c8ql.kill(query_id)

    
    # client.create_restql
    def create_restql(self, data):
        """Save restql by name.

        :param data: data to be used for restql POST API
        :type data: dict
        :return: Results of restql API
        :rtype: dict
        :raise c8.exceptions.RestqlCreateError: if restql operation failed
        """
        return self._fabric.save_restql(data)

    
    # client.execute_restql
    def execute_restql(self, name, data=None):
        """Execute restql by name.

        :param name: restql name
        :type name: str | unicode
        :param data: restql data (optional)
        :type data: dict
        :return: Results of execute restql
        :rtype: dict
        :raise c8.exceptions.RestqlExecuteError: if restql execution failed
        """
        return self._fabric.execute_restql(name, data=data)

    
    # client.delete_restql
    def delete_restql(self, name):
        """Delete restql by name.

        :param name: restql name
        :type name: str | unicode
        :return: True if restql is deleted
        :rtype: bool
        :raise c8.exceptions.RestqlDeleteError: if restql deletion failed
        """
        return self._fabric.delete_restql(name)

    
    # client.update_restql
    def update_restql(self, name, data):
        """Update restql by name.

        :param name: name of restql
        :type name: str | unicode
        :param data: restql data
        :type data: dict
        :return: True if restql is updated
        :rtype: bool
        :raise c8.exceptions.RestqlUpdateError: if query update failed
        """
        return self._fabric.update_restql(name, data)


    # client.get_restqls
    def get_restqls(self):
        """Get all restql associated for user.

        :return: Details of all restql
        :rtype: list
        :raise c8.exceptions.RestqlListError: if getting restql failed
        """
        return self._fabric.get_all_restql()

    
    # client.create_stream
    def create_stream(self, stream, local=False):
        """
        Create the stream under the given fabric
        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If creating streams fails.
        """
        return self._fabric.create_stream(stream, local=local)
    

    # client.terminate_stream
    def terminate_stream(self, stream, isCollectionStream=False, local=False):
        """
        Delete the streams under the given fabric
        :param stream: name of stream
        :param force:
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If deleting streams fails.
        """
        return self._fabric.terminate_stream(stream=stream, isCollectionStream=isCollectionStream,
                                              local=local)
                                        

    # client.has_stream
    def has_stream(self, stream, isCollectionStream=False, local=False):
        """ Check if the list of streams has a stream with the given name.

        :param stream: The name of the stream for which to check in the list
                       of all streams.
        :type stream: str | unicode
        :return: True=stream found; False=stream not found.
        :rtype: bool
        """
        return self._fabric.has_stream(stream=stream, isCollectionStream=isCollectionStream,
                                       local=local)

    
    # client.get_stream
    def get_stream(self, operation_timeout_seconds=30):
        """Return the stream collection API wrapper.

        :return: stream collection API wrapper.
        :rtype: c8.stream_collection.StreamCollection
        """
        return self._fabric.stream(operation_timeout_seconds=operation_timeout_seconds)


    # client.get_streams
    def get_streams(self):
        """Get list of all streams under given fabric

        :return: List of streams under given fabric.
        :rtype: json
        :raise c8.exceptions.StreamListError: If retrieving streams fails.
        """
        return self._fabric.streams()

    
    #client.get_stream_stats
    def get_stream_stats(self, stream, isCollectionStream=False, local=False):
        """Get the stats for the given stream

        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: If getting subscriptions
                                                     for a stream fails.
        """
        _stream = self._fabric.stream()
        return _stream.get_stream_stats(stream, 
                            isCollectionStream=isCollectionStream, local=local)


    # client.create_stream_producer
    def enum(**enums):
        return type('Enum', (), enums)

    COMPRESSION_TYPES = enum(LZ4="LZ4",
                             ZLIB="ZLib",
                             NONE=None)

    ROUTING_MODE = enum(
        SINGLE_PARTITION="SinglePartition",
        ROUND_ROBIN_PARTITION="RoundRobinPartition",  # noqa
        CUSTOM_PARTITION="CustomPartition"
    )

    def create_stream_producer(self, stream, isCollectionStream=False, local=False, producer_name=None,
                        initial_sequence_id=None, send_timeout_millis=30000,
                        compression_type=COMPRESSION_TYPES.NONE,
                        max_pending_messages=1000,
                        batching_enabled=False,
                        batching_max_messages=1000,
                        batching_max_allowed_size_in_bytes=131072,
                        batching_max_publish_delay_ms=10,
                        message_routing_mode=ROUTING_MODE.ROUND_ROBIN_PARTITION
                        ):
        """Create a new producer on a given stream.
        **Args**
        * `stream`: The stream name
        **Options**
        * `persistent`: If the stream_stream is persistent or non-persistent
                        default its persitent
        * `local`: If the stream_stream is local or global default its global
        * `producer_name`: Specify a name for the producer. If not assigned,
                           the system will generate a globally unique name
                           which can be accessed with
                           `Producer.producer_name()`. When specifying a name,
                           it is app to the user to ensure that, for a given
                           topic, the producer name is unique across all
                           Pulsar's clusters.
        * `initial_sequence_id`: Set the baseline for the sequence ids for
                                 messages published by the producer. First
                                 message will be using
                                 `(initialSequenceId + 1)`` as its sequence id
                                 and subsequent messages will be assigned
                                 incremental sequence ids, if not otherwise
                                 specified.
        * `send_timeout_seconds`: If a message is not acknowledged by the
                                  server before the `send_timeout` expires, an
                                  error will be reported.
        * `compression_type`: Set the compression type for the producer. By
                              default, message payloads are not compressed.
                              Supported compression types are
                              `CompressionType.LZ4` and `CompressionType.ZLib`.
        * `max_pending_messages`: Set the max size of the queue holding the
                                  messages pending to receive
                                  an acknowledgment from the broker.
        * `block_if_queue_full`: Set whether `send_async` operations should
                                 block when the outgoing message queue is full.
        * `message_routing_mode`: Set the message routing mode for the
                                  partitioned producer. Default is
                                  `PartitionsRoutingMode.RoundRobinDistribution`,  # noqa
                                  other option is
                                  `PartitionsRoutingMode.UseSinglePartition`
        """
        _stream = self._fabric.stream()
        return _stream.create_producer(stream, isCollectionStream=isCollectionStream,
                        local=local, producer_name=producer_name,
                        initial_sequence_id=initial_sequence_id,
                        send_timeout_millis=send_timeout_millis,
                        compression_type=compression_type,
                        max_pending_messages=max_pending_messages,
                        batching_enabled=batching_enabled,
                        batching_max_messages=batching_max_messages,
                        batching_max_allowed_size_in_bytes=batching_max_allowed_size_in_bytes,
                        batching_max_publish_delay_ms=batching_max_publish_delay_ms,
                        message_routing_mode=message_routing_mode)

    # client.subscribe
    CONSUMER_TYPES = enum(EXCLUSIVE="Exclusive",
                          SHARED="Shared",
                          FAILOVER="Failover")

    def subscribe(self, stream, isCollectionStream=False, local=False,
                  subscription_name=None,
                  consumer_type=CONSUMER_TYPES.EXCLUSIVE,
                  message_listener=None,
                  receiver_queue_size=1000,
                  consumer_name=None,
                  unacked_messages_timeout_ms=None,
                  broker_consumer_stats_cache_time_ms=30000,
                  is_read_compacted=False,
                  ):
        """
        Subscribe to the given topic and subscription combination.
        **Args**
        * `stream`: The name of the stream.
        * `subscription`: The name of the subscription.
        **Options**
        * `local`: If the stream_stream is local or global default its global
        * `consumer_type`: Select the subscription type to be used when
                           subscribing to the topic.
        * `message_listener`: Sets a message listener for the consumer. When
                              the listener is set, the application will receive
                              messages through it. Calls to
                              `consumer.receive()` will not be allowed. The
                              listener function needs to accept
                              (consumer, message)
        * `receiver_queue_size`:
            Sets the size of the consumer receive queue. The consumer receive
            queue controls how many messages can be accumulated by the consumer
            before the application calls `receive()`. Using a higher value
            could potentially increase the consumer throughput at the expense
            of higher memory utilization. Setting the consumer queue size to
            zero decreases the throughput of the consumer by disabling
            pre-fetching of messages. This approach improves the message
            distribution on shared subscription by pushing messages only to
            those consumers that are ready to process them. Neither receive
            with timeout nor partitioned topics can be used if the consumer
            queue size is zero. The `receive()` function call should not be
            interrupted when the consumer queue size is zero. The default value
            is 1000 messages and should work well for most use cases.
        * `consumer_name`: Sets the consumer name.
        * `unacked_messages_timeout_ms`:
            Sets the timeout in milliseconds for unacknowledged messages.
            The timeout needs to be greater than 10 seconds. An exception is
            thrown if the given value is less than 10 seconds. If a successful
            acknowledgement is not sent within the timeout, all the
            unacknowledged messages are redelivered.
        * `broker_consumer_stats_cache_time_ms`:
            Sets the time duration for which the broker-side consumer stats
            will be cached in the client.
        """
        _stream = self._fabric.stream()
        return _stream.subscribe(stream=stream, 
                  local=local, isCollectionStream=isCollectionStream,
                  subscription_name=subscription_name,
                  consumer_type=consumer_type,
                  message_listener=message_listener,
                  receiver_queue_size=receiver_queue_size,
                  consumer_name=consumer_name,
                  unacked_messages_timeout_ms=unacked_messages_timeout_ms,
                  broker_consumer_stats_cache_time_ms=broker_consumer_stats_cache_time_ms,
                  is_read_compacted=is_read_compacted)


    # client.create_stream_reader
    def create_stream_reader(self, stream, start_message_id,
                      local=False, isCollectionStream=False,
                      reader_listener=None,
                      receiver_queue_size=1000,
                      reader_name=None,
                      subscription_role_prefix=None,
                      ):
        """
        Create a reader on a particular topic
        **Args**
        * `stream`: The name of the stream.
        * `start_message_id`: The initial reader positioning is done by
                              specifying a message id.
        **Options**
        * `local`: If the stream_stream is local or global default its global
        * `reader_listener`:
            Sets a message listener for the reader. When the listener is set,
            the application will receive messages through it. Calls to
            `reader.read_next()` will not be allowed. The listener function
            needs to accept (reader, message), for example:
        * `receiver_queue_size`:
            Sets the size of the reader receive queue. The reader receive
            queue controls how many messages can be accumulated by the reader
            before the application calls `read_next()`. Using a higher value
            could potentially increase the reader throughput at the expense of
            higher memory utilization.
        * `reader_name`: Sets the reader name.
        * `subscription_role_prefix`: Sets the subscription role prefix.
        """
        _stream = self._fabric.stream()
        return _stream.create_reader(stream=stream, start_message_id=start_message_id,
                      local=local, isCollectionStream=isCollectionStream,
                      reader_listener=reader_listener,
                      receiver_queue_size=receiver_queue_size,
                      reader_name=reader_name,
                      subscription_role_prefix=subscription_role_prefix)


    # client.unsubscribe
    def unsubscribe(self,subscription):
        """Unsubscribes the given subscription on all streams on a stream fabric
        :param subscription
        :return: 200, OK if operation successful
        raise c8.exceptions.StreamPermissionError: If unsubscribing fails.
        """
        _stream = self._fabric.stream()
        return _stream.unsubscribe(subscription=subscription)


    # client.delete_stream_subscription
    def delete_stream_subscription(self, stream, subscription, local=False):
        """Delete a subscription.

        :param stream: name of stream
        :param subscription: name of subscription
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If Subscription has active
                                                 consumers
        """
        _stream = self._fabric.stream()
        return  _stream.delete_stream_subscription(stream, subscription, local=False)


    # client.get_stream_subscriptions
    def get_stream_subscriptions(self, stream, local=False):
        """Get the list of persistent subscriptions for a given stream.

        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: List of stream subscription, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: If getting subscriptions
                                                     for a stream fails.
        """
        _stream = self._fabric.stream()
        return _stream.get_stream_subscriptions(stream=stream, local=local)


    # client.get_stream_backlog
    def get_stream_backlog(self, stream, local=False):
        """Get estimated backlog for offline stream.

        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: If getting subscriptions
                                                     for a stream fails.
        """
        _stream = self._fabric.stream()
        return _stream.get_stream_backlog(stream=stream, local=local)



    # client. clear_stream_backlog
    def  clear_stream_backlog(self, subscription):
        """Clear backlog for the given stream on a stream fabric
        :param: name of subscription
        :return: 200, OK if operation successful
        :raise c8.exceptions.StreamPermissionError: If clearing backlogs for
                                                    all streams fails.
        """
        _stream = self._fabric.stream()
        return _stream.clear_stream_backlog(subscription=subscription)


    # client.clear_streams_backlog
    def clear_streams_backlog(self):
        """Clear backlog for all streams on a stream fabric
        :return: 200, OK if operation successful
        :raise c8.exceptions.StreamPermissionError: If clearing backlogs for
                                                    all streams fails.
        """
        _stream = self._fabric.stream()
        return _stream.clear_streams_backlog()


    # client.create_stream_app
    def create_stream_app(self, data, dclist=[]):
        """Creates a stream application by given data
        @data: stream app definition
        @dclist: regions where stream app has to be deployed
        """
        return self._fabric.create_stream_app(data=data, dclist=dclist)

    
    # client.delete_stream_app
    def delete_stream_app(self, streamapp_name):
        """deletes the stream app by name
        :param: name of stream app
        :return: True, OK if operation successful
        """
        _streamapp = self._fabric.stream_app(streamapp_name)
        return _streamapp.delete()

    
    # client.validate_stream_app
    def validate_stream_app(self, data):
        """validates the stream app definition
        :param: definition of stream app
        :return: True, OK if app definition is valid.
        """
        return self._fabric.validate_stream_app(data=data)


    # client.retrieve_stream_app
    def retrieve_stream_app(self):
        """retrives stream apps in a fabric
        :param: name of stream app
        :return: Object with list of stream Apps
        """
        return self._fabric.retrive_stream_app()

    
    # client.get_stream_app
    def get_stream_app(self, streamapp_name):
        """returns info of a stream app 
        :param: name of stream app
        :return: Information of a particular stream app
        """
        _streamapp = self._fabric.stream_app(streamapp_name)
        return _streamapp.get()


    # client.get_stream_app_samples
    def get_stream_app_samples(self):
        """gets samples for stream apps
        """
        return self._fabric.get_samples_stream_app()

    
    # client.activate_stream_app
    def activate_stream_app(self, streamapp_name, activate=True):
        """activates r deactivates a stream app
        :param streamapp_name: name of stream app
        :param activate:
        :return: Object with list of properties
        """
        _streamapp = self._fabric.stream_app(streamapp_name)
        return _streamapp.change_state(active=activate)


    # client.has_graph
    def has_graph(self, graph_name):
        """Check if a graph exists in the fabric.

        :param graph_name: Graph name.
        :type name: str | unicode
        :return: True if graph exists, False otherwise.
        :rtype: bool
        """
        return self._fabric.has_graph(name=graph_name)


    # client.get_graphs
    def get_graphs(self):
        """List all graphs in the fabric.

        :return: Graphs in the fabric.
        :rtype: [dict]
        :raise c8.exceptions.GraphListError: If retrieval fails.
        """

        return self._fabric.graphs()

    # client.create_graph
    def create_graph(self, graph_name,
                     edge_definitions=None,
                     orphan_collections=None,
                     shard_count=None):
        """Create a new graph.

        :param name: Graph name.
        :type name: str | unicode
        :param edge_definitions: List of edge definitions, where each edge
            definition entry is a dictionary with fields "edge_collection",
            "from_vertex_collections" and "to_vertex_collections" (see below
            for example).
        :type edge_definitions: [dict]
        :param orphan_collections: Names of additional vertex collections that
            are not in edge definitions.
        :type orphan_collections: [str | unicode]
        :param shard_count: Number of shards used for every collection in the
            graph. To use this, parameter **smart** must be set to True and
            every vertex in the graph must have the smart field. This number
            cannot be modified later once set. Applies only to enterprise
            version of C8Db.
        :type shard_count: int
        :return: Graph API wrapper.
        :rtype: c8.graph.Graph
        :raise c8.exceptions.GraphCreateError: If create fails.

        Here is an example entry for parameter **edge_definitions**:

        .. code-block:: python

            {
                'edge_collection': 'teach',
                'from_vertex_collections': ['teachers'],
                'to_vertex_collections': ['lectures']
            }
        """

        return self._fabric.create_graph(name=graph_name,
                     edge_definitions=edge_definitions,
                     orphan_collections=orphan_collections,
                     shard_count=shard_count)


    # client.delete_graph
    def delete_graph(self, graph_name, ignore_missing=False, drop_collections=None):
        """Drop the graph of the given name from the fabric.

        :param graph_name: Graph name.
        :type graph_name: str | unicode
        :param ignore_missing: Do not raise an exception on missing graph.
        :type ignore_missing: bool
        :param drop_collections: Drop the collections of the graph also. This
            is only if they are not in use by other graphs.
        :type drop_collections: bool
        :return: True if graph was deleted successfully, False if graph was not
            found and **ignore_missing** was set to True.
        :rtype: bool
        :raise c8.exceptions.GraphDeleteError: If delete fails.
        """
        return self._fabric.delete_graph(name=graph_name, 
                                    ignore_missing=ignore_missing,
                                    drop_collections=drop_collections)
    
    # client.get_graph
    def get_graph(self, graph_name):
        """Return the graph API wrapper.

        :param graph_name: Graph name.
        :type garph_name: str | unicode
        :return: Graph API wrapper.
        :rtype: c8.graph.Graph
        """
        return self._fabric.graph(graph_name)

    
    # client.insert_edge
    def insert_edge(self, graph_name,
                    edge_collection,
                    from_vertex_collections,
                    to_vertex_collections):
        """Create a new edge definition.

        An edge definition consists of an edge collection, "from" vertex
        collection(s) and "to" vertex collection(s). Here is an example entry:

        .. code-block:: python

            {
                'edge_collection': 'edge_collection_name',
                'from_vertex_collections': ['from_vertex_collection_name'],
                'to_vertex_collections': ['to_vertex_collection_name']
            }
        :param graph_name: Name of the Graph for which you want to create edge.
        :type graph_name: str | unicode
        :param edge_collection: Edge collection name.
        :type edge_collection: str | unicode
        :param from_vertex_collections: Names of "from" vertex collections.
        :type from_vertex_collections: [str | unicode]
        :param to_vertex_collections: Names of "to" vertex collections.
        :type to_vertex_collections: [str | unicode]
        :return: Edge collection API wrapper.
        :rtype: c8.collection.EdgeCollection
        :raise c8.exceptions.EdgeDefinitionCreateError: If create fails.
        """
        _graph = self._fabric.graph(graph_name)
        return _graph.create_edge_definition(edge_collection=edge_collection,
                    from_vertex_collections=from_vertex_collections,
                    to_vertex_collections=to_vertex_collections)

        
    # client.replace_edge
    def replace_edge(self, graph_name,
                    edge_collection,
                    from_vertex_collections,
                    to_vertex_collections):
        """Replaces an edge definition.

        :param graph_name: Name of the Graph for which you want to create edge.
        :type graph_name: str | unicode
        :param edge_collection: Edge collection name.
        :type edge_collection: str | unicode
        :param from_vertex_collections: Names of "from" vertex collections.
        :type from_vertex_collections: [str | unicode]
        :param to_vertex_collections: Names of "to" vertex collections.
        :type to_vertex_collections: [str | unicode]
        :return: Edge collection API wrapper.
        :rtype: c8.collection.EdgeCollection
        :raise c8.exceptions.EdgeDefinitionCreateError: If create fails.
        """
        _graph = self._fabric.graph(graph_name)
        return _graph.replace_edge_definition(edge_collection=edge_collection,
                    from_vertex_collections=from_vertex_collections,
                    to_vertex_collections=to_vertex_collections)

    # client.update_edge
    def update_edge(self,
                    graph_name,edge,
                    check_rev=True,
                    keep_none=True,
                    sync=None,
                    silent=False):
        """Update an edge document.

        :param graph_name: Name of the Graph for which you want to create edge.
        :type graph_name: str | unicode
        :param edge: Partial or full edge document with updated values. It must
            contain the "_id" field.
        :type edge: dict
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. If set to False, they are removed completely.
        :type keep_none: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise c8.exceptions.DocumentUpdateError: If update fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        _graph = self._fabric.graph(graph_name)
        return _graph.update_edge(edge=edge,
                    check_rev=check_rev,
                    keep_none=keep_none,
                    sync=sync,
                    silent=silent)


    # client.delete_edge
    def delete_edge(self,graph_name, edge_name, purge=False):
        """Delete an edge definition from the graph.
        :param graph_name: Name of the Graph for which you want to delete edge.
        :type graph_name: str | unicode
        :param name: Edge collection name.
        :type name: str | unicode
        :param purge: If set to True, the edge definition is not just removed
            from the graph but the edge collection is also deleted completely
            from the fabric.
        :type purge: bool
        :return: True if edge definition was deleted successfully.
        :rtype: bool
        :raise c8.exceptions.EdgeDefinitionDeleteError: If delete fails.
        """
        _graph = self._fabric.graph(graph_name)
        return _graph.delete_edge_definition(name=edge_name, purge=purge)


    # client.get_edges
    def get_edges(self, graph_name):
        """Return the edge definitions of the graph.
        :param graph_name: Name of the Graph for which you want to get the edge.
        :type graph_name: str | unicode
        :return: Edge definitions of the graph.
        :rtype: [dict]
        :raise c8.exceptions.EdgeDefinitionListError: If retrieval fails.
        """
        _graph = self._fabric.graph(graph_name)
        return _graph.edge_definitions()

    
    # client.link_edge
    def link_edge(self,
             graph_name,
             collection,
             from_vertex,
             to_vertex,
             data=None,
             sync=None,
             silent=False):
        """Insert a new edge document linking the given vertices.
    
        :param graph_name: Name of the Graph.
        :type graph_name: str | unicode
        :param collection: Edge collection name.
        :type collection: str | unicode
        :param from_vertex: "From" vertex document ID or body with "_id" field.
        :type from_vertex: str | unicode | dict
        :param to_vertex: "To" vertex document ID or body with "_id" field.
        :type to_vertex: str | unicode | dict
        :param data: Any extra data for the new edge document. If it has "_key"
            or "_id" field, its value is used as key of the new edge document
            (otherwise it is auto-generated).
        :type data: dict
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise c8.exceptions.DocumentInsertError: If insert fails.
        """
        _graph = self._fabric.graph(graph_name)
        return _graph.link(collection=collection,
                        from_vertex=from_vertex,
                        to_vertex=to_vertex,
                        data=data,
                        sync=sync,
                        silent=silent)

    
    # client.has_user
    def has_user(self, username):
        """Check if user exists.

        :param username: Username.
        :type username: str | unicode
        :return: True if user exists, False otherwise.
        :rtype: bool
        """
        return self._tenant.has_user(username)


    #client.get_users
    def get_users(self):
        """Return all user details.

        :return: List of user details.
        :rtype: [dict]
        :raise c8.exceptions.UserListError: If retrieval fails.
        """
        return self._tenant.users()

    
    # client.get_user
    def get_user(self, username):
        """Return user details.

        :param username: Username.
        :type username: str | unicode
        :return: User details.
        :rtype: dict
        :raise c8.exceptions.UserGetError: If retrieval fails.
        """
        return self._tenant.user(username)

    
    # client.create_user
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
        return self._tenant.create_user(username=username,
                                        email=email, password=password,
                                        active=active, extra=extra)

    
    # client.update_user
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
        return self._tenant.update_user(username=username, password=password,
                                        active=active, extra=extra)



    # client.replace_user
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
        return self._tenant.replace_user(username=username, password=password,
                                         active=active, extra=extra)

    
    # client.delete_user
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
        return self._tenant.delete_user(username=username,
                                        ignore_missing=ignore_missing)


    # client.get_permissions
    def get_permissions(self, username):
        """Return user permissions for all fabrics and collections.

        :param username: Username.
        :type username: str | unicode
        :return: User permissions for all fabrics and collections.
        :rtype: dict
        :raise: c8.exceptions.PermissionListError: If retrieval fails.
        """
        return self._tenant.permissions(username)

    
    # client.get_permission
    def get_permission(self, username, fabric, collection=None):
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
        return self._tenant.permission(username=username, fabric=fabric,
                                       collection=collection)

    
    # client.update_permission
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
        return self._tenant.update_permission(username=username,
                                              permission=permission,
                                              fabric=fabric,
                                              collection=collection)

    
    # client.reset_permission()
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
        return self._tenant.reset_permission(username=username, fabric=fabric,
                                             collection=collection)


    # client.kv_get_collections
    def get_collections_kv(self):
        """Returns the list of collections using kv.
        :return: Existing Collections.
        :rtype: list
        :raise c8.exceptions.ListCollections: If retrieval fails.
        """
        return self._fabric.key_value.get_collections()


    # client.create_collection_kv
    def create_collection_kv(self, name, expiration=False):
        """Creates Collection.

        :param name: Collection name.
        :type name: str | unicode
        :param expiration:if True then the namesapce supports TTL.
        :type expiration: boolean
        :return: True if the request is successful.
        :rtype: boolean
        :raise c8.exceptions.CreateCollectionError: If creation fails.
        """
        return self._fabric.key_value.create_collection(name=name, expiration=expiration)


    # client.delete_collection_kv
    def delete_collection_kv(self, name):
            """Deletes Collection.

            :param name: Collection name.
            :type name: str | unicode
            :return: True if the request is successful.
            :rtype: boolean
            :raise c8.exceptions.DeleteCollectionError: If creation fails.
            """
            return self._fabric.key_value.delete_collection(name=name)

    
    # client.has_collection_kv
    def has_collection_kv(self, name):
        """Checks if a Collection exists.

        :param name: Collection name.
        :type name: str | unicode
        :return: True if the collection exists.
        :rtype: boolean
        """
        return self._fabric.key_value.has_collection(name)

    
    # client.insert_key_value_pair
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
        return self._fabric.key_value.insert_key_value_pair(name=name, data=data)


    # client.delete_entry_for_key
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
        return self._fabric.key_value.delete_entry_for_key(name=name, key=key)


    # client.delete_entry_for_keys
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
        return self._fabric.key_value.delete_entry_for_keys(name=name, keys=keys)

    
    # client.get_value_for_key
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
        return self._fabric.key_value.get_value_for_key(name=name, key=key)

    
    # client.get_keys
    def get_keys(self, name):
        """gets keys of a collection.

        :param name: Collection name.
        :type name: str | unicode
        :return: List of Keys.
        :rtype: list
        :raise c8.exceptions.GetKeysError: If request fails.
        """
        return self._fabric.key_value.get_keys(name)


    # client.get_kv_count
    def get_kv_count(self, name):
        """gets the kv count of a collection.

        :param name: Collection name.
        :type name: str | unicode
        :return:Number of kv entries.
        :rtype: int
        :raise c8.exceptions.GetCountError: If request fails.
        """
        return self._fabric.key_value.get_kv_count(name)

    
    # client.create_api_key
    def create_api_key(self, keyid):
        """Creates an api key.
        
        :return: Creates an api key.
        :rtype: list
        :raise c8.exceptions.CreateAPIKey: If request fails.
        """
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.create_api_key()


    # client.list_all_api_keys
    def list_all_api_keys(self):
        return self._fabric.list_all_api_keys()


    # client.remove_api_key
    def remove_api_key(self, keyid):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.remove_api_key()


    # client.list_accessible_databases
    def list_accessible_databases(self, keyid):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.list_accessible_databases()


    # client.get_database_access_level
    def get_database_access_level(self, keyid, databasename):
        """Fetch the database access level for a specific database.


        :param databasename: Name of the database
        :type databasename: string
        :return: AccessLevel of a db.
        :rtype: string
        :raise c8.exceptions.DataBaseAccessLevel: If request fails.
        """
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.get_database_access_level(databasename)


    def set_database_access_level(self, keyid, databasename, grant='ro'):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.set_database_access_level(databasename, grant=grant)

    
    def clear_database_access_level(self, keyid, databasename):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.clear_database_access_level(databasename)


    def get_collection_access_level(self, keyid, collection_name, databasename='_system'):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.get_collection_access_level(collection_name, databasename)


    def set_collection_access_level(self, keyid, collection_name, databasename='_system',
                                     grant='ro'):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.set_collection_access_level(collection_name, databasename, grant)


    def clear_collection_access_level(self, keyid, collection_name, databasename='_system'):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.clear_collection_access_level(collection_name, databasename)


    def list_accessible_streams(self, keyid, databasename='_system', full=False):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.list_accessible_streams(databasename, full)


    def get_stream_access_level(self, keyid, streamname, databasename='_system', local=False):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.get_stream_access_level(streamname, databasename, local)


    def set_stream_access_level(self, keyid, streamname, databasename='_system', grant='ro', local=False):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.set_stream_access_level(streamname, databasename, grant, local=local)



    def clear_stream_access_level(self, keyid, streamname, databasename='_system', local=False):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.clear_stream_access_level(streamname, databasename, local)


    def get_billing_access_level(self, keyid):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.get_billing_access_level()


    def set_billing_access_level(self, keyid, grant='ro'):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.set_billing_access_level(grant)


    def clear_billing_access_level(self, keyid):
        _apiKeys = self._fabric.api_keys(keyid)
        return _apiKeys.clear_billing_access_level()

    # def fabric(self, tenant, name, email, password, verify=False):
    #     """Connect to a fabric and return the fabric API wrapper.

    #     :param name: Fabric name.
    #     :type name: str | unicode
    #     :param username: Username for basic authentication.
    #     :type username: str | unicode
    #     :param password: Password for basic authentication.
    #     :type password: str | unicode
    #     :param verify: Verify the connection by sending a test request.
    #     :type verify: bool
    #     :return: Standard fabric API wrapper.
    #     :rtype: c8.fabric.StandardFabric
    #     :raise c8.exceptions.ServerConnectionError: If **verify** was set
    #         to True and the connection to C8Db fails.
    #     """
    #     connection = FabricConnection(
    #         url=self._url, stream_port=self._stream_port, tenant=tenant,
    #         fabric=name, email=email, password=password,
    #         http_client=self._http_client
    #     )
    #     fabric = StandardFabric(connection)

    #     if verify:  # Check the server connection by making a read API call
    #         try:
    #             fabric.ping()
    #         except ServerConnectionError as err:
    #             raise err
    #         except Exception as err:
    #             raise ServerConnectionError('bad connection: {}'.format(err))

    #     return fabric
