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

    def __init__(self, protocol='http', host='127.0.0.1', port=80,
                 stream_port=constants.STREAM_PORT, http_client=None,
                 email=None, password=None, fabric="_system"):
        self._protocol = protocol.strip('/')
        self._host = host.strip('/')
        self._port = int(port)
        if self._protocol == 'https':
            self._port = 443
        self._stream_port = int(stream_port)
        if "api-" in self.host:
            self._url = '{}://{}:{}'.format(self._protocol, self.host, self.port)
        else:
            self._url = '{}://api-{}:{}'.format(self._protocol, self.host, self.port)
        self._http_client = http_client

        if email and password:
            self._email = email
            self._password = password
            self._fabricname = fabric
            self._tenant = self.tenant(self._email, self._password)
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

    def tenant(self, email, password, verify=False):
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
                                      http_client=self._http_client)
        tenant = Tenant(connection)

        # TODO : handle verify

        return tenant

    # Reducing steps
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


    # client.insert 
    def insert(self, collection_name="", document=None, return_new=False,
               sync=None, silent=False):
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
        _collection =  self._fabric.collection(collection_name)
        if isinstance(document, dict):
           resp =  _collection.insert(document=document,retun_new=return_new,
                                      sync=sync, silent=silent)
        elif isinstance(document, list):
            resp = _collection.insert_many(documents=document,retun_new=return_new,
                                           sync=sync, silent=silent)
        return resp

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
