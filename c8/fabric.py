from __future__ import absolute_import, unicode_literals

from c8.utils import get_col_name
import json

__all__ = [
    'StandardFabric',
    'AsyncFabric',
    'BatchFabric',
    'TransactionFabric'
]

from datetime import datetime

from c8.api import APIWrapper
from c8.c8ql import C8QL
from c8 import constants
from c8.executor import (
    DefaultExecutor,
    AsyncExecutor,
    BatchExecutor,
    TransactionExecutor,
)
from c8.collection import StandardCollection
from c8.stream_collection import StreamCollection
from c8.exceptions import (
    AsyncJobClearError,
    AsyncJobListError,
    CollectionCreateError,
    CollectionDeleteError,
    CollectionListError,
    FabricDeleteError,
    FabricCreateError,
    FabricListError,
    FabricPropertiesError,
    GraphListError,
    GraphCreateError,
    GraphDeleteError,
    ServerConnectionError,
    ServerDetailsError,
    ServerVersionError,
    TransactionExecuteError,
    TenantDcListError,
)
from c8 import exceptions as ex

from c8.graph import Graph
from c8.request import Request
import json
import random
import pulsar
from urllib.parse import urlparse


def printdata(event):
    """Prints the event.

    :param event: real-time update.
    :type event: str | unicode
    """
    print(event)


class Fabric(APIWrapper):
    """Base class for Fabric API wrappers.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection, executor):
        self.url=connection.url
        self.stream_port=connection.stream_port
        self.pulsar_client=None
        self.persistent = True
        super(Fabric, self).__init__(connection, executor)

    def __getitem__(self, name):
        """Return the collection API wrapper.

        :param name: Collection name.
        :type name: str | unicode
        :return: Collection API wrapper.
        :rtype: c8.collection.StandardCollection
        """
        return self.collection(name)

    def _get_col_by_doc(self, document):
        """Return the collection of the given document.

        :param document: Document ID or body with "_id" field.
        :type document: str | unicode | dict
        :return: Collection API wrapper.
        :rtype: c8.collection.StandardCollection
        :raise c8.exceptions.DocumentParseError: On malformed document.
        """
        return self.collection(get_col_name(document))

    @property
    def name(self):
        """Return fabric name.

        :return: Fabric name.
        :rtype: str | unicode
        """
        return self.fabric_name

    @property
    def c8ql(self):
        """Return C8QL (C8Db Query Language) API wrapper.

        :return: C8QL API wrapper.
        :rtype: c8.c8ql.C8QL
        """
        return C8QL(self._conn, self._executor)

    def on_change(self, collection, callback=printdata):
        """Execute given input function on receiving a change.

        :param callback: Function to execute on a change
        :type callback: function
        :param collections: Collection name or Collection names regex to listen for
        :type collections: str
        """
        if not collection: 
            raise ValueError('You must specify a collection on which to watch for realtime data!')

        namespace = constants.STREAM_LOCAL_NS_PREFIX + self.tenant_name + '.' + self.fabric_name
        if self.tenant_name == "_mm":
            namespace = constants.STREAM_LOCAL_NS_PREFIX + self.fabric_name

        topic = "persistent://" + self.tenant_name + "/" + namespace + "/" + collection
        subscription_name = self.tenant_name + "-" + self.fabric_name + "-subscription-" + str(random.randint(1, 1000))
        print("pyC8 Realtime: Subscribing to topic: "+ topic + " on Subscription name: "+subscription_name)
    
        if self.pulsar_client:
            print("pyC8 Realtime: Initialized C8Streams connection to "+ self.url + ":" + str(self.stream_port))
        else:
            dcl_local = self.dclist_local()
            self.pulsar_client = pulsar.Client('pulsar://' + constants.PLUSAR_URL_PREFIX + dcl_local['tags']['url'] + ":" + str(self.stream_port))
        
        consumer = self.pulsar_client.subscribe(topic, subscription_name)

        try:
            print("pyC8 Realtime: Begin monitoring realtime updates for "+topic)
            while True:
                msg = consumer.receive()
                data = msg.data().decode('utf-8')
                jdata = json.loads(data)
                #self.consumer.acknowledge(msg)
                callback(jdata)
        finally:
            self.pulsar_client.close()

    def properties(self):
        """Return fabric properties.

        :return: Fabric properties.
        :rtype: dict
        :raise c8.exceptions.FabricPropertiesError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/database/current',
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FabricPropertiesError(resp, request)
            result = resp.body['result']
            result['system'] = result.pop('isSystem')
            return result

        return self._execute(request, response_handler)

    def execute_transaction(self,
                            command,
                            params=None,
                            read=None,
                            write=None,
                            sync=None,
                            timeout=None,
                            max_size=None,
                            allow_implicit=None,
                            intermediate_commit_count=None,
                            intermediate_commit_size=None):
        """Execute raw Javascript command in transaction.

        :param command: Javascript command to execute.
        :type command: str | unicode
        :param read: Names of collections read during transaction. If parameter
            **allow_implicit** is set to True, any undeclared read collections
            are loaded lazily.
        :type read: [str | unicode]
        :param write: Names of collections written to during transaction.
            Transaction fails on undeclared write collections.
        :type write: [str | unicode]
        :param params: Optional parameters passed into the Javascript command.
        :type params: dict
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :param timeout: Timeout for waiting on collection locks. If set to 0,
            C8Db server waits indefinitely. If not set, system default
            value is used.
        :type timeout: int
        :param max_size: Max transaction size limit in bytes. Applies only
            to RocksDB storage engine.
        :type max_size: int
        :param allow_implicit: If set to True, undeclared read collections are
            loaded lazily. If set to False, transaction fails on any undeclared
            collections.
        :type allow_implicit: bool
        :param intermediate_commit_count: Max number of operations after which
            an intermediate commit is performed automatically. Applies only to
            RocksDB storage engine.
        :type intermediate_commit_count: int
        :param intermediate_commit_size: Max size of operations in bytes after
            which an intermediate commit is performed automatically. Applies
            only to RocksDB storage engine.
        :type intermediate_commit_size: int
        :return: Return value of **command**.
        :rtype: str | unicode
        :raise c8.exceptions.TransactionExecuteError: If execution fails.
        """
        collections = {'allowImplicit': allow_implicit}
        if read is not None:
            collections['read'] = read
        if write is not None:
            collections['write'] = write

        data = {'action': command}
        if collections:
            data['collections'] = collections
        if params is not None:
            data['params'] = params
        if timeout is not None:
            data['lockTimeout'] = timeout
        if sync is not None:
            data['waitForSync'] = sync
        if max_size is not None:
            data['maxTransactionSize'] = max_size
        if intermediate_commit_count is not None:
            data['intermediateCommitCount'] = intermediate_commit_count
        if intermediate_commit_size is not None:
            data['intermediateCommitSize'] = intermediate_commit_size

        request = Request(
            method='post',
            endpoint='/transaction',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TransactionExecuteError(resp, request)
            return resp.body.get('result')

        return self._execute(request, response_handler)

    def fabrics_detail(self):
        request = Request(
            method='get',
            endpoint='/database/user'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FabricListError(resp, request)
            return [{
                'id': col['id'],
                'name': col['name'],
                'system': col['isSystem'],
                'path': col['path'],
                'options': col['options']
        } for col in map(dict, resp.body['result'])]

        return self._execute(request, response_handler)

    def version(self):
        """Return C8Db server version.

        :return: Server version.
        :rtype: str | unicode
        :raise c8.exceptions.ServerVersionError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_admin/version',
            params={'details': False}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerVersionError(resp, request)
            return resp.body['version']

        return self._execute(request, response_handler)

    def ping(self):
        """Ping the C8Db server by sending a test request.

        :return: Response code from server.
        :rtype: int
        :raise c8.exceptions.ServerConnectionError: If ping fails.
        """
        request = Request(
            method='get',
            endpoint='/collection',
        )

        def response_handler(resp):
            code = resp.status_code
            if code in {401, 403}:
                raise ServerConnectionError('bad username and/or password')
            if not resp.is_success:
                raise ServerConnectionError(
                    resp.error_message or 'bad server response')
            return code

        return self._execute(request, response_handler)

    #########################
    # Datacenter Management #
    #########################

    def dclist(self):
        """Return the list of names of Datacenters

        :return: DC List.
        :rtype: [str | unicode ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/datacenter/all'
        )

        def response_handler(resp):
            #print("dclist() : Response body: " + str(resp.body))
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            dc_list = []
            for dc in resp.body:
                dc_list.append(dc['name'])
            return dc_list

        return self._execute(request, response_handler)

    def dclist_detail(self):
        """Return the list of details of Datacenters

        :return: DC List.
        :rtype: [str | unicode ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/datacenter/all'
        )

        def response_handler(resp):
            #print("dclist() : Response body: " + str(resp.body))
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            return resp.body

        return self._execute(request, response_handler)

    def dclist_local(self):
        """Return the list of local Datacenters

        :return: DC List.
        :rtype: [str | unicode ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/datacenter/local'
        )

        def response_handler(resp):
            #print("dclist() : Response body: " + str(resp.body))
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            return resp.body

        return self._execute(request, response_handler)


    #######################
    # Fabric Management #
    #######################

    def fabrics(self):
        """Return the names all fabrics.

        :return: Fabric names.
        :rtype: [str | unicode]
        :raise c8.exceptions.FabricListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/database'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FabricListError(resp, request)
            return resp.body['result']

        return self._execute(request, response_handler)

    def has_fabric(self, name):
        """Check if a fabric exists.

        :param name: Fabric name.
        :type name: str | unicode
        :return: True if fabric exists, False otherwise.
        :rtype: bool
        """
        return name in self.fabrics()

    def create_fabric(self, name, users=None, dclist=None, realtime=False):
        """Create a new fabric.

        :param name: Fabric name.
        :type name: str | unicode
        :param users: List of users with access to the new fabric, where each
            user is a dictionary with fields "username", "password", "active"
            and "extra" (see below for example). If not set, only the admin and
            current user are granted access.
        :type users: [dict]
        :param dclist : list of strings of datacenters
        :type dclist: [str | unicode]
        :param realtime: Whether or not the DB is realtime-enabled.
        :type realtime: bool
        :return: True if fabric was created successfully.
        :rtype: bool
        :raise c8.exceptions.FabricCreateError: If create fails.

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
        if users is not None:
            data['users'] = [{
                'username': user['username'],
                'passwd': user['password'],
                'active': user.get('active', True),
                'extra': user.get('extra', {})
            } for user in users]

        options = {}
        options['realTime'] = realtime
        dcl = ''
        if dclist:
            # Process dclist param (type list) to build up comma-separated string of DCs
            for dc in dclist:
                if len(dcl) > 0:
                    dcl += ','
                dcl += dc
        options['dcList'] = dcl

        data['options'] = options

        request = Request(
            method='post',
            endpoint='/database',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FabricCreateError(resp, request)
            return True

        return self._execute(request, response_handler)

    def delete_fabric(self, name, ignore_missing=False):
        """Delete the fabric.

        :param name: Fabric name.
        :type name: str | unicode
        :param ignore_missing: Do not raise an exception on missing fabric.
        :type ignore_missing: bool
        :return: True if fabric was deleted successfully, False if fabric
            was not found and **ignore_missing** was set to True.
        :rtype: bool
        :raise c8.exceptions.FabricDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/database/{}'.format(name)
        )

        def response_handler(resp):
            if resp.error_code == 1228 and ignore_missing:
                return False
            if not resp.is_success:
                raise FabricDeleteError(resp, request)
            return resp.body['result']

        return self._execute(request, response_handler)

    #########################
    # Collection Management #
    #########################

    def collection(self, name):
        """Return the standard collection API wrapper.

        :param name: Collection name.
        :type name: str | unicode
        :return: Standard collection API wrapper.
        :rtype: c8.collection.StandardCollection
        """
        return StandardCollection(self._conn, self._executor, name)

    def has_collection(self, name):
        """Check if collection exists in the fabric.

        :param name: Collection name.
        :type name: str | unicode
        :return: True if collection exists, False otherwise.
        :rtype: bool
        """
        return any(col['name'] == name for col in self.collections())

    def collections(self):
        """Return the collections in the fabric.

        :return: Collections in the fabric and their details.
        :rtype: [dict]
        :raise c8.exceptions.CollectionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/collection'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionListError(resp, request)
            return [{
                'id': col['id'],
                'name': col['name'],
                'system': col['isSystem'],
                'type': StandardCollection.types[col['type']],
                'status': StandardCollection.statuses[col['status']],
            } for col in map(dict, resp.body['result'])]

        return self._execute(request, response_handler)

    def create_collection(self,
                          name,
                          sync=False,
                          compact=True,
                          system=False,
                          journal_size=None,
                          edge=False,
                          volatile=False,
                          user_keys=True,
                          key_increment=None,
                          key_offset=None,
                          key_generator='traditional',
                          shard_fields=None,
                          shard_count=None,
                          index_bucket_count=None,
                          replication_factor=None,
                          shard_like=None,
                          sync_replication=None,
                          enforce_replication_factor=None):
        """Create a new collection.

        :param name: Collection name.
        :type name: str | unicode
        :param sync: If set to True, document operations via the collection
            will block until synchronized to disk by default.
        :type sync: bool
        :param compact: If set to True, the collection is compacted. Applies
            only to MMFiles storage engine.
        :type compact: bool
        :param system: If set to True, a system collection is created. The
            collection name must have leading underscore "_" character.
        :type system: bool
        :param journal_size: Max size of the journal in bytes.
        :type journal_size: int
        :param edge: If set to True, an edge collection is created.
        :type edge: bool
        :param volatile: If set to True, collection data is kept in-memory only
            and not made persistent. Unloading the collection will cause the
            collection data to be discarded. Stopping or re-starting the server
            will also cause full loss of data.
        :type volatile: bool
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
        :param shard_count: Number of shards to create.
        :type shard_count: int
        :param index_bucket_count: Number of buckets into which indexes using
            hash tables are split. The default is 16, and this number has to be
            a power of 2 and less than or equal to 1024. For large collections,
            one should increase this to avoid long pauses when the hash table
            has to be initially built or re-sized, since buckets are re-sized
            individually and can be initially built in parallel. For instance,
            64 may be a sensible value for 100 million documents.
        :type index_bucket_count: int
        :param replication_factor: Number of copies of each shard on different
            servers in a cluster. Allowed values are 1 (only one copy is kept
            and no synchronous replication), and n (n-1 replicas are kept and
            any two copies are replicated across servers synchronously, meaning
            every write to the master is copied to all slaves before operation
            is reported successful).
        :type replication_factor: int
        :param shard_like: Name of prototype collection whose sharding
            specifics are imitated. Prototype collections cannot be dropped
            before imitating collections. Applies to enterprise version of
            C8Db only.
        :type shard_like: str | unicode
        :param sync_replication: If set to True, server reports success only
            when collection is created in all replicas. You can set this to
            False for faster server response, and if full replication is not a
            concern.
        :type sync_replication: bool
        :param enforce_replication_factor: Check if there are enough replicas
            available at creation time, or halt the operation.
        :type enforce_replication_factor: bool
        :return: Standard collection API wrapper.
        :rtype: c8.collection.StandardCollection
        :raise c8.exceptions.CollectionCreateError: If create fails.
        """
        key_options = {'type': key_generator, 'allowUserKeys': user_keys}
        if key_increment is not None:
            key_options['increment'] = key_increment
        if key_offset is not None:
            key_options['offset'] = key_offset

        data = {
            'name': name,
            'waitForSync': sync,
            'doCompact': compact,
            'isSystem': system,
            'isVolatile': volatile,
            'keyOptions': key_options,
            'type': 3 if edge else 2
        }
        if journal_size is not None:
            data['journalSize'] = journal_size
        if shard_count is not None:
            data['numberOfShards'] = shard_count
        if shard_fields is not None:
            data['shardKeys'] = shard_fields
        if index_bucket_count is not None:
            data['indexBuckets'] = index_bucket_count
        if replication_factor is not None:
            data['replicationFactor'] = replication_factor
        if shard_like is not None:
            data['distributeShardsLike'] = shard_like

        params = {}
        if sync_replication is not None:
            params['waitForSyncReplication'] = sync_replication
        if enforce_replication_factor is not None:
            params['enforceReplicationFactor'] = enforce_replication_factor

        request = Request(
            method='post',
            endpoint='/collection',
            params=params,
            data=data
        )

        def response_handler(resp):
            if resp.is_success:
                return self.collection(name)
            raise CollectionCreateError(resp, request)

        return self._execute(request, response_handler)

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
        :raise c8.exceptions.CollectionDeleteError: If delete fails.
        """
        params = {}
        if system is not None:
            params['isSystem'] = system

        request = Request(
            method='delete',
            endpoint='/collection/{}'.format(name),
            params=params
        )

        def response_handler(resp):
            if resp.error_code == 1203 and ignore_missing:
                return False
            if not resp.is_success:
                raise CollectionDeleteError(resp, request)
            return True

        return self._execute(request, response_handler)

    ####################
    # Graph Management #
    ####################

    def graph(self, name):
        """Return the graph API wrapper.

        :param name: Graph name.
        :type name: str | unicode
        :return: Graph API wrapper.
        :rtype: c8.graph.Graph
        """
        return Graph(self._conn, self._executor, name)

    def has_graph(self, name):
        """Check if a graph exists in the fabric.

        :param name: Graph name.
        :type name: str | unicode
        :return: True if graph exists, False otherwise.
        :rtype: bool
        """
        for graph in self.graphs():
            if graph['name'] == name:
                return True
        return False

    def graphs(self):
        """List all graphs in the fabric.

        :return: Graphs in the fabric.
        :rtype: [dict]
        :raise c8.exceptions.GraphListError: If retrieval fails.
        """
        request = Request(method='get', endpoint='/_api/graph')

        def response_handler(resp):
            if not resp.is_success:
                raise GraphListError(resp, request)
            return [
                {
                    'id': body['_id'],
                    'name': body['_key'],
                    'revision': body['_rev'],
                    'orphan_collections': body['orphanCollections'],
                    'edge_definitions': [
                        {
                            'edge_collection': definition['collection'],
                            'from_vertex_collections': definition['from'],
                            'to_vertex_collections': definition['to'],
                        }
                        for definition in body['edgeDefinitions']
                    ],
                    'shard_count': body.get('numberOfShards'),
                    'replication_factor': body.get('replicationFactor')
                } for body in resp.body['graphs']
            ]

        return self._execute(request, response_handler)

    def create_graph(self,
                     name,
                     edge_definitions=None,
                     orphan_collections=None,
                     smart=None,
                     smart_field=None,
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
        :param smart: If set to True, sharding is enabled (see parameter
            **smart_field** below). Applies only to enterprise version of
            C8Db.
        :type smart: bool
        :param smart_field: Document field used to shard the vertices of the
            graph. To use this, parameter **smart** must be set to True and
            every vertex in the graph must have the smart field. Applies only
            to enterprise version of C8Db.
        :type smart_field: str | unicode
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
        data = {'name': name}
        if edge_definitions is not None:
            data['edgeDefinitions'] = [{
                'collection': definition['edge_collection'],
                'from': definition['from_vertex_collections'],
                'to': definition['to_vertex_collections']
            } for definition in edge_definitions]
        if orphan_collections is not None:
            data['orphanCollections'] = orphan_collections
        if smart is not None:  # pragma: no cover
            data['isSmart'] = smart
        if smart_field is not None:  # pragma: no cover
            data['smartGraphAttribute'] = smart_field
        if shard_count is not None:  # pragma: no cover
            data['numberOfShards'] = shard_count

        request = Request(
            method='post',
            endpoint='/_api/graph',
            data=data
        )

        def response_handler(resp):
            if resp.is_success:
                return Graph(self._conn, self._executor, name)
            raise GraphCreateError(resp, request)

        return self._execute(request, response_handler)

    def delete_graph(self, name, ignore_missing=False, drop_collections=None):
        """Drop the graph of the given name from the fabric.

        :param name: Graph name.
        :type name: str | unicode
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
        params = {}
        if drop_collections is not None:
            params['dropCollections'] = drop_collections

        request = Request(
            method='delete',
            endpoint='/_api/graph/{}'.format(name),
            params=params
        )

        def response_handler(resp):
            if resp.error_code == 1924 and ignore_missing:
                return False
            if not resp.is_success:
                raise GraphDeleteError(resp, request)
            return True

        return self._execute(request, response_handler)

    #######################
    # Document Management #
    #######################

    def has_document(self, document, rev=None, check_rev=True):
        """Check if a document exists.

        :param document: Document ID or body with "_id" field.
        :type document: str | unicode | dict
        :param rev: Expected document revision. Overrides value of "_rev" field
            in **document** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **document** (if given)
            is compared against the revision of target document.
        :type check_rev: bool
        :return: True if document exists, False otherwise.
        :rtype: bool
        :raise c8.exceptions.DocumentInError: If check fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_doc(document).has(
            document=document,
            rev=rev,
            check_rev=check_rev
        )

    def document(self, document, rev=None, check_rev=True):
        """Return a document.

        :param document: Document ID or body with "_id" field.
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
        return self._get_col_by_doc(document).get(
            document=document,
            rev=rev,
            check_rev=check_rev
        )

    def insert_document(self,
                        collection,
                        document,
                        return_new=False,
                        sync=None,
                        silent=False):
        """Insert a new document.

        :param collection: Collection name.
        :type collection: str | unicode
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
        :raise c8.exceptions.DocumentInsertError: If insert fails.
        """
        return self.collection(collection).insert(
            document=document,
            return_new=return_new,
            sync=sync,
            silent=silent
        )

    def update_document(self,
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
            must contain the "_id" field.
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
        return self._get_col_by_doc(document).update(
            document=document,
            check_rev=check_rev,
            merge=merge,
            keep_none=keep_none,
            return_new=return_new,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

    def replace_document(self,
                         document,
                         check_rev=True,
                         return_new=False,
                         return_old=False,
                         sync=None,
                         silent=False):
        """Replace a document.

        :param document: New document to replace the old one with. It must
            contain the "_id" field. Edge document must also have "_from" and
            "_to" fields.
        :type document: dict
        :param check_rev: If set to True, revision of **document** (if given)
            is compared against the revision of target document.
        :type check_rev: bool
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
        :raise c8.exceptions.DocumentReplaceError: If replace fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_doc(document).replace(
            document=document,
            check_rev=check_rev,
            return_new=return_new,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

    def delete_document(self,
                        document,
                        rev=None,
                        check_rev=True,
                        ignore_missing=False,
                        return_old=False,
                        sync=None,
                        silent=False):
        """Delete a document.

        :param document: Document ID, key or body. Document body must contain
            the "_id" field.
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
        return self._get_col_by_doc(document).delete(
            document=document,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            return_old=return_old,
            sync=sync,
            silent=silent
        )


    ###################
    # User Management #
    ###################
    # See tenant.py

    #########################
    # Permission Management #
    #########################
    # See tenant.py


    ########################
    # Async Job Management #
    ########################

    # Pratik: APIs not supported in documentation. Waiting for verification
    # def async_jobs(self, status, count=None):
    #     """Return IDs of async jobs with given status.
    #
    #     :param status: Job status (e.g. "pending", "done").
    #     :type status: str | unicode
    #     :param count: Max number of job IDs to return.
    #     :type count: int
    #     :return: List of job IDs.
    #     :rtype: [str | unicode]
    #     :raise c8.exceptions.AsyncJobListError: If retrieval fails.
    #     """
    #     params = {}
    #     if count is not None:
    #         params['count'] = count
    #
    #     request = Request(
    #         method='get',
    #         endpoint='/job/{}'.format(status),
    #         params=params
    #     )
    #
    #     def response_handler(resp):
    #         if resp.is_success:
    #             return resp.body
    #         raise AsyncJobListError(resp, request)
    #
    #     return self._execute(request, response_handler)
    #
    # def clear_async_jobs(self, threshold=None):
    #     """Clear async job results from the server.
    #
    #     Async jobs that are still queued or running are not stopped.
    #
    #     :param threshold: If specified, only the job results created prior to
    #         the threshold (a unix timestamp) are deleted. Otherwise, all job
    #         results are deleted.
    #     :type threshold: int
    #     :return: True if job results were cleared successfully.
    #     :rtype: bool
    #     :raise c8.exceptions.AsyncJobClearError: If operation fails.
    #     """
    #     if threshold is None:
    #         url = '/job/all'
    #         params = None
    #     else:
    #         url = '/job/expired'
    #         params = {'stamp': threshold}
    #
    #     request = Request(
    #         method='delete',
    #         endpoint=url,
    #         params=params
    #     )
    #
    #     def response_handler(resp):
    #         if resp.is_success:
    #             return True
    #         raise AsyncJobClearError(resp, request)
    #
    #     return self._execute(request, response_handler)

    ########################
    # Streams Management   #
    ########################

    def stream(self, operation_timeout_seconds=30):
        """Return the stream collection API wrapper.

        :return: stream collection API wrapper.
        :rtype: c8.stream_collection.StreamCollection
        """
        return StreamCollection(self, self._conn, self._executor, self.url, self.stream_port, operation_timeout_seconds)

    def streams(self):
        """Get list of all streams under given fabric

        :return: List of streams under given fabric.
        :rtype: json
        :raise c8.exceptions.StreamListError: If retrieving streams fails.
        """
        url_endpoint = '/streams'

        request = Request(
            method='get',
            endpoint=url_endpoint
        )
        
        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                # NOTE: server API returns stream name as field 'topic' - we provide both here for user convenience
                return [{
                    'name': col['topic'],
                    'topic': col['topic'],
                    'local': col['local'],
                    'db': col['db'],
                    'tenant': col['tenant'],
                    'type': StreamCollection.types[col['type']],
                    'status': 'terminated' if 'terminated' in col else 'active',
                } for col in map(dict, resp.body['result'])]
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)
        
        return self._execute(request, response_handler)
 
    def persistent_streams(self, local=False):
        """Get list of all streams under given fabric

        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: List of streams under given fabric.
        :rtype: json
        :raise c8.exceptions.StreamListError: If retrieving streams fails.
        """
        url_endpoint = '/streams/persistent?local={}'.format(local)

        request = Request(
            method='get',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                # NOTE: server API returns stream name as field 'topic' - we provide both here for user convenience
                return [{
                    'name': col['topic'],
                    'topic': col['topic'],
                    'local': col['local'],
                    'db': col['db'],
                    'tenant': col['tenant'],
                    'type': StreamCollection.types[col['type']],
                    'status': 'terminated' if 'terminated' in col else 'active',
                } for col in map(dict, resp.body['result'])]
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)


    # def nonpersistent_streams(self, local=False):
    #     """Get list of all streams under given fabric
    #
    #     :param persistent: persistent flag (if it is set to True, the API deletes persistent stream.
    #     If it is set to False, API deletes non-persistent stream)
    #     :param local: Operate on a local stream instead of a global one. Default value: false
    #     :return: List of streams under given fabric.
    #     :rtype: json
    #     :raise c8.exceptions.StreamListError: If retrieving streams fails.
    #     """
    #     url_endpoint = '/streams/non-persistent?local={}'.format(local)
    #
    #     request = Request(
    #         method='get',
    #         endpoint=url_endpoint
    #     )
    #
    #     def response_handler(resp):
    #         code = resp.status_code
    #         if resp.is_success:
    #             # NOTE: server API returns stream name as field 'topic' - we provide both here for user convenience
    #             return [{
    #                 'name': col['topic'],
    #                 'topic': col['topic'],
    #                 'local': col['local'],
    #                 'db': col['db'],
    #                 'tenant': col['tenant'],
    #                 'type': StreamCollection.types[col['type']],
    #                 'status': 'terminated' if 'terminated' in col else 'active',
    #             } for col in map(dict, resp.body['result'])]
    #
    #         elif code == 403:
    #             raise ex.StreamPermissionError(resp, request)
    #         raise ex.StreamConnectionError(resp, request)
    #
    #     return self._execute(request, response_handler)


    def has_stream(self, stream):
        """ Check if the list of streams has a stream with the given name.

        :param stream: The name of the stream for which to check in the list of all streams. 
        :type stream: str | unicode
        :return: True=stream found; False=stream not found.
        :rtype: bool
        """
        return any(mystream['name'] == stream for mystream in self.streams())


    def has_persistent_stream(self, stream, local=False):
        """ Check if the list of persistent streams has a stream with the given name
        and local setting.

        :param stream: The name of the stream for which to check in the list of persistent streams.
        :type stream: str | unicode
        :param local: if True, operate on a local stream instead of a global one. Default value: false
        :type local: bool
        :return: True=stream found; False=stream not found.
        :rtype: bool
        """
        return any(mystream['name'] == stream for mystream in self.persistent_streams(local))
        

    # def has_nonpersistent_stream(self, stream, local=False):
    #     """ Check if the list of nonpersistent streams has a stream with the given name
    #     and local setting.
    #
    #     :param stream: The name of the stream for which to check in the list of nonpersistent streams
    #     :type stream: str | unicode
    #     :param local: if True, operate on a local stream instead of a global one. Default value: false
    #     :type local: bool
    #     :return: True=stream found; False=stream not found.
    #     :rtype: bool
    #     """
    #     return any(mystream['name'] == stream for mystream in self.nonpersistent_streams(local))


    def create_stream(self, stream, local=False):
        """
        Create the stream under the given fabric
        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If creating streams fails.
        """
        if self.persistent:
            url_endpoint = '/streams/' + 'persistent/stream/{}?local={}'.format(stream, local)
        # else:
        #     url_endpoint = '/streams/' + 'non-persistent/stream/{}?local={}'.format(stream, local)
        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 502:
                raise ex.StreamCommunicationError(resp, request)

            raise ex.StreamCreateError(resp, request)

        return self._execute(request, response_handler)

    def delete_stream(self, stream, force=False, local=False):
        """
        Delete the streams under the given fabric
        :param stream: name of stream
        :param force:
        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If deleting streams fails.
        """
        # KARTIK : 20181002 : Stream delete not supported. 
        # We still have some issues to work through for stream deletion on the
        # pulsar side. So for v0.9.0 we only support terminate, and that too 
        # only for persistent streams.
        if not self.persistent:
            print("WARNING: Delete not yet implemented for nonpersistent streams. Returning 204. Stream will not be deleted.")
            # 204 = No Content
            return 204 

        # Persistent stream, let's terminate it instead.
        print("WARNING: Delete not yet implemented for persistent streams, calling terminate instead.")
        return self.terminate_stream(stream=stream, local=local)

        ######## HEY HEY DO THE ZOMBIE STOMP ########
        # KARTIK : 20181002 : Stream delete not supported. 
        # TODO : When stream delete is implemented, enable below code and 
        # remove the above code.
        # Below code is dead code for the moment, until delete stream is 
        # implemented on the server side. Consider it to be "#if 0"-ed out :-)
        # (why, yes indeed, that was a C reference)
        #if force and persistent:
        #    url_endpoint = '/streams/persistent/stream/{}?force=true&local={}'.format(stream, local)
        #elif force and not persistent:
        #    url_endpoint = '/streams/non-persistent/stream/{}?force=true&local={}'.format(stream, local)
        #elif not force and persistent:
        #    url_endpoint = '/streams/persistent/stream/{}?local={}'.format(stream, local)
        #elif not force and not persistent:
        #    url_endpoint = '/streams/non-persistent/stream/{}?local={}'.format(stream, local)
        #
        #request = Request(
        #    method='delete',
        #    endpoint=url_endpoint
        #)
        #
        #def response_handler(resp):
        #    code = resp.status_code
        #    if resp.is_success:
        #        return resp.body['result']
        #    elif code == 403:
        #        raise ex.StreamPermissionError(resp, request)
        #    elif code == 412:
        #        raise ex.StreamDeleteError(resp, request)
        #    raise ex.StreamConnectionError(resp, request)
        #
        #return self._execute(request, response_handler)

    def terminate_stream(self, stream,  local=False):
        """
        Terminate a stream. A stream that is terminated will not accept any more messages to be published and will let consumer to drain existing messages in backlog
        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: Dont have permission.
        """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/terminate?local={}'.format(stream, local)
        # else:
        #     # url_endpoint = '/streams/non-persistent/stream/{}/terminate?local={}'.format(stream, local)
        #     # KARTIK : 20181002 : terminate not supported for nonpersistent
        #     # streams. Just return 204 = No Content
        #     print("WARNING: Nonpersistent streams cannot be terminated. Returning 204.")
        #     return 204

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)


class StandardFabric(Fabric):
    """Standard fabric API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, connection):
        super(StandardFabric, self).__init__(
            connection=connection,
            executor=DefaultExecutor(connection)
        )

    def __repr__(self):
        return '<StandardFabric {}>'.format(self.name)

    def begin_async_execution(self, return_result=True):
        """Begin async execution.

        :param return_result: If set to True, API executions return instances
            of :class:`c8.job.AsyncJob`, which you can use to retrieve
            results from server once available. If set to False, API executions
            return None and no results are stored on server.
        :type return_result: bool
        :return: Fabric API wrapper built specifically for async execution.
        :rtype: c8.fabric.AsyncFabric
        """
        return AsyncFabric(self._conn, return_result)

    def begin_batch_execution(self, return_result=True):
        """Begin batch execution.

        :param return_result: If set to True, API executions return instances
            of :class:`c8.job.BatchJob` that are populated with results on
            commit. If set to False, API executions return None and no results
            are tracked client-side.
        :type return_result: bool
        :return: Fabric API wrapper built specifically for batch execution.
        :rtype: c8.fabric.BatchFabric
        """
        return BatchFabric(self._conn, return_result)

    def begin_transaction(self,
                          return_result=True,
                          timeout=None,
                          sync=None,
                          read=None,
                          write=None):
        """Begin transaction.

        :param return_result: If set to True, API executions return instances
            of :class:`c8.job.TransactionJob` that are populated with
            results on commit. If set to False, API executions return None and
            no results are tracked client-side.
        :type return_result: bool
        :param read: Names of collections read during transaction. If not
            specified, they are added automatically as jobs are queued.
        :type read: [str | unicode]
        :param write: Names of collections written to during transaction.
            If not specified, they are added automatically as jobs are queued.
        :type write: [str | unicode]
        :param timeout: Timeout for waiting on collection locks. If set to 0,
            C8Db server waits indefinitely. If not set, system default
            value is used.
        :type timeout: int
        :param sync: Block until the transaction is synchronized to disk.
        :type sync: bool
        :return: Fabric API wrapper built specifically for transactions.
        :rtype: c8.fabric.TransactionFabric
        """
        return TransactionFabric(
            connection=self._conn,
            return_result=return_result,
            read=read,
            write=write,
            timeout=timeout,
            sync=sync
        )


class AsyncFabric(Fabric):
    """Fabric API wrapper tailored specifically for async execution.

    See :func:`c8.fabric.StandardFabric.begin_async_execution`.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param return_result: If set to True, API executions return instances of
        :class:`c8.job.AsyncJob`, which you can use to retrieve results
        from server once available. If set to False, API executions return None
        and no results are stored on server.
    :type return_result: bool
    """

    def __init__(self, connection, return_result):
        super(AsyncFabric, self).__init__(
            connection=connection,
            executor=AsyncExecutor(connection, return_result)
        )

    def __repr__(self):
        return '<AsyncFabric {}>'.format(self.name)


class BatchFabric(Fabric):
    """Fabric API wrapper tailored specifically for batch execution.

    See :func:`c8.fabric.StandardFabric.begin_batch_execution`.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param return_result: If set to True, API executions return instances of
        :class:`c8.job.BatchJob` that are populated with results on commit.
        If set to False, API executions return None and no results are tracked
        client-side.
    :type return_result: bool
    """

    def __init__(self, connection, return_result):
        super(BatchFabric, self).__init__(
            connection=connection,
            executor=BatchExecutor(connection, return_result)
        )

    def __repr__(self):
        return '<BatchFabric {}>'.format(self.name)

    def __enter__(self):
        return self

    def __exit__(self, exception, *_):
        if exception is None:
            self._executor.commit()

    def queued_jobs(self):
        """Return the queued batch jobs.

        :return: Queued batch jobs or None if **return_result** parameter was
            set to False during initialization.
        :rtype: [c8.job.BatchJob] | None
        """
        return self._executor.jobs

    def commit(self):
        """Execute the queued requests in a single batch API request.

        If **return_result** parameter was set to True during initialization,
        :class:`c8.job.BatchJob` instances are populated with results.

        :return: Batch jobs, or None if **return_result** parameter was set to
            False during initialization.
        :rtype: [c8.job.BatchJob] | None
        :raise c8.exceptions.BatchStateError: If batch state is invalid
            (e.g. batch was already committed or the response size did not
            match expected).
        :raise c8.exceptions.BatchExecuteError: If commit fails.
        """
        return self._executor.commit()


class TransactionFabric(Fabric):
    """Fabric API wrapper tailored specifically for transactions.

    See :func:`c8.fabric.StandardFabric.begin_transaction`.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param return_result: If set to True, API executions return instances of
        :class:`c8.job.TransactionJob` that are populated with results on
        commit. If set to False, API executions return None and no results are
        tracked client-side.
    :type return_result: bool
    :param read: Names of collections read during transaction.
    :type read: [str | unicode]
    :param write: Names of collections written to during transaction.
    :type write: [str | unicode]
    :param timeout: Timeout for waiting on collection locks. If set to 0, the
        C8Db server waits indefinitely. If not set, system default value
        is used.
    :type timeout: int
    :param sync: Block until operation is synchronized to disk.
    :type sync: bool
    """

    def __init__(self, connection, return_result, read, write, timeout, sync):
        super(TransactionFabric, self).__init__(
            connection=connection,
            executor=TransactionExecutor(
                connection=connection,
                return_result=return_result,
                read=read,
                write=write,
                timeout=timeout,
                sync=sync
            )
        )

    def __repr__(self):
        return '<TransactionFabric {}>'.format(self.name)

    def __enter__(self):
        return self

    def __exit__(self, exception, *_):
        if exception is None:
            self._executor.commit()

    def queued_jobs(self):
        """Return the queued transaction jobs.

        :return: Queued transaction jobs, or None if **return_result** was set
            to False during initialization.
        :rtype: [c8.job.TransactionJob] | None
        """
        return self._executor.jobs

    def commit(self):
        """Execute the queued requests in a single transaction API request.

        If **return_result** parameter was set to True during initialization,
        :class:`c8.job.TransactionJob` instances are populated with
        results.

        :return: Transaction jobs, or None if **return_result** parameter was
            set to False during initialization.
        :rtype: [c8.job.TransactionJob] | None
        :raise c8.exceptions.TransactionStateError: If the transaction was
            already committed.
        :raise c8.exceptions.TransactionExecuteError: If commit fails.
        """
        return self._executor.commit()

