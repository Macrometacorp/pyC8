from __future__ import absolute_import, unicode_literals

import json
import random
import base64

import websocket

from c8.api import APIWrapper
from c8.c8ql import C8QL
from c8.keyvalue import KV
from c8.collection import StandardCollection
from c8.stream_apps import StreamApps
from c8.apikeys import APIKeys
from c8 import constants
from c8.exceptions import (
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
    ServerVersionError,
    StreamCommunicationError,
    StreamConnectionError,
    StreamCreateError,
    StreamPermissionError,
    TransactionExecuteError,
    TenantDcListError,
    SpotRegionUpdateError,
    RestqlValidationError,
    RestqlListError,
    RestqlCreateError,
    RestqlUpdateError,
    RestqlDeleteError,
    RestqlExecuteError,
    EventCreateError,
    EventDeleteError,
    EventGetError,
    StreamAppGetSampleError,
    GetAPIKeys
)
from c8.executor import (
    DefaultExecutor,
    AsyncExecutor,
    BatchExecutor,
    TransactionExecutor,
)
from c8.graph import Graph
from c8.stream_collection import StreamCollection
from c8.request import Request

__all__ = [
    'StandardFabric',
    'AsyncFabric',
    'BatchFabric',
    'TransactionFabric'
]
ENDPOINT = "/streams"


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

    def enum(**enums):
        return type('Enum', (), enums)

    SPOT_CREATION_TYPES = enum(AUTOMATIC='automatic', NONE='none',
                               SPOT_REGION='spot_region')

    def __init__(self, connection, executor):
        self.url = connection.url
        self.header = connection.headers
        self.stream_port = constants.STREAM_PORT
        super(Fabric, self).__init__(connection, executor)

    def __getitem__(self, name):
        """Return the collection API wrapper.

        :param name: Collection name.
        :type name: str | unicode
        :return: Collection API wrapper.
        :rtype: c8.collection.StandardCollection
        """
        return self.collection(name)

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

    @property
    def key_value(self):
        """Return KV (Key Value) API wrapper.

        :return: KV API wrapper.
        :rtype: c8.keyvalue.KV
        """
        return KV(self._conn, self._executor)


    def on_change(self, collection, callback):
        """Execute given input function on receiving a change.

        :param callback: Function to execute on a change
        :type callback: function
        :param collections: Collection name(s) regex to listen for
        :type collections: str
        """
        if not callback:
            raise ValueError('You must specify a callback function')
        
        if not collection:
            raise ValueError('You must specify a collection on which realtime '
                             'data is to be watched!')

        namespace = constants.STREAM_LOCAL_NS_PREFIX + self.fabric_name

        subscription_name = "%s-%s-subscription-%s" % (
            self.tenant_name, self.fabric_name, str(random.randint(1, 1000)))
        
        url = self.url.split("//")[1].split(":")[0]

        topic = "wss://{}/_ws/ws/v2/consumer/persistent/{}/{}/{}/{}".format(
            url,self.tenant_name,namespace,
            collection,subscription_name)

        ws = websocket.create_connection(topic, header=self.header)

        try:
            print("pyC8 Realtime: Begin monitoring realtime updates for " +
                  topic)
            while True:
                msg = json.loads(ws.recv())
                data =  base64.b64decode(msg['payload'])
                ws.send(json.dumps({'messageId' : msg['messageId']}))
                callback(data)
              
        except Exception as e:
            print(e)
        finally:
            ws.close()

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

    def update_spot_region(self, tenant, fabric, new_dc):
        """Updates spot primary region for the geo-fabric
            :param: tenant: tenant name
            :type: str
            :param: fabric: fabric name
            :type: str
            :param: new_dc: New spot region
            :type: str
            :return: True if request successful,false otherwise
            :rtype: bool
            :raise c8.exceptions.SpotRegionUpdateError: If updation fails.
        """

        request = Request(method='put',
                          endpoint='_fabric/{}/database/{}'.format(
                          fabric, new_dc))

        def response_handler(resp):
            if not resp.is_success:
                raise SpotRegionUpdateError(resp, request)
            return True

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
                'name': col['name'],
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
            endpoint='/version',
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

    def dclist(self, detail=False):
        """Return the list of names of Datacenters

        :param detail: detail list of DCs if set to true else only DC names
        :type: boolean
        :return: DC List.
        :rtype: [str | unicode ]
        :raise c8.exceptions.TenantListError: If retrieval fails.
        """
        properties = self.properties()
        if not detail:
            return properties["options"]["dcList"].split(",")

        tenant_name = properties["options"]["tenant"]

        request = Request(
            method='get',
            endpoint='/datacenter/_tenant/%s' % tenant_name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TenantDcListError(resp, request)
            dc_list = resp.body[0]["dcInfo"]
            for dc in dc_list:
                if dc["name"] not in properties["options"]["dcList"]:
                    dc_list.remove(dc)
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

    def create_fabric(self, name, spot_dc=None, users=None, dclist=None,
                      spot_creation_type=SPOT_CREATION_TYPES.AUTOMATIC):
        """Create a new fabric.

        :param name: Fabric name.
        :type name: str | unicode
        :param spot_creation_type: Specifying the mode of creating geo-fabric.
                                   If you use AUTOMATIC, a random spot region
                                   will be assigned by the system. If you
                                   specify NONE, a geo-fabric is created
                                   without the spot properties. If you specify
                                   SPOT_REGION,pass the corresponding spot
                                   region in the spot_dc parameter.
        :type name: Enum containing spot region creation types
        :param name: Spot Region name, if spot_creation_type is set to
                     SPOT_REGION
        :type name: str
        :param users: List of users with access to the new fabric, where each
            user is a dictionary with fields "username", "password", "active"
            and "extra" (see below for example). If not set, only the admin and
            current user are granted access.
        :type users: [dict]
        :param dclist : list of strings of datacenters
        :type dclist: [str | unicode]
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
        dcl = ''
        if dclist:
            # Process dclist param (type list) to build up comma-separated
            # string of DCs
            for dc in dclist:
                if len(dcl) > 0:
                    dcl += ','
                dcl += dc
        options['dcList'] = dcl

        if spot_creation_type == self.SPOT_CREATION_TYPES.NONE:
            options['spotDc'] = ''
        elif (spot_creation_type == self.SPOT_CREATION_TYPES.SPOT_REGION and
                spot_dc):
            options['spotDc'] = spot_dc

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
                'isSpot': col["isSpot"],
                'type': StandardCollection.types[col['type']],
                'status': StandardCollection.statuses[col['status']],
            } for col in map(dict, resp.body['result'])]

        return self._execute(request, response_handler)

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
        :raise c8.exceptions.CollectionCreateError: If create fails.
        """
        key_options = {'type': key_generator, 'allowUserKeys': user_keys}
        if key_increment is not None:
            key_options['increment'] = key_increment
        if key_offset is not None:
            key_options['offset'] = key_offset
        if spot_collection and local_collection:
            return("Collection can either be spot or local")
        else:
            data = {
                'name': name,
                'waitForSync': sync,
                'keyOptions': key_options,
                'type': 3 if edge else 2,
                'isSpot': spot_collection,
                'isLocal': local_collection,
                'isSystem': is_system
            }

        if shard_fields is not None:
            data['shardKeys'] = shard_fields
        if index_bucket_count is not None:
            data['indexBuckets'] = index_bucket_count

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
        request = Request(method='get', endpoint='/graph')

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
                        } for definition in body['edgeDefinitions']
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
        data = {'name': name}
        if edge_definitions is not None:
            data['edgeDefinitions'] = [{
                'collection': definition['edge_collection'],
                'from': definition['from_vertex_collections'],
                'to': definition['to_vertex_collections']
            } for definition in edge_definitions]
        if orphan_collections is not None:
            data['orphanCollections'] = orphan_collections
        if shard_count is not None:  # pragma: no cover
            data['numberOfShards'] = shard_count

        request = Request(
            method='post',
            endpoint='/graph',
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
            endpoint='/graph/{}'.format(name),
            params=params
        )

        def response_handler(resp):
            if resp.error_code == 1924 and ignore_missing:
                return False
            if not resp.is_success:
                raise GraphDeleteError(resp, request)
            return True

        return self._execute(request, response_handler)

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
        return StreamCollection(self, self._conn, self._executor, self.url,
                                self.stream_port, operation_timeout_seconds)

    def streams(self, local=False):
        """Get list of all streams under given fabric

        :return: List of streams under given fabric.
        :rtype: json
        :raise c8.exceptions.StreamListError: If retrieving streams fails.
        """
        if local is False:
            url_endpoint = '/streams?global=true'

        elif local is True:
            url_endpoint = '/streams?global=false'

        request = Request(
            method='get',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return [{
                    'name': col['topic'],
                    'topic': col['topic'],
                    'local': col['local'],
                    'db': col['db'],
                    'tenant': col['tenant'],
                    'type': StreamCollection.types[col['type']],
                    'status': 'terminated' if 'terminated' in col else 'active',  # noqa
                } for col in map(dict, resp.body['result'])]
            elif code == 403:
                raise StreamPermissionError(resp, request)
            raise StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

 
    def has_stream(self, stream, isCollectionStream=False, local=False):
     
        """ Check if the list of streams has a stream with the given name.

        :param stream: The name of the stream for which to check in the list
                       of all streams.
        :type stream: str | unicode
        :return: True=stream found; False=stream not found.
        :rtype: bool
        """
        if isCollectionStream is False:
            if local is False and "c8globals" not in stream:
                stream = "c8globals." + stream
            elif local is True and "c8locals" not in stream:
                stream = "c8locals." + stream     
        return any(mystream['name'] == stream for mystream in self.streams(local=local))


    def create_stream(self, stream, local=False):
        """
        Create the stream under the given fabric
        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If creating streams fails.
        """
        if local is True:
            endpoint = '{}/{}?global=False'.format(ENDPOINT, stream)
        elif local is False:
            endpoint = '{}/{}?global=True'.format(ENDPOINT, stream)

        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 502:
                raise StreamCommunicationError(resp, request)
            raise StreamCreateError(resp, request)

        return self._execute(request, response_handler)

    def delete_stream(self, stream, force=False, isCollectionStream=False, local=False):
        """
        Delete the streams under the given fabric
        :param stream: name of stream
        :param force:
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If deleting streams fails.
        """
        # KARTIK : 20181002 : Stream delete not supported.
        # We still have some issues to work through for stream deletion on the
        # pulsar side. So for v0.9.0 we only support terminate, and that too
        # only for persistent streams.
        print("WARNING: Delete not yet implemented for persistent streams, "
              "calling terminate instead.")
        # if isCollectionStream is False:
           # if local is True:
              #  stream = "c8locals." + stream
            #else:
             #   stream = "c8globals." + stream
        return self.terminate_stream(stream=stream, isCollectionStream=isCollectionStream,  local=local)

        # TODO : When stream delete is implemented, enable below code and
        # remove the above code.
        # endpoint = '{}/{}?local={}'.format(ENDPOINT, stream, local)
        # if force:
        #    endpoint = endpoint + "&force=true"
        #
        # request = Request(method='delete', endpoint=endpoint)
        #
        # def response_handler(resp):
        #    code = resp.status_code
        #    if resp.is_success:
        #        return resp.body['result']
        #    elif code == 403:
        #        raise StreamPermissionError(resp, request)
        #    elif code == 412:
        #        raise StreamDeleteError(resp, request)
        #    raise StreamConnectionError(resp, request)
        #
        # return self._execute(request, response_handler)

    def terminate_stream(self, stream, isCollectionStream=False, local=False):
        """Terminate a stream. A stream that is terminated will not accept any
        more messages to be published and will let consumer to drain existing
        messages in backlog

        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: Dont have permission.
        """
        if isCollectionStream is False:
            if local is False:
                stream = "c8globals." + stream
            else:
                stream = "c8locals." + stream
        endpoint = '{}/{}/terminate?local={}'.format(ENDPOINT, stream, local)
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise StreamPermissionError(resp, request)
            raise StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    #####################
    # Restql Management #
    #####################

    def save_restql(self, data):
        """Save restql by name.

        :param data: data to be used for restql POST API
        :type data: dict
        :return: Results of restql API
        :rtype: dict
        :raise c8.exceptions.RestqlCreateError: if restql operation failed
        """

        query_name = data["query"]["name"]
        if " " in query_name:
            raise RestqlValidationError("White Spaces not allowed in Query "
                                        "Name")

        request = Request(method="post", endpoint="/restql", data=data)

        def response_handler(resp):
            if not resp.is_success:
                raise RestqlCreateError(resp, request)
            return resp.body["result"]

        return self._execute(request, response_handler)

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

        if data and "bindVars" in data:
            request = Request(method="post", data=data,
                              endpoint="/restql/execute/%s" % name)
        else:
            request = Request(method="post",
                              endpoint="/restql/execute/%s" % name)

        def response_handler(resp):
            if not resp.is_success:
                raise RestqlExecuteError(resp, request)
            return resp.body

        return self._execute(request, response_handler)

    def get_all_restql(self):
        """Get all restql associated for user.

        :return: Details of all restql
        :rtype: list
        :raise c8.exceptions.RestqlListError: if getting restql failed
        """

        request = Request(method="get", endpoint="/restql/user")

        def response_handler(resp):
            if not resp.is_success:
                raise RestqlListError(resp, request)
            return resp.body["result"]

        return self._execute(request, response_handler)

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
        request = Request(method="put", data=data, endpoint="/restql/" + name)

        def response_handler(resp):
            if not resp.is_success:
                raise RestqlUpdateError(resp, request)
            return True

        return self._execute(request, response_handler)

    def delete_restql(self, name):
        """Delete restql by name.

        :param name: restql name
        :type name: str | unicode
        :return: True if restql is deleted
        :rtype: bool
        :raise c8.exceptions.RestqlDeleteError: if restql deletion failed
        """
        request = Request(method="delete", endpoint="/restql/" + name)

        def response_handler(resp):
            if not resp.is_success:
                raise RestqlDeleteError(resp, request)
            return True

        return self._execute(request, response_handler)

    ########################
    # Events #
    ########################

    def create_event(self, payload):
        """Create an event.

        :param payload: Payload to create event
        :type payload: dict
        :return:  Dictionary containing the event id
        :rtype: dict
        :raise c8.exceptions.EventCreateError: if event creation failed

        Here is an example entry for parameter **payload**:

        .. code-block:: python
            {
            "action": "string",
            "attributes": {},
            "description": "string",
            "details": "string",
            "entityName": "string",
            "entityType": "string",
            "status": "string"
            }
        """
        request = Request(method="post", endpoint="/events", data=payload)

        def response_handler(resp):
            if not resp.is_success:
                raise EventCreateError(resp, request)
            return resp.body

        return self._execute(request, response_handler)

    def delete_event(self, eventIds):
        """Delete an event/s.

        :param eventIds: The event id for which you want to fetch the event details
        :type eventId: list of strings(event Ids)
        :return: List containig all the information of existing events
        :rtype: list
        :raise c8.exceptions.EventDeleteError: if event creation failed

        """
        data = (json.dumps((eventIds)))

        request = Request(method="delete", endpoint="/events", data = data)

        def response_handler(resp):
            if not resp.is_success:
                raise EventGetError(resp, request)
            return True

        return self._execute(request, response_handler)

    def get_all_events(self):
        """Create an event.

        :return: List containig all the information of existing events
        :rtype: list
        :raise c8.exceptions.EventGetError: if event creation failed

        """
        request = Request(method="get", endpoint="/events/tenant")

        def response_handler(resp):
            if not resp.is_success:
                raise EventGetError(resp, request)
            return resp.body

        return self._execute(request, response_handler)
    
    def get_event_by_Id(self, eventId):
        """Create an event.

        :param eventId: The event id for which you want to fetch the event details
        :return: List containig all the information of existing events
        :rtype: list
        :raise c8.exceptions.EventGetError: if event creation failed

        """
        request = Request(method="get", endpoint="/events/"+str(eventId))

        def response_handler(resp):
            if not resp.is_success:
                raise EventGetError(resp, request)
            return resp.body

        return self._execute(request, response_handler)     

    ########################
    # Stream Apps #
    ########################

    def stream_app(self,name):
        return StreamApps(self._conn, self._executor, name)

    def validate_stream_app(self, data):
        """validates a stream app by given data
        @data: stream app defination string
        """
        body = {"definition": data}
        req = Request(
            method = "post",
            endpoint='/streamapps/validate',
            data=json.dumps(body)
        )
        
        def response_handler(resp):
            if resp.is_success is True:
                return True
            print(resp.body)
            return False
        
        return self._execute(req,response_handler)

    def retrive_stream_app(self):
        """retrieves a stream app by given body
        """
        req = Request(
            method = "get",
            endpoint='/streamapps',
        )
        
        def response_handler(resp):
            if resp.is_success is True:
                return resp.body
            print(resp.body)
            return False
        
        return self._execute(req,response_handler)

    def get_samples_stream_app(self):
        """gets samples for stream apps
        """
        req = Request(
            method = "get",
            endpoint='/streamapps/samples',
        )
        
        def response_handler(resp):
            if resp.is_success is not True:
                raise StreamAppGetSampleError(resp,req)
            return resp.body["streamAppSample"]    
            
        
        return self._execute(req,response_handler)

    def create_stream_app(self, data, dclist=[]):
        """Creates a stream application by given data
        @data: stream app definition
        @dclist: regions where stream app has to be deployed
        """
        # create request body
        req_body = {
            "definition":data,
            "regions":dclist
        } 
        # create request
        req = Request(
            method = "post",
            endpoint='/streamapps',
            data=json.dumps(req_body)
        )
        # create response handler
        def response_handler(resp):
            if resp.is_success is True:
                print(resp.body)
                return True
            print(resp.body)
            return False
        # call api
        return self._execute(req,response_handler)   


    ########################
    # APIKeys #
    ########################
    def api_keys(self, keyid):
        """Return the API keys API wrapper.
        :param keyid: API Key id
        :type kaeyid: string | unicode
        :return:API keys API wrapper.
        :rtype: c8.stream_collection.StreamCollection
        """
        return APIKeys(self._conn, self._executor, keyid)


    def list_all_api_keys(self):
        """List the API keys.

        :return:list.
        :raise c8.exceptions.GetAPIKeys: If request fails
        """
        request = Request(
            method = "get",
            endpoint='/key',
        )
        # create response handler
        def response_handler(resp):
            if not resp.is_success:
                raise GetAPIKeys(resp, request)
            else:
                return resp.body['result']
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