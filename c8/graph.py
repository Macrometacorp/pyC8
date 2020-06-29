from __future__ import absolute_import, unicode_literals

from c8.api import APIWrapper
from c8.collection import EdgeCollection
from c8.collection import VertexCollection
from c8.exceptions import (
    EdgeDefinitionListError,
    EdgeDefinitionCreateError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionReplaceError,
    GraphPropertiesError,
    VertexCollectionListError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
)
from c8.request import Request
from c8.utils import get_col_name

__all__ = ['Graph']


class Graph(APIWrapper):
    """Graph API wrapper.

    :param executor: API executor.
    :type executor: c8.executor.Executor
    :param name: Graph name.
    :type name: str | unicode
    """

    def __init__(self, connection, executor, name):
        super(Graph, self).__init__(connection, executor)
        self._name = name

    def __repr__(self):
        return '<Graph {}>'.format(self._name)

    def _get_col_by_vertex(self, vertex):
        """Return the vertex collection for the given vertex document.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | unicode | dict
        :return: Vertex collection API wrapper.
        :rtype: c8.collection.VertexCollection
        """
        return self.vertex_collection(get_col_name(vertex))

    def _get_col_by_edge(self, edge):
        """Return the vertex collection for the given edge document.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | unicode | dict
        :return: Edge collection API wrapper.
        :rtype: c8.collection.EdgeCollection
        """
        return self.edge_collection(get_col_name(edge))

    @property
    def name(self):
        """Return the graph name.

        :return: Graph name.
        :rtype: str | unicode
        """
        return self._name

    def properties(self):
        """Return graph properties.

        :return: Graph properties.
        :rtype: dict
        :raise c8.exceptions.GraphPropertiesError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/graph/{}'.format(self._name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise GraphPropertiesError(resp, request)
            body = resp.body['graph']
            properties = {
                'id': body['_id'],
                'name': body['name'],
                'revision': body['_rev'],
                'orphan_collections': body['orphanCollections'],
                'edge_definitions': [
                    {
                        'edge_collection': edge_definition['collection'],
                        'from_vertex_collections': edge_definition['from'],
                        'to_vertex_collections': edge_definition['to'],
                    }
                    for edge_definition in body['edgeDefinitions']
                ]
            }
            if 'isSmart' in body:
                properties['smart'] = body['isSmart']
            if 'smartGraphAttribute' in body:
                properties['smart_field'] = body['smartGraphAttribute']
            if 'numberOfShards' in body:
                properties['shard_count'] = body['numberOfShards']
            if 'replicationFactor' in body:
                properties['replication_factor'] = body['replicationFactor']
            return properties

        return self._execute(request, response_handler)

    ################################
    # Vertex Collection Management #
    ################################

    def has_vertex_collection(self, name):
        """Check if the graph has the given vertex collection.

        :param name: Vertex collection name.
        :type name: str | unicode
        :return: True if vertex collection exists, False otherwise.
        :rtype: bool
        """
        return name in self.vertex_collections()

    def vertex_collections(self):
        """Return vertex collections in the graph that are not orphaned.

        :return: Names of vertex collections that are not orphaned.
        :rtype: [str | unicode]
        :raise c8.exceptions.VertexCollectionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/graph/{}/vertex'.format(self._name),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise VertexCollectionListError(resp, request)
            return sorted(set(resp.body['collections']))

        return self._execute(request, response_handler)

    def vertex_collection(self, name):
        """Return the vertex collection API wrapper.

        :param name: Vertex collection name.
        :type name: str | unicode
        :return: Vertex collection API wrapper.
        :rtype: c8.collection.VertexCollection
        """
        return VertexCollection(self._conn, self._executor, self._name, name)

    def create_vertex_collection(self, name):
        """Create a vertex collection in the graph.

        :param name: Vertex collection name.
        :type name: str | unicode
        :return: Vertex collection API wrapper.
        :rtype: c8.collection.VertexCollection
        :raise c8.exceptions.VertexCollectionCreateError: If create fails.
        """
        request = Request(
            method='post',
            endpoint='/graph/{}/vertex'.format(self._name),
            data={'collection': name}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise VertexCollectionCreateError(resp, request)
            return self.vertex_collection(name)

        return self._execute(request, response_handler)

    def delete_vertex_collection(self, name, purge=False):
        """Remove a vertex collection from the graph.

        :param name: Vertex collection name.
        :type name: str | unicode
        :param purge: If set to True, the vertex collection is not just deleted
            from the graph but also from the fabric completely.
        :type purge: bool
        :return: True if vertex collection was deleted successfully.
        :rtype: bool
        :raise c8.exceptions.VertexCollectionDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/graph/{}/vertex/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise VertexCollectionDeleteError(resp, request)
            return True

        return self._execute(request, response_handler)

    ##############################
    # Edge Collection Management #
    ##############################

    def has_edge_definition(self, name):
        """Check if the graph has the given edge definition.

        :param name: Edge collection name.
        :type name: str | unicode
        :return: True if edge definition exists, False otherwise.
        :rtype: bool
        """
        return any(
            definition['edge_collection'] == name
            for definition in self.edge_definitions()
        )

    def has_edge_collection(self, name):
        """Check if the graph has the given edge collection.

        :param name: Edge collection name.
        :type name: str | unicode
        :return: True if edge collection exists, False otherwise.
        :rtype: bool
        """
        return self.has_edge_definition(name)

    def edge_collection(self, name):
        """Return the edge collection API wrapper.

        :param name: Edge collection name.
        :type name: str | unicode
        :return: Edge collection API wrapper.
        :rtype: c8.collection.EdgeCollection
        """
        return EdgeCollection(self._conn, self._executor, self._name, name)

    def edge_definitions(self):
        """Return the edge definitions of the graph.

        :return: Edge definitions of the graph.
        :rtype: [dict]
        :raise c8.exceptions.EdgeDefinitionListError: If retrieval fails.
        """
        try:
            return self.properties()['edge_definitions']
        except GraphPropertiesError as err:
            raise EdgeDefinitionListError(err.response, err.request)

    def create_edge_definition(self,
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
        request = Request(
            method='post',
            endpoint='/graph/{}/edge'.format(self._name),
            data={
                'collection': edge_collection,
                'from': from_vertex_collections,
                'to': to_vertex_collections
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise EdgeDefinitionCreateError(resp, request)
            return self.edge_collection(edge_collection)

        return self._execute(request, response_handler)

    def replace_edge_definition(self,
                                edge_collection,
                                from_vertex_collections,
                                to_vertex_collections):
        """Replace an edge definition.

        :param edge_collection: Edge collection name.
        :type edge_collection: str | unicode
        :param from_vertex_collections: Names of "from" vertex collections.
        :type from_vertex_collections: [str | unicode]
        :param to_vertex_collections: Names of "to" vertex collections.
        :type to_vertex_collections: [str | unicode]
        :return: True if edge definition was replaced successfully.
        :rtype: bool
        :raise c8.exceptions.EdgeDefinitionReplaceError: If replace fails.
        """
        request = Request(
            method='put',
            endpoint='/graph/{}/edge/{}'.format(
                self._name, edge_collection
            ),
            data={
                'collection': edge_collection,
                'from': from_vertex_collections,
                'to': to_vertex_collections
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise EdgeDefinitionReplaceError(resp, request)
            return self.edge_collection(edge_collection)

        return self._execute(request, response_handler)

    def delete_edge_definition(self, name, purge=False):
        """Delete an edge definition from the graph.

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
        request = Request(
            method='delete',
            endpoint='/graph/{}/edge/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise EdgeDefinitionDeleteError(resp, request)
            return True

        return self._execute(request, response_handler)

    #####################
    # Vertex Management #
    #####################

    def has_vertex(self, vertex, rev=None, check_rev=True):
        """Check if the given vertex document exists in the graph.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :return: True if vertex document exists, False otherwise.
        :rtype: bool
        :raise c8.exceptions.DocumentGetError: If check fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).has(vertex, rev, check_rev)

    def vertex(self, vertex, rev=None, check_rev=True):
        """Return a vertex document.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :return: Vertex document or None if not found.
        :rtype: dict | None
        :raise c8.exceptions.DocumentGetError: If retrieval fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).get(vertex, rev, check_rev)

    def insert_vertex(self, collection, vertex, sync=None, silent=False):
        """Insert a new vertex document.

        :param collection: Vertex collection name.
        :type collection: str | unicode
        :param vertex: New vertex document to insert. If it has "_key" or "_id"
            field, its value is used as key of the new vertex (otherwise it is
            auto-generated). Any "_rev" field is ignored.
        :type vertex: dict
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
        return self.vertex_collection(collection).insert(vertex, sync, silent)

    def update_vertex(self,
                      vertex,
                      check_rev=True,
                      keep_none=True,
                      sync=None,
                      silent=False):
        """Update a vertex document.

        :param vertex: Partial or full vertex document with updated values. It
            must contain the "_id" field.
        :type vertex: dict
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
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
        return self._get_col_by_vertex(vertex).update(
            vertex=vertex,
            check_rev=check_rev,
            keep_none=keep_none,
            sync=sync,
            silent=silent
        )

    def replace_vertex(self, vertex, check_rev=True, sync=None, silent=False):
        """Replace a vertex document.

        :param vertex: New vertex document to replace the old one with. It must
            contain the "_id" field.
        :type vertex: dict
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
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
        return self._get_col_by_vertex(vertex).replace(
            vertex=vertex,
            check_rev=check_rev,
            sync=sync,
            silent=silent
        )

    def delete_vertex(self,
                      vertex,
                      rev=None,
                      check_rev=True,
                      ignore_missing=False,
                      sync=None):
        """Delete a vertex document.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised on failures.
        :type ignore_missing: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :return: True if vertex was deleted successfully, False if vertex was
            not found and **ignore_missing** was set to True (does not apply in
            transactions).
        :rtype: bool
        :raise c8.exceptions.DocumentDeleteError: If delete fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).delete(
            vertex=vertex,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            sync=sync
        )

    ###################
    # Edge Management #
    ###################

    def has_edge(self, edge, rev=None, check_rev=True):
        """Check if the given edge document exists in the graph.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :return: True if edge document exists, False otherwise.
        :rtype: bool
        :raise c8.exceptions.DocumentInError: If check fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).has(edge, rev, check_rev)

    def edge(self, edge, rev=None, check_rev=True):
        """Return an edge document.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :return: Edge document or None if not found.
        :rtype: dict | None
        :raise c8.exceptions.DocumentGetError: If retrieval fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).get(edge, rev, check_rev)

    def insert_edge(self, collection, edge, sync=None, silent=False):
        """Insert a new edge document.

        :param collection: Edge collection name.
        :type collection: str | unicode
        :param edge: New edge document to insert. It must contain "_from" and
            "_to" fields. If it has "_key" or "_id" field, its value is used
            as key of the new edge document (otherwise it is auto-generated).
            Any "_rev" field is ignored.
        :type edge: dict
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
        return self.edge_collection(collection).insert(edge, sync, silent)

    def update_edge(self,
                    edge,
                    check_rev=True,
                    keep_none=True,
                    sync=None,
                    silent=False):
        """Update an edge document.

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
        return self._get_col_by_edge(edge).update(
            edge=edge,
            check_rev=check_rev,
            keep_none=keep_none,
            sync=sync,
            silent=silent
        )

    def replace_edge(self, edge, check_rev=True, sync=None, silent=False):
        """Replace an edge document.

        :param edge: New edge document to replace the old one with. It must
            contain the "_id" field. It must also contain the "_from" and "_to"
            fields.
        :type edge: dict
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
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
        return self._get_col_by_edge(edge).replace(
            edge=edge,
            check_rev=check_rev,
            sync=sync,
            silent=silent
        )

    def delete_edge(self,
                    edge,
                    rev=None,
                    check_rev=True,
                    ignore_missing=False,
                    sync=None):
        """Delete an edge document.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if present.
        :type rev: str | unicode
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised on failures.
        :type ignore_missing: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool
        :return: True if edge was deleted successfully, False if edge was not
            found and **ignore_missing** was set to True (does not  apply in
            transactions).
        :rtype: bool
        :raise c8.exceptions.DocumentDeleteError: If delete fails.
        :raise c8.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).delete(
            edge=edge,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            sync=sync
        )

    def link(self,
             collection,
             from_vertex,
             to_vertex,
             data=None,
             sync=None,
             silent=False):
        """Insert a new edge document linking the given vertices.

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
        return self.edge_collection(collection).link(
            from_vertex=from_vertex,
            to_vertex=to_vertex,
            data=data,
            sync=sync,
            silent=silent
        )

    def edges(self, collection, vertex, direction=None):
        """Return the edge documents coming in and/or out of given vertex.

        :param collection: Edge collection name.
        :type collection: str | unicode
        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | unicode | dict
        :param direction: The direction of the edges. Allowed values are "in"
            and "out". If not set, edges in both directions are returned.
        :type direction: str | unicode
        :return: List of edges
        :rtype: dict
        :raise c8.exceptions.EdgeListError: If retrieval fails.
        """
        return self.edge_collection(collection).edges(vertex, direction)
