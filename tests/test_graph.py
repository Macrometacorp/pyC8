from __future__ import absolute_import, unicode_literals

import random

from six import string_types

from c8.collection import EdgeCollection
from c8.exceptions import (
    DocumentDeleteError,
    DocumentInsertError,
    DocumentParseError,
    DocumentRevisionError,
    DocumentUpdateError,
    EdgeDefinitionCreateError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionReplaceError,
    GraphCreateError,
    GraphDeleteError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
    VertexCollectionListError,
)
from tests.helpers import (
    assert_raises,
    clean_doc,
    extract,
    generate_col_name,
    generate_doc_key,
    generate_graph_name,
)


def test_graph_properties(graph, tst_fabric):
    assert repr(graph) == "<Graph {}>".format(graph.name)
    properties = graph.properties()
    assert properties["id"] == "_graphs/{}".format(graph.name)
    assert properties["name"] == graph.name
    assert len(properties["edge_definitions"]) == 1
    assert properties["orphan_collections"] == []
    assert "smart" in properties
    assert "shard_count" in properties
    assert isinstance(properties["revision"], string_types)

    new_graph_name = generate_graph_name()
    new_graph = tst_fabric.create_graph(
        new_graph_name,
        # TODO only possible with enterprise edition
        # smart=True,
        # smart_field='foo',
        # shard_count=2
    )
    properties = new_graph.properties()
    assert properties["id"] == "_graphs/{}".format(new_graph_name)
    assert properties["name"] == new_graph_name
    assert properties["edge_definitions"] == []
    assert properties["orphan_collections"] == []
    assert isinstance(properties["revision"], string_types)
    # TODO only possible with enterprise edition
    # assert properties['smart'] is True
    # assert properties['smart_field'] == 'foo'
    # assert properties['shard_count'] == 2


def test_graph_management(tst_fabric):
    # Test create graph
    graph_name = generate_graph_name()
    assert tst_fabric.has_graph(graph_name) is False

    graph = tst_fabric.create_graph(graph_name)
    assert tst_fabric.has_graph(graph_name) is True
    assert graph.name == graph_name
    assert graph.fabric_name == tst_fabric.name

    # Test create duplicate graph
    with assert_raises(GraphCreateError) as err:
        tst_fabric.create_graph(graph_name)
    assert err.value.error_code == 1925

    # Test get graph
    result = tst_fabric.graph(graph_name)
    assert result.name == graph.name
    assert result.fabric_name == graph.fabric_name

    # Test get graphs
    result = tst_fabric.graphs()
    for entry in result:
        assert "revision" in entry
        assert "edge_definitions" in entry
        assert "orphan_collections" in entry
    assert graph_name in extract("name", tst_fabric.graphs())

    # Test delete graph
    assert tst_fabric.delete_graph(graph_name) is True
    assert graph_name not in extract("name", tst_fabric.graphs())

    # Test delete missing graph
    with assert_raises(GraphDeleteError) as err:
        tst_fabric.delete_graph(graph_name)
    assert err.value.error_code == 1924
    assert tst_fabric.delete_graph(graph_name, ignore_missing=True) is False

    # Create a graph with vertex and edge collections and delete the graph
    graph = tst_fabric.create_graph(graph_name)
    ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()

    graph.create_vertex_collection(fvcol_name)
    graph.create_vertex_collection(tvcol_name)
    graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name],
    )
    collections = extract("name", tst_fabric.collections())
    assert fvcol_name in collections
    assert tvcol_name in collections
    assert ecol_name in collections

    tst_fabric.delete_graph(graph_name)
    collections = extract("name", tst_fabric.collections())
    assert fvcol_name in collections
    assert tvcol_name in collections
    assert ecol_name in collections

    # Create a graph with vertex and edge collections and delete all
    graph = tst_fabric.create_graph(graph_name)
    graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name],
    )
    tst_fabric.delete_graph(graph_name, drop_collections=True)
    collections = extract("name", tst_fabric.collections())
    assert fvcol_name not in collections
    assert tvcol_name not in collections
    assert ecol_name not in collections


def test_vertex_collection_management(tst_fabric, graph, bad_graph, client):
    # Test create valid "from" vertex collection
    fvcol_name = generate_col_name()
    assert not graph.has_vertex_collection(fvcol_name)
    assert not tst_fabric.has_collection(fvcol_name)

    fvcol = graph.create_vertex_collection(fvcol_name)
    assert graph.has_vertex_collection(fvcol_name)
    assert tst_fabric.has_collection(fvcol_name)
    assert fvcol.name == fvcol_name
    assert fvcol.graph == graph.name
    assert fvcol_name in repr(fvcol)
    assert fvcol_name in graph.vertex_collections()
    assert fvcol_name in extract("name", tst_fabric.collections())

    # Test create duplicate vertex collection
    with assert_raises(VertexCollectionCreateError) as err:
        graph.create_vertex_collection(fvcol_name)
    assert err.value.error_code == 1938
    assert fvcol_name in graph.vertex_collections()
    assert fvcol_name in extract("name", tst_fabric.collections())

    # Test create valid "to" vertex collection
    tvcol_name = generate_col_name()
    assert not graph.has_vertex_collection(tvcol_name)
    assert not tst_fabric.has_collection(tvcol_name)

    tvcol = graph.create_vertex_collection(tvcol_name)
    assert graph.has_vertex_collection(tvcol_name)
    assert tst_fabric.has_collection(tvcol_name)
    assert tvcol_name == tvcol_name
    assert tvcol.graph == graph.name
    assert tvcol_name in repr(tvcol)
    assert tvcol_name in graph.vertex_collections()
    assert tvcol_name in extract("name", tst_fabric.collections())

    # Test list vertex collection via bad fabric
    with assert_raises(VertexCollectionListError) as err:
        client._tenant.useFabric(generate_graph_name()).graph(
            bad_graph.name
        ).vertex_collections()
    assert err.value.error_code == 11

    # Test delete missing vertex collection
    with assert_raises(VertexCollectionDeleteError) as err:
        graph.delete_vertex_collection(generate_col_name())
    assert err.value.error_code == 1928

    # Test delete "to" vertex collection with purge option
    assert graph.delete_vertex_collection(tvcol_name, purge=True) is True
    assert tvcol_name not in graph.vertex_collections()
    assert fvcol_name in extract("name", tst_fabric.collections())
    assert tvcol_name not in extract("name", tst_fabric.collections())
    assert not graph.has_vertex_collection(tvcol_name)

    # Test delete "from" vertex collection without purge option
    assert graph.delete_vertex_collection(fvcol_name, purge=False) is True
    assert fvcol_name not in graph.vertex_collections()
    assert fvcol_name in extract("name", tst_fabric.collections())
    assert not graph.has_vertex_collection(fvcol_name)


def test_edge_definition_management(tst_fabric, graph, fvcol, tvcol, ecol, bad_graph):
    # Check graph with random generated edge definitions
    ecol_name = generate_col_name()
    assert not graph.has_edge_definition(ecol_name)
    assert not graph.has_edge_collection(ecol_name)
    assert not tst_fabric.has_collection(ecol_name)

    # Test create edge definition with missing vertex collection
    with assert_raises(EdgeDefinitionCreateError) as err:
        graph.create_edge_definition(ecol_name, [], [])
    assert err.value.error_code == 1923

    # Check existing edge definitions
    assert [
        {
            "edge_collection": ecol.name,
            "from_vertex_collections": [fvcol.name],
            "to_vertex_collections": [tvcol.name],
        }
    ] == graph.edge_definitions()
    assert graph.has_edge_collection(ecol.name) is True
    assert tst_fabric.has_collection(ecol.name) is True

    # Test create duplicate edge definition
    with assert_raises(EdgeDefinitionCreateError) as err:
        graph.create_edge_definition(
            edge_collection=ecol.name,
            from_vertex_collections=[fvcol.name],
            to_vertex_collections=[tvcol.name],
        )
    assert err.value.error_code == 1920

    # Test create edge definition with existing vertex collection
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    ecol_name = generate_col_name()
    edge_col = graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name],
    )
    assert edge_col.name == ecol_name
    assert {
        "edge_collection": ecol_name,
        "from_vertex_collections": [fvcol_name],
        "to_vertex_collections": [tvcol_name],
    } in graph.edge_definitions()

    assert ecol_name in extract("name", tst_fabric.collections())
    vertex_collections = graph.vertex_collections()
    assert fvcol_name in vertex_collections
    assert tvcol_name in vertex_collections

    # Test create edge definition with missing vertex collection
    bad_vcol_name = generate_col_name()
    ecol_name = generate_col_name()
    ecol = graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[bad_vcol_name],
        to_vertex_collections=[bad_vcol_name],
    )
    assert graph.has_edge_definition(ecol_name)
    assert graph.has_edge_collection(ecol_name)
    assert ecol.name == ecol_name
    assert {
        "edge_collection": ecol_name,
        "from_vertex_collections": [bad_vcol_name],
        "to_vertex_collections": [bad_vcol_name],
    } in graph.edge_definitions()
    assert bad_vcol_name in graph.vertex_collections()
    assert bad_vcol_name in extract("name", tst_fabric.collections())
    assert bad_vcol_name in extract("name", tst_fabric.collections())

    # Test replace edge definition (happy path)
    ecol = graph.replace_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[tvcol_name],
        to_vertex_collections=[fvcol_name],
    )
    assert isinstance(ecol, EdgeCollection)
    assert ecol.name == ecol_name
    assert {
        "edge_collection": ecol_name,
        "from_vertex_collections": [tvcol_name],
        "to_vertex_collections": [fvcol_name],
    } in graph.edge_definitions()

    # Test replace missing edge definition
    bad_ecol_name = generate_col_name()
    with assert_raises(EdgeDefinitionReplaceError):
        graph.replace_edge_definition(
            edge_collection=bad_ecol_name,
            from_vertex_collections=[],
            to_vertex_collections=[fvcol_name],
        )

    # Test delete missing edge definition
    with assert_raises(EdgeDefinitionDeleteError) as err:
        graph.delete_edge_definition(bad_ecol_name)
    assert err.value.error_code == 1930

    # Test delete existing edge definition with purge
    assert graph.delete_edge_definition(ecol_name, purge=True) is True
    assert {
        "edge_collection": ecol_name,
        "from_vertex_collections": [tvcol_name],
        "to_vertex_collections": [fvcol_name],
    } not in graph.edge_definitions()
    assert ecol_name in extract("name", tst_fabric.collections())
    assert not graph.has_edge_definition(ecol_name)
    assert not graph.has_edge_collection(ecol_name)


def test_create_graph_with_edge_definition(tst_fabric):
    new_graph_name = generate_graph_name()
    new_ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    ovcol_name = generate_col_name()

    edge_definition = {
        "edge_collection": new_ecol_name,
        "from_vertex_collections": [fvcol_name],
        "to_vertex_collections": [tvcol_name],
    }
    new_graph = tst_fabric.create_graph(
        new_graph_name,
        edge_definitions=[edge_definition],
        orphan_collections=[ovcol_name],
    )
    assert edge_definition in new_graph.edge_definitions()


def test_vertex_management(fvcol, bad_fvcol, fvdocs):
    # Test insert vertex with no key
    result = fvcol.insert({})
    assert result["_key"] in fvcol
    assert len(fvcol) == 1
    fvcol.truncate()

    # Test insert vertex with ID
    vertex_id = fvcol.name + "/" + "foo"
    fvcol.insert({"_id": vertex_id})
    assert "foo" in fvcol
    assert vertex_id in fvcol
    assert len(fvcol) == 1
    fvcol.truncate()

    with assert_raises(DocumentParseError) as err:
        fvcol.insert({"_id": generate_col_name() + "/" + "foo"})
    assert "bad collection name" in err.value.message

    vertex = fvdocs[0]
    key = vertex["_key"]

    # Test insert first valid vertex
    result = fvcol.insert(vertex)
    assert result["_key"] == key
    assert "_rev" in result
    assert vertex in fvcol and key in fvcol
    assert len(fvcol) == 1
    assert fvcol[key]["val"] == vertex["val"]

    # Test insert duplicate vertex
    with assert_raises(DocumentInsertError) as err:
        fvcol.insert(vertex)
    assert err.value.error_code == 1210
    assert len(fvcol) == 1

    vertex = fvdocs[1]
    key = vertex["_key"]

    # Test insert second valid vertex
    result = fvcol.insert(vertex, sync=True)
    assert result["_key"] == key
    assert vertex in fvcol and key in fvcol
    assert len(fvcol) == 2
    assert fvcol[key]["val"] == vertex["val"]

    vertex = fvdocs[2]
    key = vertex["_key"]

    # Test insert third valid vertex with silent set to True
    assert fvcol.insert(vertex, silent=True) is True
    assert len(fvcol) == 3
    assert fvcol[key]["val"] == vertex["val"]

    # Test get missing vertex
    if fvcol.context != "transaction":
        assert fvcol.get(generate_doc_key()) is None

    # Test get existing edge by body with "_key" field
    result = fvcol.get({"_key": key})
    assert clean_doc(result) == vertex

    # Test get existing edge by body with "_id" field
    result = fvcol.get({"_id": fvcol.name + "/" + key})
    assert clean_doc(result) == vertex

    # Test get existing vertex by key
    result = fvcol.get(key)
    assert clean_doc(result) == vertex

    # Test get existing vertex by ID
    result = fvcol.get(fvcol.name + "/" + key)
    assert clean_doc(result) == vertex

    # Test get existing vertex with bad revision
    old_rev = result["_rev"]
    with assert_raises(DocumentRevisionError) as err:
        fvcol.get(key, rev=old_rev + "1", check_rev=True)
    assert err.value.error_code == 1200

    # Test update vertex with a single field change
    assert "foo" not in fvcol.get(key)
    result = fvcol.update({"_key": key, "foo": 100})
    assert result["_key"] == key
    assert fvcol[key]["foo"] == 100
    old_rev = fvcol[key]["_rev"]

    # Test update vertex with silent set to True
    assert "bar" not in fvcol[vertex]
    assert fvcol.update({"_key": key, "bar": 200}, silent=True) is True
    assert fvcol[vertex]["bar"] == 200
    assert fvcol[vertex]["_rev"] != old_rev
    old_rev = fvcol[key]["_rev"]

    # Test update vertex with multiple field changes
    result = fvcol.update({"_key": key, "foo": 200, "bar": 300})
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert fvcol[key]["foo"] == 200
    assert fvcol[key]["bar"] == 300
    old_rev = result["_rev"]

    # Test update vertex with correct revision
    result = fvcol.update({"_key": key, "_rev": old_rev, "bar": 400})
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert fvcol[key]["foo"] == 200
    assert fvcol[key]["bar"] == 400
    old_rev = result["_rev"]

    # Test update vertex with bad revision
    if fvcol.context != "transaction":
        new_rev = old_rev + "1"
        with assert_raises(DocumentRevisionError) as err:
            fvcol.update({"_key": key, "_rev": new_rev, "bar": 500})
        assert err.value.error_code == 1200
        assert fvcol[key]["foo"] == 200
        assert fvcol[key]["bar"] == 400

    # Test update vertex in missing vertex collection
    with assert_raises(DocumentUpdateError) as err:
        fvcol.update({"_key": "1000", "bar": 500})
    assert err.value.error_code == 1202
    assert fvcol[key]["foo"] == 200
    assert fvcol[key]["bar"] == 400

    # Test update vertex with sync set to True
    result = fvcol.update({"_key": key, "bar": 500}, sync=True)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert fvcol[key]["foo"] == 200
    assert fvcol[key]["bar"] == 500
    old_rev = result["_rev"]

    # Test update vertex with keep_none set to True
    result = fvcol.update({"_key": key, "bar": None}, keep_none=True)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert fvcol[key]["foo"] == 200
    assert fvcol[key]["bar"] is None
    old_rev = result["_rev"]

    # Test update vertex with keep_none set to False
    result = fvcol.update({"_key": key, "foo": None}, keep_none=False)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert "foo" not in fvcol[key]
    assert fvcol[key]["bar"] is None

    # Test replace vertex with a single field change
    result = fvcol.replace({"_key": key, "baz": 100})
    assert result["_key"] == key
    assert "foo" not in fvcol[key]
    assert "bar" not in fvcol[key]
    assert fvcol[key]["baz"] == 100
    old_rev = result["_rev"]

    # Test replace vertex with silent set to True
    assert fvcol.replace({"_key": key, "bar": 200}, silent=True) is True
    assert "foo" not in fvcol[key]
    assert "baz" not in fvcol[vertex]
    assert fvcol[vertex]["bar"] == 200
    assert len(fvcol) == 3
    assert fvcol[vertex]["_rev"] != old_rev
    old_rev = fvcol[vertex]["_rev"]

    # Test replace vertex with multiple field changes
    vertex = {"_key": key, "foo": 200, "bar": 300}
    result = fvcol.replace(vertex)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert clean_doc(fvcol[key]) == vertex
    old_rev = result["_rev"]

    # Test replace vertex with correct revision
    vertex = {"_key": key, "_rev": old_rev, "bar": 500}
    result = fvcol.replace(vertex)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert clean_doc(fvcol[key]) == clean_doc(vertex)
    old_rev = result["_rev"]

    # Test replace vertex with bad revision
    if fvcol.context != "transaction":
        new_rev = old_rev + "10"
        vertex = {"_key": key, "_rev": new_rev, "bar": 600}
        with assert_raises(DocumentRevisionError) as err:
            fvcol.replace(vertex)
        assert err.value.error_code == 1200
        assert fvcol[key]["bar"] == 500
        assert "foo" not in fvcol[key]

    # Test replace vertex with sync set to True
    vertex = {"_key": key, "bar": 400, "foo": 200}
    result = fvcol.replace(vertex, sync=True)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert fvcol[key]["foo"] == 200
    assert fvcol[key]["bar"] == 400

    # Test delete vertex with bad revision
    if fvcol.context != "transaction":
        old_rev = fvcol[key]["_rev"]
        vertex["_rev"] = old_rev + "1"
        with assert_raises(DocumentRevisionError) as err:
            fvcol.delete(vertex, check_rev=True)
        assert err.value.error_code == 1200
        vertex["_rev"] = old_rev
        assert vertex in fvcol

    # Test delete missing vertex
    bad_key = generate_doc_key()
    with assert_raises(DocumentDeleteError) as err:
        fvcol.delete(bad_key, ignore_missing=False)
    assert err.value.error_code == 1202
    if fvcol.context != "transaction":
        assert fvcol.delete(bad_key, ignore_missing=True) is False

    # Test delete existing vertex with sync set to True
    assert fvcol.delete(vertex, sync=True, check_rev=False) is True
    if fvcol.context != "transaction":
        assert fvcol[vertex] is None
    assert vertex not in fvcol
    assert len(fvcol) == 2
    fvcol.truncate()


def test_vertex_management_via_graph(graph, fvcol):
    # Test insert vertex via graph object
    result = graph.insert_vertex(fvcol.name, {})
    assert result["_key"] in fvcol
    assert len(fvcol) == 1
    vertex_id = result["_id"]

    # Test get vertex via graph object
    assert graph.vertex(vertex_id)["_id"] == vertex_id

    # Test update vertex via graph object
    result = graph.update_vertex({"_id": vertex_id, "foo": 100})
    assert result["_id"] == vertex_id
    assert fvcol[vertex_id]["foo"] == 100

    # Test replace vertex via graph object
    result = graph.replace_vertex({"_id": vertex_id, "bar": 200})
    assert result["_id"] == vertex_id
    assert "foo" not in fvcol[vertex_id]
    assert fvcol[vertex_id]["bar"] == 200

    # Test delete vertex via graph object
    assert graph.delete_vertex(vertex_id) is True
    assert vertex_id not in fvcol
    assert len(fvcol) == 0


def test_edge_management(ecol, bad_ecol, edocs, fvcol, fvdocs, tvcol, tvdocs):
    for vertex in fvdocs:
        fvcol.insert(vertex)
    for vertex in tvdocs:
        tvcol.insert(vertex)

    edge = edocs[0]
    key = edge["_key"]

    # Test insert edge with no key
    result = ecol.insert({"_from": edge["_from"], "_to": edge["_to"]})
    assert result["_key"] in ecol
    assert len(ecol) == 1
    ecol.truncate()

    # Test insert vertex with ID
    edge_id = ecol.name + "/" + "foo"
    ecol.insert({"_id": edge_id, "_from": edge["_from"], "_to": edge["_to"]})
    assert "foo" in ecol
    assert edge_id in ecol
    assert len(ecol) == 1
    ecol.truncate()

    with assert_raises(DocumentParseError) as err:
        ecol.insert(
            {
                "_id": generate_col_name() + "/" + "foo",
                "_from": edge["_from"],
                "_to": edge["_to"],
            }
        )
    assert "bad collection name" in err.value.message

    # Test insert first valid edge
    result = ecol.insert(edge)
    assert result["_key"] == key
    assert "_rev" in result
    assert edge in ecol and key in ecol
    assert len(ecol) == 1
    assert ecol[key]["_from"] == edge["_from"]
    assert ecol[key]["_to"] == edge["_to"]

    # Test insert duplicate edge
    with assert_raises(DocumentInsertError) as err:
        assert ecol.insert(edge)
    assert err.value.error_code in {1906, 1210}
    assert len(ecol) == 1

    edge = edocs[1]
    key = edge["_key"]

    # Test insert second valid edge with silent set to True
    assert ecol.insert(edge, sync=True, silent=True) is True
    assert edge in ecol and key in ecol
    assert len(ecol) == 2
    assert ecol[key]["_from"] == edge["_from"]
    assert ecol[key]["_to"] == edge["_to"]

    # Test insert third valid edge using link method
    from_vertex = fvcol.get(fvdocs[2])
    to_vertex = tvcol.get(tvdocs[2])
    result = ecol.link(from_vertex, to_vertex, sync=False)
    assert result["_key"] in ecol
    assert len(ecol) == 3

    # Test insert fourth valid edge using link method
    from_vertex = fvcol.get(fvdocs[2])
    to_vertex = tvcol.get(tvdocs[0])
    assert (
        ecol.link(
            from_vertex["_id"],
            to_vertex["_id"],
            {"_id": ecol.name + "/foo"},
            sync=True,
            silent=True,
        )
        is True
    )
    assert "foo" in ecol
    assert len(ecol) == 4

    with assert_raises(DocumentParseError) as err:
        assert ecol.link({}, {})
    assert err.value.message == 'field "_id" required'

    # Test get missing vertex
    bad_document_key = generate_doc_key()
    if ecol.context != "transaction":
        assert ecol.get(bad_document_key) is None

    # Test get existing edge by body with "_key" field
    result = ecol.get({"_key": key})
    assert clean_doc(result) == edge

    # Test get existing edge by body with "_id" field
    result = ecol.get({"_id": ecol.name + "/" + key})
    assert clean_doc(result) == edge

    # Test get existing edge by key
    result = ecol.get(key)
    assert clean_doc(result) == edge

    # Test get existing edge by ID
    result = ecol.get(ecol.name + "/" + key)
    assert clean_doc(result) == edge

    # Test get existing edge with bad revision
    old_rev = result["_rev"]
    with assert_raises(DocumentRevisionError) as err:
        ecol.get(key, rev=old_rev + "1")
    assert err.value.error_code in {1903, 1200}

    # Test get existing edge with bad fabric
    # with assert_raises(DocumentGetError) as err:
    #     bad_ecol.get(key)
    # assert err.value.error_code == 1228

    # Test update edge with a single field change
    assert "foo" not in ecol.get(key)
    result = ecol.update({"_key": key, "foo": 100})
    assert result["_key"] == key
    assert ecol[key]["foo"] == 100
    old_rev = ecol[key]["_rev"]

    # Test update edge with multiple field changes
    result = ecol.update({"_key": key, "foo": 200, "bar": 300})
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert ecol[key]["foo"] == 200
    assert ecol[key]["bar"] == 300
    old_rev = result["_rev"]

    # Test update edge with correct revision
    result = ecol.update({"_key": key, "_rev": old_rev, "bar": 400})
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert ecol[key]["foo"] == 200
    assert ecol[key]["bar"] == 400
    old_rev = result["_rev"]

    if ecol.context != "transaction":
        # Test update edge with bad revision
        new_rev = old_rev + "1"
        with assert_raises(DocumentRevisionError):
            ecol.update({"_key": key, "_rev": new_rev, "bar": 500})
        assert ecol[key]["foo"] == 200
        assert ecol[key]["bar"] == 400

    # Test update edge in missing edge collection
    with assert_raises(DocumentUpdateError) as err:
        ecol.update({"_key": "1000", "bar": 500})
    assert err.value.error_code == 1202
    assert ecol[key]["foo"] == 200
    assert ecol[key]["bar"] == 400

    # Test update edge with sync option
    result = ecol.update({"_key": key, "bar": 500}, sync=True)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert ecol[key]["foo"] == 200
    assert ecol[key]["bar"] == 500
    old_rev = result["_rev"]

    # Test update edge with silent option
    assert ecol.update({"_key": key, "bar": 600}, silent=True) is True
    assert ecol[key]["foo"] == 200
    assert ecol[key]["bar"] == 600
    assert ecol[key]["_rev"] != old_rev
    old_rev = ecol[key]["_rev"]

    # Test update edge without keep_none option
    result = ecol.update({"_key": key, "bar": None}, keep_none=True)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert ecol[key]["foo"] == 200
    assert ecol[key]["bar"] is None
    old_rev = result["_rev"]

    # Test update edge with keep_none option
    result = ecol.update({"_key": key, "foo": None}, keep_none=False)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert "foo" not in ecol[key]
    assert ecol[key]["bar"] is None

    # Test replace edge with a single field change
    edge["foo"] = 100
    result = ecol.replace(edge)
    assert result["_key"] == key
    assert ecol[key]["foo"] == 100
    old_rev = ecol[key]["_rev"]

    # Test replace edge with silent set to True
    edge["bar"] = 200
    assert ecol.replace(edge, silent=True) is True
    assert ecol[key]["foo"] == 100
    assert ecol[key]["bar"] == 200
    assert ecol[key]["_rev"] != old_rev
    old_rev = ecol[key]["_rev"]

    # Test replace edge with multiple field changes
    edge["foo"] = 200
    edge["bar"] = 300
    result = ecol.replace(edge)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert ecol[key]["foo"] == 200
    assert ecol[key]["bar"] == 300
    old_rev = result["_rev"]

    # Test replace edge with correct revision
    edge["foo"] = 300
    edge["bar"] = 400
    edge["_rev"] = old_rev
    result = ecol.replace(edge)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert ecol[key]["foo"] == 300
    assert ecol[key]["bar"] == 400
    old_rev = result["_rev"]

    edge["bar"] = 500
    if ecol.context != "transaction":
        # Test replace edge with bad revision
        edge["_rev"] = old_rev + key
        with assert_raises(DocumentRevisionError) as err:
            ecol.replace(edge)
        assert err.value.error_code == 1200
        assert ecol[key]["foo"] == 300
        assert ecol[key]["bar"] == 400

    # Test replace edge with bad fabric
    with assert_raises(DocumentRevisionError) as err:
        bad_ecol.replace(edge)
    assert err.value.error_code == 1200
    assert ecol[key]["foo"] == 300
    assert ecol[key]["bar"] == 400

    # Test replace edge with sync option
    result = ecol.replace(edge, sync=True, check_rev=False)
    assert result["_key"] == key
    assert result["_old_rev"] == old_rev
    assert ecol[key]["foo"] == 300
    assert ecol[key]["bar"] == 500

    # Test delete edge with bad revision
    if ecol.context != "transaction":
        old_rev = ecol[key]["_rev"]
        edge["_rev"] = old_rev + "1"
        with assert_raises(DocumentRevisionError) as err:
            ecol.delete(edge, check_rev=True)
        assert err.value.error_code == 1200
        edge["_rev"] = old_rev
        assert edge in ecol

    # Test delete missing edge
    with assert_raises(DocumentDeleteError) as err:
        ecol.delete(bad_document_key, ignore_missing=False)
    assert err.value.error_code == 1202
    if ecol.context != "transaction":
        assert not ecol.delete(bad_document_key, ignore_missing=True)

    # Test delete existing edge with sync set to True
    assert ecol.delete(edge, sync=True, check_rev=False) is True
    if ecol.context != "transaction":
        assert ecol[edge] is None
    assert edge not in ecol
    ecol.truncate()


def test_vertex_edges(tst_fabric, graph):
    graph_name = generate_graph_name()
    vcol_name = generate_col_name()
    ecol_name = generate_col_name()

    # Prepare test documents
    anna = {"_id": "{}/anna".format(vcol_name)}
    dave = {"_id": "{}/dave".format(vcol_name)}
    josh = {"_id": "{}/josh".format(vcol_name)}
    mary = {"_id": "{}/mary".format(vcol_name)}
    tony = {"_id": "{}/tony".format(vcol_name)}

    # Create test graph, vertex and edge collections
    school = tst_fabric.create_graph(graph_name)

    vcol = school.create_vertex_collection(vcol_name)
    ecol = school.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[vcol_name],
        to_vertex_collections=[vcol_name],
    )
    # Insert test vertices into the graph
    vcol.insert(anna)
    vcol.insert(dave)
    vcol.insert(josh)
    vcol.insert(mary)
    vcol.insert(tony)

    # Insert test edges into the graph
    ecol.link(anna, dave)
    ecol.link(josh, dave)
    ecol.link(mary, dave)
    ecol.link(tony, dave)
    ecol.link(dave, anna)

    # Test edges with default direction (both)
    result = ecol.edges(dave)
    assert "stats" in result
    assert "filtered" in result["stats"]
    assert "scanned_index" in result["stats"]
    assert len(result["edges"]) == 5

    result = ecol.edges(anna)
    assert len(result["edges"]) == 2

    # Test edges with direction set to "in"
    result = ecol.edges(dave, direction="in")
    assert len(result["edges"]) == 4

    result = ecol.edges(anna, direction="in")
    assert len(result["edges"]) == 1

    # Test edges with direction set to "out"
    result = ecol.edges(dave, direction="out")
    assert len(result["edges"]) == 1

    result = ecol.edges(anna, direction="out")
    assert len(result["edges"]) == 1


def test_edge_management_via_graph(graph, ecol, fvcol, fvdocs, tvcol, tvdocs):
    for vertex in fvdocs:
        fvcol.insert(vertex)
    for vertex in tvdocs:
        tvcol.insert(vertex)
    ecol.truncate()

    # Get a random "from" vertex
    from_vertex = fvcol.get({"_key": str(random.randint(1, 3))})
    assert graph.has_vertex(from_vertex)

    # Get a random "to" vertex
    to_vertex = tvcol.get({"_key": str(random.randint(4, 6))})
    assert graph.has_vertex(to_vertex)

    # Test insert edge via graph object
    result = graph.insert_edge(
        ecol.name, {"_from": from_vertex["_id"], "_to": to_vertex["_id"]}
    )
    assert result["_key"] in ecol
    assert graph.has_edge(result["_id"])
    assert len(ecol) == 1

    # Test link vertices via graph object
    result = graph.link(ecol.name, from_vertex, to_vertex)
    assert result["_key"] in ecol
    assert len(ecol) == 2
    edge_id = result["_id"]

    # Test get edge via graph object
    assert graph.edge(edge_id)["_id"] == edge_id

    # Test list edges via graph object
    result = graph.edges(ecol.name, from_vertex, direction="out")
    assert "stats" in result
    assert len(result["edges"]) == 2

    result = graph.edges(ecol.name, from_vertex, direction="in")
    assert "stats" in result
    assert len(result["edges"]) == 0

    # Test update edge via graph object
    result = graph.update_edge({"_id": edge_id, "foo": 100})
    assert result["_id"] == edge_id
    assert ecol[edge_id]["foo"] == 100

    # Test replace edge via graph object
    result = graph.replace_edge(
        {
            "_id": edge_id,
            "_from": from_vertex["_id"],
            "_to": to_vertex["_id"],
            "bar": 200,
        }
    )
    assert result["_id"] == edge_id
    assert "foo" not in ecol[edge_id]
    assert ecol[edge_id]["bar"] == 200

    # Test delete edge via graph object
    assert graph.delete_edge(edge_id) is True
    assert edge_id not in ecol
    assert len(ecol) == 1
