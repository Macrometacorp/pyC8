from __future__ import absolute_import, unicode_literals

from c8.exceptions import (
    AnalyzerGetDefinitionError,
    AnalyzerListError,
    SearchCollectionSetError,
    SearchError,
    ViewCreateError,
    ViewGetError,
    ViewRenameError,
    ViewDeleteError,
    ViewGetError,
    ViewGetPropertiesError,
    ViewRenameError,
    ViewUpdatePropertiesError,
    AnalyzerListError,
    AnalyzerGetDefinitionError,
)
from tests.helpers import assert_raises, extract


def test_search_methods(client, tst_fabric_name, col, docs):
    client._tenant.useFabric(tst_fabric_name)
    col.insert_many(docs)

    # Test set and search in collection
    assert client.set_search(col.name, "true", "text") is True
    resp = client.search_in_collection(
        collection=col.name,
        search="FILTER doc.text == @input RETURN doc",
        bindVars={"input": "foo"},
        ttl=30,
    )
    result = resp["result"]
    for doc in result:
        assert doc["text"] == "foo"

    # Test create view
    links = {}
    links[col.name] = {
        "analyzers": ["identity"],
        "fields": {"_key": {}},
        "includeAllFields": False,
        "storeValues": "none",
        "trackListPositions": False,
    }

    primary_sort = [{"field": "_key", "asc": True}]
    resp = client.create_view("view1", links, primary_sort)
    assert resp["links"] == links
    assert resp["name"] == "view1"
    assert resp["primarySort"] == primary_sort

    # Test update properties
    links[col.name] = {
        "analyzers": ["identity"],
        "fields": {"val": {}},
        "includeAllFields": False,
        "storeValues": "none",
        "trackListPositions": False,
    }

    properties = {"name": "view1", "primarySort": [], "links": links, "type": "search"}

    client.update_view_properties("view1", properties)
    assert "view1" in extract("name", client.list_all_views())

    # Test primary sort is immutable
    resp = client.get_view_properties("view1")
    assert resp["result"]["links"] == links
    assert resp["result"]["primarySort"] == primary_sort

    # Test get view info
    resp = client.get_view_info("view1")
    assert resp["result"]["type"] == "search"
    assert resp["result"]["name"] == "view1"

    # Test rename view
    client.rename_view("view1", "view2")
    assert "view1" not in extract("name", client.list_all_views())
    assert "view2" in extract("name", client.list_all_views())

    # Test get analyzers
    assert "identity" in extract("name", client.get_list_of_analyzer())
    assert "text_en" in extract("name", client.get_list_of_analyzer())

    # Test get analyzer definition
    resp = client.get_analyzer_definition("identity")
    assert resp["type"] == "identity"
    assert resp["name"] == "identity"
    assert resp["features"] == ["norm", "frequency"]

    # Test delete view
    client.delete_view("view2")
    assert "view2" not in extract("name", client.list_all_views())


def test_search_exceptions(
    client, tst_fabric_name, col, docs, bad_fabric_name, bad_col
):
    client._tenant.useFabric(bad_fabric_name)

    # Test set search in bad fabric
    with assert_raises(SearchCollectionSetError):
        client.set_search(col.name, "true", "text")

    # Test list all views in bad fabric
    with assert_raises(ViewGetError):
        client.list_all_views()

    # Test list all analyzers in bad fabric
    with assert_raises(AnalyzerListError):
        client.get_list_of_analyzer()

    # Test get analyzer definition in bad fabric
    with assert_raises(AnalyzerGetDefinitionError):
        client.get_analyzer_definition("identity")

    client._tenant.useFabric(tst_fabric_name)
    # Test search in bad collection
    with assert_raises(SearchError) as err:
        client.search_in_collection(
            collection="",
            search="FILTER doc.text == @input RETURN doc",
            bindVars={"input": "foo"},
            ttl=30,
        )
    assert err.value.http_code == 400

    # Test create view with bad properties
    links = {}
    links[col.name] = {
        "analyzers": [""],
        "fields": {"_key": {}},
        "includeAllFields": False,
        "storeValues": "none",
        "trackListPositions": False,
    }

    primary_sort = [{"field": "_key", "asc": True}]

    with assert_raises(ViewCreateError) as err:
        client.create_view("view1", links, primary_sort)
    assert err.value.http_code == 400

    # Tests on non existing view

    links[col.name] = {
        "analyzers": ["identity"],
        "fields": {"val": {}},
        "includeAllFields": False,
        "storeValues": "none",
        "trackListPositions": False,
    }

    properties = {"name": "view1", "primarySort": [], "links": links, "type": "search"}

    with assert_raises(ViewUpdatePropertiesError) as err:
        client.update_view_properties("view1", properties)
    assert err.value.http_code == 404

    with assert_raises(ViewGetPropertiesError) as err:
        client.get_view_properties("view1")
    assert err.value.http_code == 404

    with assert_raises(ViewGetError) as err:
        client.get_view_info("view1")
    assert err.value.http_code == 404

    with assert_raises(ViewRenameError) as err:
        client.rename_view("view1", "view2")
    assert err.value.http_code == 404

    # Test get analyzer definition of invalid analyzer
    with assert_raises(AnalyzerGetDefinitionError) as err:
        client.get_analyzer_definition("view1")
    assert err.value.http_code == 404

    # Test delete invalid view
    with assert_raises(ViewDeleteError) as err:
        client.delete_view("view1")
    assert err.value.http_code == 404
