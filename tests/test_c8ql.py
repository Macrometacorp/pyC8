from __future__ import absolute_import, unicode_literals

import threading
import time

from c8.exceptions import (
    C8QLQueryClearError,
    C8QLQueryExecuteError,
    C8QLQueryExplainError,
    C8QLQueryKillError,
    C8QLQueryListError,
    C8QLQueryValidateError,
)
from tests.helpers import assert_raises, extract


def test_export_data_query_invalid_operations(client, col, tst_fabric_name):
    client._tenant.useFabric(tst_fabric_name)
    # Testing invalid operation REMOVE
    with assert_raises(C8QLQueryExecuteError) as err:
        client.export_data_query(query='REMOVE "1" IN {}'.format(col.name))
    assert err.value.error_code == 10

    # Testing invalid operation UPDATE
    with assert_raises(C8QLQueryExecuteError) as err:
        client.export_data_query(
            query="UPDATE @id WITH {alive: false} IN @@collection",
            bind_vars={"@collection": col.name, "id": 2},
        )
    assert err.value.error_code == 10

    # Testing invalid operation INSERT
    with assert_raises(C8QLQueryExecuteError) as err:
        client.export_data_query(
            query="INSERT @value INTO @@collection",
            bind_vars={"@collection": col.name, "value": {"value": 10}},
        )
    assert err.value.error_code == 10

    # Testing invalid operation REPLACE
    with assert_raises(C8QLQueryExecuteError) as err:
        client.export_data_query(
            query="FOR u IN @@collection REPLACE @value IN @@collection",
            bind_vars={"@collection": col.name, "value": {"value": 2, "text": "zoo "}},
        )
    assert err.value.error_code == 10

    # Testing invalid operation UPSERT
    with assert_raises(C8QLQueryExecuteError) as err:
        client.export_data_query(
            query="UPSERT @value INSERT @toInsert UPDATE @toUpsert in @@collection",
            bind_vars={
                "@collection": col.name,
                "value": {"text": "zoo "},
                "toInsert": {"_key": 2, "updatedAt": "DATE_NOW()"},
                "toUpsert": {"_key": 2, "updatedAt": "December"},
            },
        )
    assert err.value.error_code == 10


def test_export_data_query(client, docs, tst_fabric_name, col):
    client._tenant.useFabric(tst_fabric_name)
    client.insert_document(collection_name=col.name, document=docs)

    # Test export data query
    resp = client.export_data_query(
        query="FOR u IN @@collection RETURN u", bind_vars={"@collection": col.name}
    )
    assert resp["error"] is False
    assert resp["result"] == docs

    # Test export data query with filter
    resp = client.export_data_query(
        query="FOR u IN @@collection FILTER u.val > @value RETURN u",
        bind_vars={"@collection": col.name, "value": 1},
    )
    assert resp["error"] is False
    assert len(resp["result"]) == (len(docs) - 1)

    # Test export data query without bind_vars
    resp = client.export_data_query(query="FOR u IN {} RETURN u".format(col.name))
    assert resp["error"] is False
    assert resp["result"] == docs


def test_c8ql_attributes(client, tst_fabric_name):
    tst_fabric = client._tenant.useFabric(tst_fabric_name)
    assert tst_fabric.context in ["default", "async", "batch", "transaction"]
    assert tst_fabric.tenant_name == client._tenant.name
    assert tst_fabric.fabric_name == tst_fabric.name
    assert repr(tst_fabric.c8ql) == "<C8QL in {}>".format(tst_fabric_name)


def test_c8ql_query_management(client, tst_fabric_name, bad_fabric_name, col, docs):
    tst_fabric = client._tenant.useFabric(tst_fabric_name)
    plan_fields = [
        "estimatedNrItems",
        "estimatedCost",
        "rules",
        "variables",
        "collections",
    ]
    # Test explain invalid query
    with assert_raises(C8QLQueryExplainError) as err:
        tst_fabric.c8ql.explain("INVALID QUERY")
    assert err.value.error_code == 1501

    # Test explain valid query with all_plans set to False
    plan = tst_fabric.c8ql.explain(
        "FOR d IN {} RETURN d".format(col.name),
        all_plans=False,
        opt_rules=["-all", "+use-index-range"],
    )
    assert all(field in plan for field in plan_fields)

    # Test explain valid query with all_plans set to True
    plans = tst_fabric.c8ql.explain(
        "FOR d IN {} RETURN d".format(col.name),
        all_plans=True,
        opt_rules=["-all", "+use-index-range"],
        max_plans=10,
    )
    for plan in plans:
        assert all(field in plan for field in plan_fields)
    assert len(plans) < 10

    # Test validate invalid query
    with assert_raises(C8QLQueryValidateError) as err:
        tst_fabric.c8ql.validate("INVALID QUERY")
    assert err.value.error_code == 1501

    # Test validate valid query
    result = tst_fabric.c8ql.validate("FOR d IN {} RETURN d".format(col.name))
    assert "ast" in result
    assert "bind_vars" in result
    assert "collections" in result
    assert "parsed" in result

    # Test execute invalid C8QL query
    with assert_raises(C8QLQueryExecuteError) as err:
        tst_fabric.c8ql.execute("INVALID QUERY")
    assert err.value.error_code == 1501

    # Test execute valid query
    tst_fabric.collection(col.name).import_bulk(docs)
    # Test for sql
    cursor = tst_fabric.c8ql.execute(f"SELECT * FROM {col.name}", sql=True)
    doc_response = [doc for doc in cursor]

    entries = ("_id", "_rev")
    for doc in doc_response:
        for key in entries:
            if key in doc:
                del doc[key]

    assert doc_response == docs

    # Test for c8ql
    cursor = tst_fabric.c8ql.execute(
        """
        FOR d IN {col}
            UPDATE {{_key: d._key, _val: @val }} IN {col}
            RETURN NEW
        """.format(
            col=col.name
        ),
        count=True,
        batch_size=1,
        ttl=10,
        bind_vars={"val": 42},
        full_count=True,
        optimizer_rules=["+all"],
        fail_on_warning=False,
        profile=True,
        max_transaction_size=100000,
        max_warning_count=10,
        skip_inaccessible_collections=False,
    )
    if tst_fabric.context == "transaction":
        assert cursor.id is None
        assert cursor.type == "cursor"
        assert cursor.batch() is not None
        assert cursor.has_more() is False
        assert cursor.count() == len(col)
        assert cursor.cached() is None
        assert cursor.profile() is None
        assert cursor.warnings() is None
        assert extract("_key", cursor) == extract("_key", docs)
        assert cursor.close() is None
    else:
        assert cursor.id is not None
        assert cursor.type == "cursor"
        assert cursor.batch() is not None
        assert cursor.has_more() is True
        assert cursor.count() == len(col)
        assert cursor.cached() is False
        assert cursor.profile() is not None
        assert cursor.warnings() == []
        assert extract("_key", cursor) == extract("_key", docs)
        assert cursor.close(ignore_missing=True) is False

    # Kick off some long lasting queries in the background
    def query():
        with assert_raises(C8QLQueryExecuteError) as err:
            tst_fabric.c8ql.execute("RETURN SLEEP(50)")
        assert err.value.error_code == 1500

    def kill_query():
        time.sleep(2)
        # Test list queries
        queries = tst_fabric.c8ql.queries()
        for query in queries:
            assert "id" in query
            assert "query" in query
            assert "started" in query
            assert "state" in query
            assert "bind_vars" in query
            assert "runtime" in query
        assert len(queries) == 2

        # Test kill queries
        query_id_1, query_id_2 = extract("id", queries)
        assert tst_fabric.c8ql.kill(query_id_1) is True
        while len(queries) > 1:
            queries = tst_fabric.c8ql.queries()
        assert query_id_1 not in extract("id", queries)

        assert tst_fabric.c8ql.kill(query_id_2) is True
        while len(queries) > 0:
            queries = tst_fabric.c8ql.queries()
        assert query_id_2 not in extract("id", queries)

        # Test kill missing queries
        with assert_raises(C8QLQueryKillError) as err:
            tst_fabric.c8ql.kill(query_id_1)
        assert err.value.error_code == 1591
        with assert_raises(C8QLQueryKillError) as err:
            tst_fabric.c8ql.kill(query_id_2)
        assert err.value.error_code == 1591

    def run_queries():
        t1 = threading.Thread(target=query)
        t2 = threading.Thread(target=query)
        t3 = threading.Thread(target=kill_query)
        t1.start(), t2.start(), t3.start()
        t1.join(), t2.join(), t3.join()

    run_queries()

    # Test list slow queries
    assert tst_fabric.c8ql.slow_queries() == []

    # Test clear slow queries
    assert tst_fabric.c8ql.clear_slow_queries() is True

    bad_fabric = client._tenant.useFabric(bad_fabric_name)
    # Test list queries with bad fabric
    with assert_raises(C8QLQueryListError):
        bad_fabric.c8ql.queries()

    # Test list slow queries with bad fabric
    with assert_raises(C8QLQueryListError):
        bad_fabric.c8ql.slow_queries()

    # Test clear slow queries with bad fabric
    with assert_raises(C8QLQueryClearError):
        bad_fabric.c8ql.clear_slow_queries()
