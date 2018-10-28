from __future__ import absolute_import, unicode_literals

from c8.exceptions import (
    C8QLCacheClearError,
    C8QLCacheConfigureError,
    C8QLCachePropertiesError,
    C8QLFunctionCreateError,
    C8QLFunctionDeleteError,
    C8QLFunctionListError,
    C8QLQueryClearError,
    C8QLQueryExecuteError,
    C8QLQueryExplainError,
    C8QLQueryListError,
    C8QLQueryKillError,
    C8QLQueryValidateError,
)
from tests.helpers import assert_raises, extract


def test_c8ql_attributes(db, username):
    assert db.context in ['default', 'async', 'batch', 'transaction']
    assert db.username == username
    assert db.db_name == db.name
    assert repr(db.c8ql) == '<C8QL in {}>'.format(db.name)


def test_c8ql_query_management(db, bad_db, col, docs):
    plan_fields = [
        'estimatedNrItems',
        'estimatedCost',
        'rules',
        'variables',
        'collections',
    ]
    # Test explain invalid query
    with assert_raises(C8QLQueryExplainError) as err:
        db.c8ql.explain('INVALID QUERY')
    assert err.value.error_code == 1501

    # Test explain valid query with all_plans set to False
    plan = db.c8ql.explain(
        'FOR d IN {} RETURN d'.format(col.name),
        all_plans=False,
        opt_rules=['-all', '+use-index-range']
    )
    assert all(field in plan for field in plan_fields)

    # Test explain valid query with all_plans set to True
    plans = db.c8ql.explain(
        'FOR d IN {} RETURN d'.format(col.name),
        all_plans=True,
        opt_rules=['-all', '+use-index-range'],
        max_plans=10
    )
    for plan in plans:
        assert all(field in plan for field in plan_fields)
    assert len(plans) < 10

    # Test validate invalid query
    with assert_raises(C8QLQueryValidateError) as err:
        db.c8ql.validate('INVALID QUERY')
    assert err.value.error_code == 1501

    # Test validate valid query
    result = db.c8ql.validate('FOR d IN {} RETURN d'.format(col.name))
    assert 'ast' in result
    assert 'bind_vars' in result
    assert 'collections' in result
    assert 'parsed' in result

    # Test execute invalid C8QL query
    with assert_raises(C8QLQueryExecuteError) as err:
        db.c8ql.execute('INVALID QUERY')
    assert err.value.error_code == 1501

    # Test execute valid query
    db.collection(col.name).import_bulk(docs)
    cursor = db.c8ql.execute(
        '''
        FOR d IN {col}
            UPDATE {{_key: d._key, _val: @val }} IN {col}
            RETURN NEW
        '''.format(col=col.name),
        count=True,
        batch_size=1,
        ttl=10,
        bind_vars={'val': 42},
        full_count=True,
        max_plans=1000,
        optimizer_rules=['+all'],
        cache=True,
        memory_limit=1000000,
        fail_on_warning=False,
        profile=True,
        max_transaction_size=100000,
        max_warning_count=10,
        intermediate_commit_count=1,
        intermediate_commit_size=1000,
        satellite_sync_wait=False,
        write_collections=[col.name],
        read_collections=[col.name]
    )
    if db.context == 'transaction':
        assert cursor.id is None
        assert cursor.type == 'cursor'
        assert cursor.batch() is not None
        assert cursor.has_more() is False
        assert cursor.count() == len(col)
        assert cursor.cached() is None
        assert cursor.profile() is None
        assert cursor.warnings() is None
        assert extract('_key', cursor) == extract('_key', docs)
        assert cursor.close() is None
    else:
        assert cursor.id is not None
        assert cursor.type == 'cursor'
        assert cursor.batch() is not None
        assert cursor.has_more() is True
        assert cursor.count() == len(col)
        assert cursor.cached() is False
        assert cursor.profile() is not None
        assert cursor.warnings() == []
        assert extract('_key', cursor) == extract('_key', docs)
        assert cursor.close(ignore_missing=True) is False

    # Kick off some long lasting queries in the background
    db.begin_async_execution().c8ql.execute('RETURN SLEEP(100)')
    db.begin_async_execution().c8ql.execute('RETURN SLEEP(50)')

    # Test list queries
    queries = db.c8ql.queries()
    for query in queries:
        assert 'id' in query
        assert 'query' in query
        assert 'started' in query
        assert 'state' in query
        assert 'bind_vars' in query
        assert 'runtime' in query
    assert len(queries) == 2

    # Test list queries with bad fabric
    with assert_raises(C8QLQueryListError) as err:
        bad_db.c8ql.queries()
    assert err.value.error_code == 1228

    # Test kill queries
    query_id_1, query_id_2 = extract('id', queries)
    assert db.c8ql.kill(query_id_1) is True

    while len(queries) > 1:
        queries = db.c8ql.queries()
    assert query_id_1 not in extract('id', queries)

    assert db.c8ql.kill(query_id_2) is True
    while len(queries) > 0:
        queries = db.c8ql.queries()
    assert query_id_2 not in extract('id', queries)

    # Test kill missing queries
    with assert_raises(C8QLQueryKillError) as err:
        db.c8ql.kill(query_id_1)
    assert err.value.error_code == 1591
    with assert_raises(C8QLQueryKillError) as err:
        db.c8ql.kill(query_id_2)
    assert err.value.error_code == 1591

    # Test list slow queries
    assert db.c8ql.slow_queries() == []

    # Test list slow queries with bad fabric
    with assert_raises(C8QLQueryListError) as err:
        bad_db.c8ql.slow_queries()
    assert err.value.error_code == 1228

    # Test clear slow queries
    assert db.c8ql.clear_slow_queries() is True

    # Test clear slow queries with bad fabric
    with assert_raises(C8QLQueryClearError) as err:
        bad_db.c8ql.clear_slow_queries()
    assert err.value.error_code == 1228


def test_c8ql_function_management(db, bad_db):
    fn_group = 'functions::temperature'
    fn_name_1 = 'functions::temperature::celsius_to_fahrenheit'
    fn_body_1 = 'function (celsius) { return celsius * 1.8 + 32; }'
    fn_name_2 = 'functions::temperature::fahrenheit_to_celsius'
    fn_body_2 = 'function (fahrenheit) { return (fahrenheit - 32) / 1.8; }'
    bad_fn_name = 'functions::temperature::should_not_exist'
    bad_fn_body = 'function (celsius) { invalid syntax }'

    # Test list C8QL functions with bad fabric
    with assert_raises(C8QLFunctionListError) as err:
        bad_db.c8ql.functions()
    assert err.value.error_code == 1228

    # Test create invalid C8QL function
    with assert_raises(C8QLFunctionCreateError) as err:
        db.c8ql.create_function(bad_fn_name, bad_fn_body)
    assert err.value.error_code == 1581

    # Test create C8QL function one
    db.c8ql.create_function(fn_name_1, fn_body_1)
    assert db.c8ql.functions() == {fn_name_1: fn_body_1}

    # Test create C8QL function one again (idempotency)
    db.c8ql.create_function(fn_name_1, fn_body_1)
    assert db.c8ql.functions() == {fn_name_1: fn_body_1}

    # Test create C8QL function two
    db.c8ql.create_function(fn_name_2, fn_body_2)
    assert db.c8ql.functions() == {fn_name_1: fn_body_1, fn_name_2: fn_body_2}

    # Test delete C8QL function one
    assert db.c8ql.delete_function(fn_name_1) is True
    assert db.c8ql.functions() == {fn_name_2: fn_body_2}

    # Test delete missing C8QL function
    with assert_raises(C8QLFunctionDeleteError) as err:
        db.c8ql.delete_function(fn_name_1)
    assert err.value.error_code == 1582
    assert db.c8ql.delete_function(fn_name_1, ignore_missing=True) is False
    assert db.c8ql.functions() == {fn_name_2: fn_body_2}

    # Test delete C8QL function group
    assert db.c8ql.delete_function(fn_group, group=True) is True
    assert db.c8ql.functions() == {}


def test_c8ql_cache_management(db, bad_db):
    # Test get C8QL cache properties
    properties = db.c8ql.cache.properties()
    assert 'mode' in properties
    assert 'limit' in properties

    # Test get C8QL cache properties with bad fabric
    with assert_raises(C8QLCachePropertiesError):
        bad_db.c8ql.cache.properties()

    # Test get C8QL cache configure properties
    properties = db.c8ql.cache.configure(mode='on', limit=100)
    assert properties['mode'] == 'on'
    assert properties['limit'] == 100

    properties = db.c8ql.cache.properties()
    assert properties['mode'] == 'on'
    assert properties['limit'] == 100

    # Test get C8QL cache configure properties with bad fabric
    with assert_raises(C8QLCacheConfigureError):
        bad_db.c8ql.cache.configure(mode='on')

    # Test get C8QL cache clear
    result = db.c8ql.cache.clear()
    assert isinstance(result, bool)

    # Test get C8QL cache clear with bad fabric
    with assert_raises(C8QLCacheClearError) as err:
        bad_db.c8ql.cache.clear()
    assert err.value.error_code == 1228
