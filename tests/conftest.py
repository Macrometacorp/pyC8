from __future__ import absolute_import, unicode_literals, division

import pytest

from c8 import C8Client
from c8.database import StandardDatabase
from tests.executors import (
    TestAsyncExecutor,
    TestBatchExecutor,
    TestTransactionExecutor
)
from tests.helpers import (
    generate_db_name,
    generate_col_name,
    generate_string,
    generate_username,
    generate_graph_name,
)

global_data = dict()


def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='127.0.0.1')
    parser.addoption('--port', action='store', default='8529')
    parser.addoption('--passwd', action='store', default='passwd')
    parser.addoption("--complete", action="store_true")


# noinspection PyShadowingNames
def pytest_configure(config):
    client = C8Client(
        host=config.getoption('host'),
        port=config.getoption('port')
    )
    sys_db = client.db(
        name='_system',
        username='root',
        password=config.getoption('passwd')
    )

    # Create a user and non-system database for testing.
    username = generate_username()
    password = generate_string()
    tst_db_name = generate_db_name()
    bad_db_name = generate_db_name()
    sys_db.create_database(
        name=tst_db_name,
        users=[{
            'active': True,
            'username': username,
            'password': password,
        }]
    )
    tst_db = client.db(tst_db_name, username, password)
    bad_db = client.db(bad_db_name, username, password)

    # Create a standard collection for testing.
    col_name = generate_col_name()
    tst_col = tst_db.create_collection(col_name, edge=False)
    tst_col.add_skiplist_index(['val'])
    tst_col.add_fulltext_index(['text'])
    geo_index = tst_col.add_geo_index(['loc'])

    # Create a legacy edge collection for testing.
    lecol_name = generate_col_name()
    tst_db.create_collection(lecol_name, edge=True)

    # Create test vertex & edge collections and graph.
    graph_name = generate_graph_name()
    ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    tst_graph = tst_db.create_graph(graph_name)
    tst_graph.create_vertex_collection(fvcol_name)
    tst_graph.create_vertex_collection(tvcol_name)
    tst_graph.create_edge_definition(
        edge_collection=ecol_name,
        from_vertex_collections=[fvcol_name],
        to_vertex_collections=[tvcol_name]
    )

    global_data.update({
        'client': client,
        'username': username,
        'password': password,
        'sys_db': sys_db,
        'tst_db': tst_db,
        'bad_db': bad_db,
        'geo_index': geo_index,
        'col_name': col_name,
        'lecol_name': lecol_name,
        'graph_name': graph_name,
        'ecol_name': ecol_name,
        'fvcol_name': fvcol_name,
        'tvcol_name': tvcol_name,
    })


# noinspection PyShadowingNames
def pytest_unconfigure(*_):  # pragma: no cover
    sys_db = global_data['sys_db']

    # Remove all test async jobs.
    sys_db.clear_async_jobs()

    # Remove all test tasks.
    for task in sys_db.tasks():
        task_name = task['name']
        if task_name.startswith('test_task'):
            sys_db.delete_task(task_name, ignore_missing=True)

    # Remove all test users.
    for user in sys_db.users():
        username = user['username']
        if username.startswith('test_user'):
            sys_db.delete_user(username, ignore_missing=True)

    # Remove all test databases.
    for db_name in sys_db.databases():
        if db_name.startswith('test_database'):
            sys_db.delete_database(db_name, ignore_missing=True)

    # Remove all test collections.
    for collection in sys_db.collections():
        col_name = collection['name']
        if col_name.startswith('test_collection'):
            sys_db.delete_collection(col_name, ignore_missing=True)


# noinspection PyProtectedMember
def pytest_generate_tests(metafunc):
    tst_db = global_data['tst_db']
    bad_db = global_data['bad_db']

    tst_dbs = [tst_db]
    bad_dbs = [bad_db]

    if metafunc.config.getoption('complete'):
        tst = metafunc.module.__name__.split('.test_', 1)[-1]
        tst_conn = tst_db._conn
        bad_conn = bad_db._conn

        if tst in {'collection', 'document', 'graph', 'c8ql', 'index'}:
            # Add test transaction databases
            tst_txn_db = StandardDatabase(tst_conn)
            tst_txn_db._executor = TestTransactionExecutor(tst_conn)
            tst_txn_db._is_transaction = True
            tst_dbs.append(tst_txn_db)

            bad_txn_db = StandardDatabase(bad_conn)
            bad_txn_db._executor = TestTransactionExecutor(bad_conn)
            bad_dbs.append(bad_txn_db)

        if tst not in {'async', 'batch', 'transaction', 'client', 'exception'}:
            # Add test async databases
            tst_async_db = StandardDatabase(tst_conn)
            tst_async_db._executor = TestAsyncExecutor(tst_conn)
            tst_dbs.append(tst_async_db)

            bad_async_db = StandardDatabase(bad_conn)
            bad_async_db._executor = TestAsyncExecutor(bad_conn)
            bad_dbs.append(bad_async_db)

            # Add test batch databases
            tst_batch_db = StandardDatabase(tst_conn)
            tst_batch_db._executor = TestBatchExecutor(tst_conn)
            tst_dbs.append(tst_batch_db)

            bad_batch_bdb = StandardDatabase(bad_conn)
            bad_batch_bdb._executor = TestBatchExecutor(bad_conn)
            bad_dbs.append(bad_batch_bdb)

    if 'db' in metafunc.fixturenames and 'bad_db' in metafunc.fixturenames:
        metafunc.parametrize('db,bad_db', zip(tst_dbs, bad_dbs))

    elif 'db' in metafunc.fixturenames:
        metafunc.parametrize('db', tst_dbs)

    elif 'bad_db' in metafunc.fixturenames:
        metafunc.parametrize('bad_db', bad_dbs)


@pytest.fixture(autouse=False)
def client():
    return global_data['client']


@pytest.fixture(autouse=False)
def sys_db():
    return global_data['sys_db']


@pytest.fixture(autouse=False)
def username():
    return global_data['username']


@pytest.fixture(autouse=False)
def password():
    return global_data['password']


@pytest.fixture(autouse=False)
def col(db):
    collection = db.collection(global_data['col_name'])
    collection.truncate()
    return collection


@pytest.fixture(autouse=False)
def bad_col(bad_db):
    return bad_db.collection(global_data['col_name'])


@pytest.fixture(autouse=False)
def geo():
    return global_data['geo_index']


@pytest.fixture(autouse=False)
def lecol(db):
    collection = db.collection(global_data['lecol_name'])
    collection.truncate()
    return collection


@pytest.fixture(autouse=False)
def graph(db):
    return db.graph(global_data['graph_name'])


@pytest.fixture(autouse=False)
def bad_graph(bad_db):
    return bad_db.graph(global_data['graph_name'])


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
def fvcol(graph):
    collection = graph.vertex_collection(global_data['fvcol_name'])
    collection.truncate()
    return collection


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
def tvcol(graph):
    collection = graph.vertex_collection(global_data['tvcol_name'])
    collection.truncate()
    return collection


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
def bad_fvcol(bad_graph):
    return bad_graph.vertex_collection(global_data['fvcol_name'])


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
def ecol(graph):
    collection = graph.edge_collection(global_data['ecol_name'])
    collection.truncate()
    return collection


# noinspection PyShadowingNames
@pytest.fixture(autouse=False)
def bad_ecol(bad_graph):
    return bad_graph.edge_collection(global_data['ecol_name'])


@pytest.fixture(autouse=False)
def docs():
    return [
        {'_key': '1', 'val': 1, 'text': 'foo', 'loc': [1, 1]},
        {'_key': '2', 'val': 2, 'text': 'foo', 'loc': [2, 2]},
        {'_key': '3', 'val': 3, 'text': 'foo', 'loc': [3, 3]},
        {'_key': '4', 'val': 4, 'text': 'bar', 'loc': [4, 4]},
        {'_key': '5', 'val': 5, 'text': 'bar', 'loc': [5, 5]},
        {'_key': '6', 'val': 6, 'text': 'bar', 'loc': [5, 5]},
    ]


@pytest.fixture(autouse=False)
def fvdocs():
    return [
        {'_key': '1', 'val': 1},
        {'_key': '2', 'val': 2},
        {'_key': '3', 'val': 3},
    ]


@pytest.fixture(autouse=False)
def tvdocs():
    return [
        {'_key': '4', 'val': 4},
        {'_key': '5', 'val': 5},
        {'_key': '6', 'val': 6},
    ]


@pytest.fixture(autouse=False)
def edocs():
    fv = global_data['fvcol_name']
    tv = global_data['tvcol_name']
    return [
        {'_key': '1', '_from': '{}/1'.format(fv), '_to': '{}/4'.format(tv)},
        {'_key': '2', '_from': '{}/1'.format(fv), '_to': '{}/5'.format(tv)},
        {'_key': '3', '_from': '{}/6'.format(fv), '_to': '{}/2'.format(tv)},
        {'_key': '4', '_from': '{}/8'.format(fv), '_to': '{}/7'.format(tv)},
    ]
