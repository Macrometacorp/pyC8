from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from c8.exceptions import (
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError,
    DocumentParseError,
)
from tests.helpers import (
    assert_raises,
    clean_doc,
    extract,
    generate_doc_key,
    generate_col_name,
)


def test_document_insert(col, docs):
    # Test insert document with no key
    result = col.insert({})
    assert result['_key'] in col
    assert len(col) == 1
    col.truncate()

    # Test insert document with ID
    doc_id = col.name + '/' + 'foo'
    col.insert({'_id': doc_id})
    assert 'foo' in col
    assert doc_id in col
    assert len(col) == 1
    col.truncate()

    with assert_raises(DocumentParseError) as err:
        col.insert({'_id': generate_col_name() + '/' + 'foo'})
    assert 'bad collection name' in err.value.message

    # Test insert with default options
    for doc in docs:
        result = col.insert(doc)
        assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
        assert result['_key'] == doc['_key']
        assert isinstance(result['_rev'], string_types)
        assert col[doc['_key']]['val'] == doc['val']
    assert len(col) == len(docs)
    col.truncate()

    # Test insert with sync set to True
    doc = docs[0]
    result = col.insert(doc, sync=True)
    assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
    assert result['_key'] == doc['_key']
    assert isinstance(result['_rev'], string_types)
    assert col[doc['_key']]['_key'] == doc['_key']
    assert col[doc['_key']]['val'] == doc['val']

    # Test insert with sync set to False
    doc = docs[1]
    result = col.insert(doc, sync=False)
    assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
    assert result['_key'] == doc['_key']
    assert isinstance(result['_rev'], string_types)
    assert col[doc['_key']]['_key'] == doc['_key']
    assert col[doc['_key']]['val'] == doc['val']

    # Test insert with return_new set to True
    doc = docs[2]
    result = col.insert(doc, return_new=True)
    assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
    assert result['_key'] == doc['_key']
    assert isinstance(result['_rev'], string_types)
    assert result['new']['_id'] == result['_id']
    assert result['new']['_key'] == result['_key']
    assert result['new']['_rev'] == result['_rev']
    assert result['new']['val'] == doc['val']
    assert col[doc['_key']]['_key'] == doc['_key']
    assert col[doc['_key']]['val'] == doc['val']

    # Test insert with return_new set to False
    doc = docs[3]
    result = col.insert(doc, return_new=False)
    assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
    assert result['_key'] == doc['_key']
    assert isinstance(result['_rev'], string_types)
    assert 'new' not in result
    assert col[doc['_key']]['_key'] == doc['_key']
    assert col[doc['_key']]['val'] == doc['val']

    # Test insert with silent set to True
    doc = docs[4]
    assert col.insert(doc, silent=True) is True
    assert col[doc['_key']]['_key'] == doc['_key']
    assert col[doc['_key']]['val'] == doc['val']

    # Test insert duplicate document
    with assert_raises(DocumentInsertError) as err:
        col.insert(doc)
    assert err.value.error_code == 1210


def test_document_insert_many(col, docs):
    # Test insert_many with default options
    results = col.insert_many(docs)
    for result, doc in zip(results, docs):
        assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
        assert result['_key'] == doc['_key']
        assert isinstance(result['_rev'], string_types)
        assert col[doc['_key']]['val'] == doc['val']
    assert len(col) == len(docs)
    col.truncate()

    # Test insert_many with document IDs
    docs_with_id = [{'_id': col.name + '/' + doc['_key']} for doc in docs]
    results = col.insert_many(docs_with_id)
    for result, doc in zip(results, docs):
        assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
        assert result['_key'] == doc['_key']
        assert isinstance(result['_rev'], string_types)
    assert len(col) == len(docs)
    col.truncate()

    # Test insert_many with sync set to True
    results = col.insert_many(docs, sync=True)
    for result, doc in zip(results, docs):
        assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
        assert result['_key'] == doc['_key']
        assert isinstance(result['_rev'], string_types)
        assert col[doc['_key']]['_key'] == doc['_key']
        assert col[doc['_key']]['val'] == doc['val']
    col.truncate()

    # Test insert_many with sync set to False
    results = col.insert_many(docs, sync=False)
    for result, doc in zip(results, docs):
        assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
        assert result['_key'] == doc['_key']
        assert isinstance(result['_rev'], string_types)
        assert col[doc['_key']]['_key'] == doc['_key']
        assert col[doc['_key']]['val'] == doc['val']
    col.truncate()

    # Test insert_many with return_new set to True
    results = col.insert_many(docs, return_new=True)
    for result, doc in zip(results, docs):
        assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
        assert result['_key'] == doc['_key']
        assert isinstance(result['_rev'], string_types)
        assert result['new']['_id'] == result['_id']
        assert result['new']['_key'] == result['_key']
        assert result['new']['_rev'] == result['_rev']
        assert result['new']['val'] == doc['val']
        assert col[doc['_key']]['_key'] == doc['_key']
        assert col[doc['_key']]['val'] == doc['val']
    col.truncate()

    # Test insert_many with return_new set to False
    results = col.insert_many(docs, return_new=False)
    for result, doc in zip(results, docs):
        assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
        assert result['_key'] == doc['_key']
        assert isinstance(result['_rev'], string_types)
        assert 'new' not in result
        assert col[doc['_key']]['_key'] == doc['_key']
        assert col[doc['_key']]['val'] == doc['val']
    col.truncate()

    # Test insert_many with silent set to True
    assert col.insert_many(docs, silent=True) is True
    for doc in docs:
        assert col[doc['_key']]['_key'] == doc['_key']
        assert col[doc['_key']]['val'] == doc['val']

    # Test insert_many duplicate documents
    results = col.insert_many(docs, return_new=False)
    for result, doc in zip(results, docs):
        isinstance(result, DocumentInsertError)


def test_document_update(col, docs):
    doc = docs[0]
    col.insert(doc)

    # Test update with default options
    doc['val'] = {'foo': 1}
    doc = col.update(doc)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert col[doc['_key']]['val'] == {'foo': 1}
    old_rev = doc['_rev']

    # Test update with merge set to True
    doc['val'] = {'bar': 2}
    doc = col.update(doc, merge=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['val'] == {'foo': 1, 'bar': 2}
    old_rev = doc['_rev']

    # Test update with merge set to False
    doc['val'] = {'baz': 3}
    doc = col.update(doc, merge=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['val'] == {'baz': 3}
    old_rev = doc['_rev']

    # Test update with keep_none set to True
    doc['val'] = None
    doc = col.update(doc, keep_none=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['val'] is None
    old_rev = doc['_rev']

    # Test update with keep_none set to False
    doc['val'] = None
    doc = col.update(doc, keep_none=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert 'val' not in col[doc['_key']]
    old_rev = doc['_rev']

    # Test update with return_new and return_old set to True
    doc['val'] = 3
    doc = col.update(doc, return_new=True, return_old=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert doc['new']['_key'] == doc['_key']
    assert doc['new']['val'] == 3
    assert doc['old']['_key'] == doc['_key']
    assert 'val' not in doc['old']
    assert col[doc['_key']]['val'] == 3
    old_rev = doc['_rev']

    # Test update with return_new and return_old set to False
    doc['val'] = 4
    doc = col.update(doc, return_new=False, return_old=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert 'new' not in doc
    assert 'old' not in doc
    assert col[doc['_key']]['val'] == 4
    old_rev = doc['_rev']

    # Test update with check_rev set to True
    doc['val'] = 5
    doc['_rev'] = old_rev + '0'
    with assert_raises(DocumentRevisionError) as err:
        col.update(doc, check_rev=True)
    assert err.value.error_code == 1200
    assert col[doc['_key']]['val'] == 4

    # Test update with check_rev set to False
    doc = col.update(doc, check_rev=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['val'] == 5
    old_rev = doc['_rev']

    # Test update with sync set to True
    doc['val'] = 6
    doc = col.update(doc, sync=True, check_rev=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['val'] == 6
    old_rev = doc['_rev']

    # Test update with sync set to False
    doc['val'] = 7
    doc = col.update(doc, sync=False, check_rev=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['val'] == 7
    old_rev = doc['_rev']

    # Test update missing document
    missing_doc = docs[1]
    with assert_raises(DocumentUpdateError) as err:
        col.update(missing_doc)
    assert err.value.error_code == 1202
    assert missing_doc['_key'] not in col
    assert col[doc['_key']]['val'] == 7
    assert col[doc['_key']]['_rev'] == old_rev

    # Test update with silent set to True
    doc['val'] = 8
    assert col.update(doc, silent=True) is True
    assert col[doc['_key']]['val'] == 8


def test_document_update_many(col, docs):
    col.insert_many(docs)

    old_revs = {}
    doc_keys = [d['_key'] for d in docs]

    # Test update_many with default options
    for doc in docs:
        doc['val'] = {'foo': 0}
    results = col.update_many(docs)
    for result, doc_key in zip(results, doc_keys):
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert col[doc_key]['val'] == {'foo': 0}
        old_revs[doc_key] = result['_rev']

    # Test update_many with IDs
    docs_with_ids = [
        {'_id': col.name + '/' + d['_key'], 'val': {'foo': 1}}
        for d in docs
    ]
    results = col.update_many(docs_with_ids)
    for result, doc_key in zip(results, doc_keys):
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert col[doc_key]['val'] == {'foo': 1}
        old_revs[doc_key] = result['_rev']

    # Test update_many with merge set to True
    for doc in docs:
        doc['val'] = {'bar': 2}
    results = col.update_many(docs, merge=True)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['val'] == {'foo': 1, 'bar': 2}
        old_revs[doc_key] = result['_rev']

    # Test update_many with merge set to False
    for doc in docs:
        doc['val'] = {'baz': 3}
    results = col.update_many(docs, merge=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['val'] == {'baz': 3}
        old_revs[doc_key] = result['_rev']

    # Test update_many with keep_none set to True
    for doc in docs:
        doc['val'] = None
    results = col.update_many(docs, keep_none=True)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['val'] is None
        old_revs[doc_key] = result['_rev']

    # Test update_many with keep_none set to False
    for doc in docs:
        doc['val'] = None
    results = col.update_many(docs, keep_none=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert 'val' not in col[doc_key]
        old_revs[doc_key] = result['_rev']

    # Test update_many with return_new and return_old set to True
    for doc in docs:
        doc['val'] = 3
    results = col.update_many(docs, return_new=True, return_old=True)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert result['new']['_key'] == doc_key
        assert result['new']['val'] == 3
        assert result['old']['_key'] == doc_key
        assert 'val' not in result['old']
        assert col[doc_key]['val'] == 3
        old_revs[doc_key] = result['_rev']

    # Test update_many with return_new and return_old set to False
    for doc in docs:
        doc['val'] = 4
    results = col.update_many(docs, return_new=False, return_old=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert 'new' not in result
        assert 'old' not in result
        assert col[doc_key]['val'] == 4
        old_revs[doc_key] = result['_rev']

    # Test update_many with check_rev set to True
    for doc in docs:
        doc['val'] = 5
        doc['_rev'] = old_revs[doc['_key']] + '0'
    results = col.update_many(docs, check_rev=True)
    for result, doc_key in zip(results, doc_keys):
        assert isinstance(result, DocumentRevisionError)
    for x in range(col.count()):
        doc = col.get({'_key': str(x + 1)})
        assert doc['val'] == 4

    # Test update_many with check_rev set to False
    results = col.update_many(docs, check_rev=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['val'] == 5
        old_revs[doc_key] = result['_rev']

    # Test update_many with sync set to True
    for doc in docs:
        doc['val'] = 6
    results = col.update_many(docs, sync=True, check_rev=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['val'] == 6
        old_revs[doc_key] = result['_rev']

    # Test update_many with sync set to False
    for doc in docs:
        doc['val'] = 7
    results = col.update_many(docs, sync=False, check_rev=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['val'] == 7
        old_revs[doc_key] = result['_rev']

    # Test update_many with non-existing doc
    resp = col.update_many({'_key': '123'})
    assert str(resp) == "[DocumentUpdateError('[HTTP 202][ERR 1227] invalid document type')]"
    # Test update_many with silent set to True
    for doc in docs:
        doc['val'] = 8
    assert col.update_many(docs, silent=True, check_rev=False) is True
    for doc in docs:
        assert col[doc['_key']]['val'] == 8

    # Test update_many with bad documents
    with assert_raises(DocumentParseError) as err:
        col.update_many([{}])
    assert str(err.value) == 'field "_key" or "_id" required'


def test_document_replace(col, docs):
    doc = docs[0]
    col.insert(doc)

    # Test replace with default options
    doc['foo'] = 2
    doc.pop('val')
    doc = col.replace(doc)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert col[doc['_key']]['foo'] == 2
    assert 'val' not in col[doc['_key']]
    old_rev = doc['_rev']

    # Test update with return_new and return_old set to True
    doc['bar'] = 3
    doc = col.replace(doc, return_new=True, return_old=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert doc['new']['_key'] == doc['_key']
    assert doc['new']['bar'] == 3
    assert 'foo' not in doc['new']
    assert doc['old']['_key'] == doc['_key']
    assert doc['old']['foo'] == 2
    assert 'bar' not in doc['old']
    assert col[doc['_key']]['bar'] == 3
    assert 'foo' not in col[doc['_key']]
    old_rev = doc['_rev']

    # Test update with return_new and return_old set to False
    doc['baz'] = 4
    doc = col.replace(doc, return_new=False, return_old=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert 'new' not in doc
    assert 'old' not in doc
    assert col[doc['_key']]['baz'] == 4
    assert 'bar' not in col[doc['_key']]
    old_rev = doc['_rev']

    # Test replace with check_rev set to True
    doc['foo'] = 5
    doc['_rev'] = old_rev + '0'
    with assert_raises(DocumentRevisionError):
        col.replace(doc, check_rev=True)
    assert 'foo' not in col[doc['_key']]
    assert col[doc['_key']]['baz'] == 4

    # Test replace with check_rev set to False
    doc = col.replace(doc, check_rev=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['foo'] == 5
    old_rev = doc['_rev']

    # Test replace with sync set to True
    doc['foo'] = 6
    doc = col.replace(doc, sync=True, check_rev=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['foo'] == 6
    assert 'baz' not in col[doc['_key']]
    old_rev = doc['_rev']

    # Test replace with sync set to False
    doc['bar'] = 7
    doc = col.replace(doc, sync=False, check_rev=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == doc['_key']
    assert isinstance(doc['_rev'], string_types)
    assert doc['_old_rev'] == old_rev
    assert col[doc['_key']]['bar'] == 7
    assert 'foo' not in col[doc['_key']]
    old_rev = doc['_rev']

    # Test replace missing document
    with assert_raises(DocumentReplaceError):
        col.replace(docs[1])
    assert col[doc['_key']]['bar'] == 7
    assert col[doc['_key']]['_rev'] == old_rev

    # Test replace with silent set to True
    doc['val'] = 8
    assert col.replace(doc, silent=True) is True
    assert col[doc['_key']]['val'] == 8


def test_document_replace_many(col, docs):
    col.insert_many(docs)
    old_revs = {}
    doc_keys = list(d['_key'] for d in docs)

    # Test replace_many with default options
    for doc in docs:
        doc['foo'] = 1
        doc.pop('val')
    results = col.replace_many(docs)
    for result, doc_key in zip(results, doc_keys):
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert col[doc_key]['foo'] == 1
        assert 'val' not in col[doc_key]
        old_revs[doc_key] = result['_rev']

    # Test replace_many with IDs
    docs_with_ids = [
        {'_id': col.name + '/' + d['_key'], 'foo': 2}
        for d in docs
    ]
    results = col.replace_many(docs_with_ids)
    for result, doc_key in zip(results, doc_keys):
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert col[doc_key]['foo'] == 2
        old_revs[doc_key] = result['_rev']

    # Test update with return_new and return_old set to True
    for doc in docs:
        doc['bar'] = 3
        doc.pop('foo')
    results = col.replace_many(docs, return_new=True, return_old=True)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert result['new']['_key'] == doc_key
        assert result['new']['bar'] == 3
        assert 'foo' not in result['new']
        assert result['old']['_key'] == doc_key
        assert result['old']['foo'] == 2
        assert 'bar' not in result['old']
        assert col[doc_key]['bar'] == 3
        old_revs[doc_key] = result['_rev']

    # Test update with return_new and return_old set to False
    for doc in docs:
        doc['baz'] = 4
        doc.pop('bar')
    results = col.replace_many(docs, return_new=False, return_old=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert 'new' not in result
        assert 'old' not in result
        assert col[doc_key]['baz'] == 4
        assert 'bar' not in col[doc_key]
        old_revs[doc_key] = result['_rev']

    # Test replace_many with check_rev set to True
    for doc in docs:
        doc['foo'] = 5
        doc.pop('baz')
        doc['_rev'] = old_revs[doc['_key']] + '0'
    results = col.replace_many(docs, check_rev=True)
    for result, doc_key in zip(results, doc_keys):
        assert isinstance(result, DocumentRevisionError)
    for x in range(col.count()):
        doc = col.get({'_key': str(x + 1)})
        assert 'foo' not in doc
        assert doc['baz'] == 4

    # Test replace_many with check_rev set to False
    results = col.replace_many(docs, check_rev=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['foo'] == 5
        assert 'baz' not in col[doc_key]
        old_revs[doc_key] = result['_rev']

    # Test replace_many with sync set to True
    for doc in docs:
        doc['foo'] = 6
    results = col.replace_many(docs, sync=True, check_rev=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['foo'] == 6
        old_revs[doc_key] = result['_rev']

    # Test replace_many with sync set to False
    for doc in docs:
        doc['bar'] = 7
        doc.pop('foo')
    results = col.replace_many(docs, sync=False, check_rev=False)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['_old_rev'] == old_revs[doc_key]
        assert col[doc_key]['bar'] == 7
        assert 'foo' not in col[doc_key]
        old_revs[doc_key] = result['_rev']

    resp = col.replace_many([{'_key': '1000'}])
    # Test replace_many with bad key
    assert str(resp) == "[DocumentReplaceError('[HTTP 202][ERR 1202] document not found')]"

    resp = col.replace_many([{'_key': 1}])
    # Test replace_many with illegal key
    assert str(resp) == "[DocumentReplaceError('[HTTP 202][ERR 1221] illegal document key')]"

    # Test replace_many with silent set to True
    for doc in docs:
        doc['foo'] = 8
        doc.pop('bar')
    assert col.replace_many(docs, silent=True, check_rev=False) is True
    for doc in docs:
        doc_key = doc['_key']
        assert col[doc_key]['foo'] == 8
        assert 'bar' not in col[doc_key]

    # Test replace_many with bad documents
    with assert_raises(DocumentParseError) as err:
        col.replace_many([{}])
    assert str(err.value) == 'field "_key" or "_id" required'


def test_document_delete(col, docs):
    # Set up test documents
    col.import_bulk(docs)

    # Test delete (document) with default options
    doc = docs[0]
    result = col.delete(doc)
    assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
    assert result['_key'] == doc['_key']
    assert isinstance(result['_rev'], string_types)
    assert 'old' not in result
    assert doc['_key'] not in col
    assert len(col) == 5

    # Test delete (document ID) with return_old set to True
    doc = docs[1]
    doc_id = '{}/{}'.format(col.name, doc['_key'])
    result = col.delete(doc_id, return_old=True)
    assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
    assert result['_key'] == doc['_key']
    assert isinstance(result['_rev'], string_types)
    assert result['old']['_key'] == doc['_key']
    assert result['old']['val'] == doc['val']
    assert doc['_key'] not in col
    assert len(col) == 4

    # Test delete (document doc_key) with sync set to True
    doc = docs[2]
    result = col.delete(doc, sync=True)
    assert result['_id'] == '{}/{}'.format(col.name, doc['_key'])
    assert result['_key'] == doc['_key']
    assert isinstance(result['_rev'], string_types)
    assert doc['_key'] not in col
    assert len(col) == 3

    # Test delete (document) with check_rev set to True
    doc = docs[3]
    bad_rev = col[doc['_key']]['_rev'] + '0'
    bad_doc = doc.copy()
    bad_doc.update({'_rev': bad_rev})
    with assert_raises(DocumentRevisionError):
        col.delete(bad_doc, check_rev=True)
    assert bad_doc['_key'] in col
    assert len(col) == 3

    # Test delete (document) with check_rev set to False
    doc = docs[4]
    bad_rev = col[doc['_key']]['_rev'] + '0'
    bad_doc = doc.copy()
    bad_doc.update({'_rev': bad_rev})
    col.delete(bad_doc, check_rev=False)
    assert doc['_key'] not in col
    assert len(col) == 2

    # Test delete missing document
    bad_key = generate_doc_key()
    with assert_raises(DocumentDeleteError) as err:
        col.delete(bad_key, ignore_missing=False)
    assert err.value.error_code == 1202
    assert len(col) == 2
    if col.context != 'transaction':
        assert col.delete(bad_key, ignore_missing=True) is False

    # Test delete (document) with silent set to True
    doc = docs[5]
    assert col.delete(doc, silent=True) is True
    assert doc['_key'] not in col
    assert len(col) == 1


def test_document_delete_many(col, docs, client):
    # Set up test documents
    old_revs = {}
    doc_keys = [d['_key'] for d in docs]
    doc_ids = [col.name + '/' + d['_key'] for d in docs]

    # Test delete_many (documents) with default options
    col.import_bulk(docs)
    results = col.delete_many(docs)
    for result, doc_key in zip(results, doc_keys):
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert 'old' not in result
        assert doc_key not in col
        old_revs[doc_key] = result['_rev']
    assert len(col) == 0

    # Test delete_many (documents) with IDs
    col.import_bulk(docs)
    results = col.delete_many(doc_ids)
    for result, doc_key in zip(results, doc_keys):
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert 'old' not in result
        assert doc_key not in col
        old_revs[doc_key] = result['_rev']
    assert len(col) == 0

    # Test delete_many (documents) with return_old set to True
    col.import_bulk(docs)
    results = col.delete_many(docs, return_old=True)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert result['old']['_key'] == doc_key
        assert result['old']['val'] == doc['val']
        assert doc_key not in col
        old_revs[doc_key] = result['_rev']
    assert len(col) == 0

    # Test delete_many (document doc_keys) with sync set to True
    col.import_bulk(docs)
    results = col.delete_many(docs, sync=True)
    for result, doc in zip(results, docs):
        doc_key = doc['_key']
        assert result['_id'] == '{}/{}'.format(col.name, doc_key)
        assert result['_key'] == doc_key
        assert isinstance(result['_rev'], string_types)
        assert 'old' not in result
        assert doc_key not in col
        old_revs[doc_key] = result['_rev']
    assert len(col) == 0

    # Test delete_many with silent set to True
    col.import_bulk(docs)
    assert col.delete_many(docs, silent=True) is True
    assert len(col) == 0

    # Test delete_many (documents) with check_rev set to True
    col.import_bulk(docs)
    for doc in docs:
        doc['_rev'] = old_revs[doc['_key']] + '0'
    results = col.delete_many(docs, check_rev=True)
    for result, doc in zip(results, docs):
        assert isinstance(result, DocumentRevisionError)
    assert len(col) == 6

    # Test delete_many (documents) with missing documents
    col.truncate()
    results = col.delete_many([
        {'_key': generate_doc_key()},
        {'_key': generate_doc_key()},
        {'_key': generate_doc_key()}
    ])
    for result, doc in zip(results, docs):
        assert isinstance(result, DocumentDeleteError)
    assert len(col) == 0
    resp = col.delete_many([{'_key': '1'}])
    # Test delete_many with bad key
    assert str(resp) == "[DocumentDeleteError('[HTTP 202][ERR 1202] document not found')]"


def test_document_count(col, docs):
    # Set up test documents
    col.import_bulk(docs)
    assert len(col) == len(docs)
    assert col.count() == len(docs)


def test_document_find_near(col, docs):
    col.import_bulk(docs)

    # Test find_near with default options
    result = col.find_near(latitude=1, longitude=1)
    assert extract('_key', result) == ['1', '2', '3', '4', '5', '6']

    # Test find_near with limit of -1
    with assert_raises(AssertionError) as err:
        col.find_near(latitude=1, longitude=1, limit=-1)
    assert 'limit must be a non-negative int' == str(err.value)

    # Test find_near with limit of 0
    result = col.find_near(latitude=1, longitude=1, limit=0)
    assert extract('_key', result) == []

    # Test find_near with limit of 1
    result = col.find_near(latitude=1, longitude=1, limit=1)
    assert extract('_key', result) == ['1']

    # Test find_near with limit of 3
    result = col.find_near(latitude=1, longitude=1, limit=3)
    assert extract('_key', result) == ['1', '2', '3']

    # Test find_near with limit of 3 (another set of coordinates)
    result = col.find_near(latitude=5, longitude=5, limit=3)
    assert extract('_key', result) == ['4', '5', '6']

    # Test find_near in an empty collection
    col.truncate()
    result = col.find_near(latitude=1, longitude=1, limit=1)
    assert list(result) == []
    result = col.find_near(latitude=5, longitude=5, limit=4)
    assert list(result) == []

    # Test find near with bad params
    with assert_raises(DocumentGetError) as err:
        col.find_near(latitude=360, longitude=1, limit=1)
    assert err.value.error_code == 1505


def test_document_find_in_range(col, docs):
    col.import_bulk(docs)

    # Test find_in_range with default options
    result = col.find_in_range('val', lower=1, upper=2)
    assert extract('_key', result) == ['1']

    # Test find_in_range with limit of -1
    with assert_raises(AssertionError) as err:
        col.find_in_range('val', lower=1, upper=2, limit=-1)
    assert 'limit must be a non-negative int' == str(err.value)

    # Test find_in_range with limit of 0
    result = col.find_in_range('val', lower=1, upper=2, limit=0)
    assert extract('_key', result) == []

    # Test find_in_range with limit of 3
    result = col.find_in_range('val', lower=1, upper=5, limit=3)
    assert extract('_key', result) == ['1', '2', '3']

    # Test find_in_range with skip set to -1
    with assert_raises(AssertionError) as err:
        col.find_in_range('val', lower=1, upper=2, skip=-1)
    assert 'skip must be a non-negative int' == str(err.value)

    # Test find_in_range with skip set to 0
    result = col.find_in_range('val', lower=1, upper=5, skip=0)
    assert extract('_key', result) == ['1', '2', '3', '4']

    # Test find_in_range with skip set to 3
    result = col.find_in_range('val', lower=1, upper=5, skip=2)
    assert extract('_key', result) == ['3', '4']


def test_document_find_in_radius(col):
    doc1 = {'_key': '1', 'loc': [1, 1]}
    doc2 = {'_key': '2', 'loc': [1, 4]}
    doc3 = {'_key': '3', 'loc': [4, 1]}
    doc4 = {'_key': '4', 'loc': [4, 4]}

    col.import_bulk([doc1, doc2, doc3, doc4])

    # Test find_in_radius without distance field
    result = list(col.find_in_radius(
        latitude=1,
        longitude=4,
        radius=6,
    ))
    assert len(result) == 1
    assert clean_doc(result[0]) == {'_key': '2', 'loc': [1, 4]}

    # Test find_in_radius with distance field
    result = list(col.find_in_radius(
        latitude=1,
        longitude=1,
        radius=6,
        distance_field='dist'
    ))
    assert len(result) == 1
    if col.context == 'transaction':
        assert clean_doc(result[0]) == {'_key': '1', 'loc': [1, 1]}
    else:
        assert clean_doc(result[0]) == {'_key': '1', 'loc': [1, 1], 'dist': 0}


def test_document_find_by_text(col, docs):
    col.import_bulk(docs)

    # Test find_by_text with default options
    result = col.find_by_text(field='text', query='foo,|bar')
    assert clean_doc(result) == docs

    # Test find_by_text with limit
    with assert_raises(AssertionError) as err:
        col.find_by_text(field='text', query='foo', limit=-1)
    assert 'limit must be a non-negative int' == str(err.value)

    result = col.find_by_text(field='text', query='foo', limit=0)
    assert len(list(result)) == 0

    result = col.find_by_text(field='text', query='foo', limit=1)
    assert len(list(result)) == 1

    result = col.find_by_text(field='text', query='foo', limit=2)
    assert len(list(result)) == 2

    result = col.find_by_text(field='text', query='foo', limit=3)
    assert len(list(result)) == 3

    # Test find_by_text with invalid queries
    with assert_raises(DocumentGetError):
        col.find_by_text(field='text', query='+')
    with assert_raises(DocumentGetError):
        col.find_by_text(field='text', query='|')

    # Test find_by_text with missing column
    with assert_raises(DocumentGetError) as err:
        col.find_by_text(field='missing', query='foo')
    assert err.value.error_code == 1571


def test_document_has(col, docs):
    # Set up test document
    result = col.insert(docs[0])
    rev = result['_rev']
    bad_rev = rev + '0'

    doc_key = docs[0]['_key']
    doc_id = col.name + '/' + doc_key
    missing_doc_key = docs[1]['_key']
    missing_doc_id = col.name + '/' + missing_doc_key

    # Test existing documents without revision or with good revision
    for doc_input in [
        doc_key,
        doc_id,
        {'_key': doc_key},
        {'_id': doc_id},
        {'_id': doc_id, '_key': doc_key},
        {'_key': doc_key, '_rev': rev},
        {'_id': doc_id, '_rev': rev},
        {'_id': doc_id, '_key': doc_key, '_rev': rev},
    ]:
        assert doc_input in col
        assert col.has(doc_input) is True
        assert col.has(doc_input, rev=rev) is True
        assert col.has(doc_input, rev=rev, check_rev=True) is True
        assert col.has(doc_input, rev=rev, check_rev=False) is True
        assert col.has(doc_input, rev=bad_rev, check_rev=False) is True

        with assert_raises(DocumentRevisionError) as err:
            col.has(doc_input, rev=bad_rev, check_rev=True)
        assert err.value.error_code == 1200

    # Test existing documents with bad revision
    for doc_input in [
        {'_key': doc_key, '_rev': bad_rev},
        {'_id': doc_id, '_rev': bad_rev},
        {'_id': doc_id, '_key': doc_key, '_rev': bad_rev},
    ]:
        with assert_raises(DocumentRevisionError) as err:
            col.has(doc_input)
        assert err.value.error_code == 1200

        with assert_raises(DocumentRevisionError) as err:
            col.has(doc_input, rev=bad_rev)
        assert err.value.error_code == 1200

        with assert_raises(DocumentRevisionError) as err:
            col.has(doc_input, rev=bad_rev, check_rev=True)
        assert err.value.error_code == 1200

        assert doc_input in col
        assert col.has(doc_input, rev=rev, check_rev=True) is True
        assert col.has(doc_input, rev=rev, check_rev=False) is True
        assert col.has(doc_input, rev=bad_rev, check_rev=False) is True

    # Test missing documents
    for doc_input in [
        missing_doc_key,
        missing_doc_id,
        {'_key': missing_doc_key},
        {'_id': missing_doc_id},
        {'_id': missing_doc_id, '_key': missing_doc_key},
        {'_key': missing_doc_key, '_rev': rev},
        {'_id': missing_doc_id, '_rev': rev},
        {'_id': missing_doc_id, '_key': missing_doc_key, '_rev': rev},
    ]:
        assert doc_input not in col
        assert col.has(doc_input) is False
        assert col.has(doc_input, rev=rev) is False
        assert col.has(doc_input, rev=rev, check_rev=True) is False
        assert col.has(doc_input, rev=rev, check_rev=False) is False

    # Test documents with IDs with wrong collection name
    expected_error_msg = 'bad collection name'
    bad_id = generate_col_name() + '/' + doc_key
    for doc_input in [
        bad_id,
        {'_id': bad_id},
        {'_id': bad_id, '_rev': rev},
        {'_id': bad_id, '_rev': bad_rev},
        {'_id': bad_id, '_key': doc_key},
        {'_id': bad_id, '_key': doc_key, '_rev': rev},
        {'_id': bad_id, '_key': doc_key, '_rev': bad_rev},
    ]:
        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, check_rev=True)
        assert expected_error_msg in str(err.value)

        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, check_rev=False)
        assert expected_error_msg in str(err.value)

        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, rev=rev, check_rev=True)
        assert expected_error_msg in str(err.value)

        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, rev=rev, check_rev=False)
        assert expected_error_msg in str(err.value)

    # Test documents with missing "_id" and "_key" fields
    expected_error_msg = 'field "_key" or "_id" required'
    for doc_input in [
        {},
        {'foo': 'bar'},
        {'foo': 'bar', '_rev': rev},
        {'foo': 'bar', '_rev': bad_rev},
    ]:
        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, check_rev=True)
        assert str(err.value) == expected_error_msg

        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, check_rev=False)
        assert str(err.value) == expected_error_msg

        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, rev=rev, check_rev=True)
        assert str(err.value) == expected_error_msg

        with assert_raises(DocumentParseError) as err:
            col.has(doc_input, rev=rev, check_rev=False)
        assert str(err.value) == expected_error_msg


def test_document_get(col, docs):
    # Set up test documents
    col.import_bulk(docs)
    doc = docs[0]
    doc_val = doc['val']
    doc_key = doc['_key']
    doc_id = '{}/{}'.format(col.name, doc_key)

    # Test get existing document by body
    result = col.get(doc)
    assert result['_key'] == doc_key
    assert result['val'] == doc_val

    # Test get existing document by ID
    result = col.get(doc_id)
    assert result['_key'] == doc_key
    assert result['val'] == doc_val

    # Test get existing document by key
    result = col.get(doc_key)
    assert result['_key'] == doc_key
    assert result['val'] == doc_val

    # Test get missing document
    assert col.get(generate_doc_key()) is None

    # Test get with correct revision
    good_rev = col[doc_key]['_rev']
    result = col.get(doc, rev=good_rev)
    assert result['_key'] == doc_key
    assert result['val'] == doc_val

    # Test get with invalid revision
    bad_rev = col[doc_key]['_rev'] + '0'
    with assert_raises(DocumentRevisionError) as err:
        col.get(doc_key, rev=bad_rev, check_rev=True)
    assert err.value.error_code == 1200

    # Test get with correct revision and check_rev turned off
    result = col.get(doc, rev=bad_rev, check_rev=False)
    assert result['_key'] == doc_key
    assert result['_rev'] != bad_rev
    assert result['val'] == doc_val


def test_document_import_bulk(col, docs):
    # Test import_bulk with default options
    results = col.import_bulk(docs)
    result = results['result']
    assert result['created'] == len(docs)
    assert result['errors'] == 0
    assert 'details' in result

    for doc in docs:
        doc_key = doc['_key']
        assert doc_key in col
        assert col[doc_key]['_key'] == doc_key
        assert col[doc_key]['val'] == doc['val']
        assert col[doc_key]['loc'] == doc['loc']
    col.truncate()

    # Test import bulk without details
    results = col.import_bulk(docs, details=False)
    result = results['result']
    assert result['created'] == len(docs)
    assert result['errors'] == 0
    assert 'details' not in result
    for doc in docs:
        doc_key = doc['_key']
        assert doc_key in col
        assert col[doc_key]['_key'] == doc_key
        assert col[doc_key]['val'] == doc['val']
        assert col[doc_key]['loc'] == doc['loc']


def test_document_edge(lecol, docs, edocs):
    ecol = lecol  # legacy edge collection

    # Test insert edge without "_from" and "_to" fields
    with assert_raises(DocumentInsertError):
        ecol.insert(docs[0])

    # Test insert many edges without "_from" and "_to" fields
    for result in ecol.insert_many(docs):
        assert isinstance(result, DocumentInsertError)

    # Test update edge without "_from" and "_to" fields
    with assert_raises(DocumentUpdateError):
        ecol.update(docs[0])

    # Test update many edges without "_from" and "_to" fields
    for result in ecol.update_many(docs):
        assert isinstance(result, DocumentUpdateError)

    # Test replace edge without "_from" and "_to" fields
    with assert_raises(DocumentReplaceError):
        ecol.replace(docs[0])

    # Test replace many edges without "_from" and "_to" fields
    for result in ecol.replace_many(docs):
        assert isinstance(result, DocumentReplaceError)

    # Test edge document happy path
    edoc = edocs[0]

    # Test insert edge
    result = ecol.insert(edoc, return_new=True, sync=True)
    assert len(ecol) == 1
    assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
    assert result['_key'] == edoc['_key']
    assert result['new']['_key'] == edoc['_key'] == ecol[edoc]['_key']
    assert result['new']['_from'] == edoc['_from'] == ecol[edoc]['_from']
    assert result['new']['_to'] == edoc['_to'] == ecol[edoc]['_to']

    # Test update edge
    new_edoc = edoc.copy()
    new_edoc.update({'_from': 'foo', '_to': 'bar'})
    result = ecol.update(new_edoc, return_old=True, return_new=True)
    assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
    assert result['_key'] == edoc['_key']
    assert result['new']['_key'] == new_edoc['_key']
    assert result['new']['_from'] == new_edoc['_from']
    assert result['new']['_to'] == new_edoc['_to']
    assert result['old']['_key'] == edoc['_key']
    assert result['old']['_from'] == edoc['_from']
    assert result['old']['_to'] == edoc['_to']
    assert ecol[edoc]['_key'] == edoc['_key']
    assert ecol[edoc]['_from'] == new_edoc['_from']
    assert ecol[edoc]['_to'] == new_edoc['_to']
    edoc = new_edoc

    # Test replace edge
    new_edoc = edoc.copy()
    new_edoc.update({'_from': 'baz', '_to': 'qux'})
    result = ecol.replace(new_edoc, return_old=True, return_new=True)
    assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
    assert result['_key'] == edoc['_key']
    assert result['new']['_key'] == new_edoc['_key']
    assert result['new']['_from'] == new_edoc['_from']
    assert result['new']['_to'] == new_edoc['_to']
    assert result['old']['_key'] == edoc['_key']
    assert result['old']['_from'] == edoc['_from']
    assert result['old']['_to'] == edoc['_to']
    assert ecol[edoc]['_key'] == edoc['_key']
    assert ecol[edoc]['_from'] == new_edoc['_from']
    assert ecol[edoc]['_to'] == new_edoc['_to']
    edoc = new_edoc

    # Test delete edge
    result = ecol.delete(edoc, return_old=True)
    assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
    assert result['_key'] == edoc['_key']
    assert result['old']['_key'] == edoc['_key']
    assert result['old']['_from'] == edoc['_from']
    assert result['old']['_to'] == edoc['_to']
    assert edoc not in ecol

    # Test insert many edges
    results = ecol.insert_many(edocs, return_new=True, sync=True)
    for result, edoc in zip(results, edocs):
        assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
        assert result['_key'] == edoc['_key']
        assert result['new']['_key'] == edoc['_key']
        assert result['new']['_from'] == edoc['_from']
        assert result['new']['_to'] == edoc['_to']
        assert ecol[edoc]['_key'] == edoc['_key']
        assert ecol[edoc]['_from'] == edoc['_from']
        assert ecol[edoc]['_to'] == edoc['_to']
    assert len(ecol) == 4

    # Test update many edges
    for edoc in edocs:
        edoc['foo'] = 1
    results = ecol.update_many(edocs, return_new=True, sync=True)
    for result, edoc in zip(results, edocs):
        assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
        assert result['_key'] == edoc['_key']
        assert result['new']['_key'] == edoc['_key']
        assert result['new']['_from'] == edoc['_from']
        assert result['new']['_to'] == edoc['_to']
        assert result['new']['foo'] == 1
        assert ecol[edoc]['_key'] == edoc['_key']
        assert ecol[edoc]['_from'] == edoc['_from']
        assert ecol[edoc]['_to'] == edoc['_to']
        assert ecol[edoc]['foo'] == 1
    assert len(ecol) == 4

    # Test replace many edges
    for edoc in edocs:
        edoc['bar'] = edoc.pop('foo')
    results = ecol.replace_many(edocs, return_new=True, sync=True)
    for result, edoc in zip(results, edocs):
        assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
        assert result['_key'] == edoc['_key']
        assert result['new']['_key'] == edoc['_key']
        assert result['new']['_from'] == edoc['_from']
        assert result['new']['_to'] == edoc['_to']
        assert result['new']['bar'] == 1
        assert 'foo' not in result['new']
        assert ecol[edoc]['_key'] == edoc['_key']
        assert ecol[edoc]['_from'] == edoc['_from']
        assert ecol[edoc]['_to'] == edoc['_to']
        assert ecol[edoc]['bar'] == 1
        assert 'foo' not in ecol[edoc]
    assert len(ecol) == 4

    results = ecol.delete_many(edocs, return_old=True)
    for result, edoc in zip(results, edocs):
        assert result['_id'] == '{}/{}'.format(ecol.name, edoc['_key'])
        assert result['_key'] == edoc['_key']
        assert result['old']['_key'] == edoc['_key']
        assert result['old']['_from'] == edoc['_from']
        assert result['old']['_to'] == edoc['_to']
        assert edoc not in ecol
        assert edoc['_key'] not in ecol
    assert len(ecol) == 0

    # Test import bulk without to_prefix and from_prefix
    for doc in edocs:
        doc['_from'] = 'foo'
        doc['_to'] = 'bar'
    results = ecol.import_bulk(edocs)
    result = results['result']
    assert result['created'] == 0
    assert result['errors'] == 4
    errors = result['details']['errors']
    for x in range(len(errors)):
        assert errors[x]['errorNumber'] == 1233
        assert errors[x]['errorMessage'] == '{}: edge attribute missing or invalid'.format(x + 1)


def test_document_management_via_db(tst_fabric, col):
    doc1_id = col.name + '/foo'
    doc2_id = col.name + '/bar'
    doc1 = {'_key': 'foo'}
    doc2 = {'_id': doc2_id}

    # Test document insert with empty body
    result = tst_fabric.collection(col.name).insert({})
    assert len(col) == 1
    assert tst_fabric.collection(col.name).has(result['_id']) is True
    assert tst_fabric.collection(col.name).has(result['_id'], rev=result['_rev']) is True

    # Test document insert with key
    assert tst_fabric.collection(col.name).has(doc1_id) is False
    result = tst_fabric.collection(col.name).insert(doc1)
    assert result['_key'] == 'foo'
    assert result['_id'] == doc1_id
    assert len(col) == 2
    assert tst_fabric.collection(col.name).has(doc1_id) is True
    assert tst_fabric.collection(col.name).has(doc1_id, rev=result['_rev']) is True

    # Test document insert with ID
    assert tst_fabric.collection(col.name).has(doc2_id) is False
    result = tst_fabric.collection(col.name).insert(doc2)
    assert result['_key'] == 'bar'
    assert result['_id'] == doc2_id
    assert len(col) == 3
    assert tst_fabric.collection(col.name).has(doc2_id) is True
    assert tst_fabric.collection(col.name).has(doc2_id, rev=result['_rev']) is True

    # Test document get with bad input
    with assert_raises(DocumentParseError) as err:
        tst_fabric.collection(col.name).get({})
    assert str(err.value) == 'field "_key" or "_id" required'

    # Test document get
    for doc_id in [doc1_id, doc2_id]:
        result = tst_fabric.collection(col.name).get(doc_id)
        assert '_rev' in result
        assert '_key' in result
        assert result['_id'] == doc_id

    with pytest.raises(DocumentUpdateError) as err:
        tst_fabric.collection(col.name).update({'_key': 'H'})
    assert str(err) == "<ExceptionInfo DocumentUpdateError('[HTTP 404][ERR 1202] document not found') tblen=5>"

    # Test document update
    result = tst_fabric.collection(col.name).update({'_id': doc1_id, 'val': 100})
    assert result['_id'] == doc1_id
    assert col[doc1_id]['val'] == 100
    assert len(col) == 3

    # Test document replace with bad input
    with assert_raises(DocumentParseError) as err:
        tst_fabric.collection(col.name).replace({})
    assert str(err.value) == 'field "_key" or "_id" required'

    # Test document replace
    result = tst_fabric.collection(col.name).replace({'_id': doc1_id, 'num': 300})
    assert result['_id'] == doc1_id
    assert 'val' not in col[doc1_id]
    assert col[doc1_id]['num'] == 300
    assert len(col) == 3

    # Test document delete with bad input
    with assert_raises(DocumentParseError) as err:
        tst_fabric.collection(col.name).delete({})
    assert str(err.value) == 'field "_key" or "_id" required'

    # Test document delete
    result = tst_fabric.collection(col.name).delete({'_id': doc1_id})
    assert result['_id'] == doc1_id
    assert doc1_id not in col
    assert len(col) == 2
