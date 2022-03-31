from __future__ import absolute_import, unicode_literals

from collections import deque
from uuid import uuid4

import pytest

from c8.cursor import Cursor
from c8.exceptions import (
    AsyncExecuteError,
    BatchExecuteError,
)


def generate_fabric_name():
    """Generate and return a random fabric name.

    :return: Random fabric name.
    :rtype: str | unicode
    """
    return 'test_fabric_{}'.format(uuid4().hex)


def generate_col_name():
    """Generate and return a random collection name.

    :return: Random collection name.
    :rtype: str | unicode
    """
    return 'test_collection_{}'.format(uuid4().hex)


def generate_graph_name():
    """Generate and return a random graph name.

    :return: Random graph name.
    :rtype: str | unicode
    """
    return 'test_graph_{}'.format(uuid4().hex)


def generate_doc_key():
    """Generate and return a random document key.

    :return: Random document key.
    :rtype: str | unicode
    """
    return 'test_document_{}'.format(uuid4().hex)


def generate_task_name():
    """Generate and return a random task name.

    :return: Random task name.
    :rtype: str | unicode
    """
    return 'test_task_{}'.format(uuid4().hex)


def generate_task_id():
    """Generate and return a random task ID.

    :return: Random task ID
    :rtype: str | unicode
    """
    return 'test_task_id_{}'.format(uuid4().hex)


def generate_username():
    """Generate and return a random username.

    :return: Random username.
    :rtype: str | unicode
    """
    return 'test_user_{}'.format(uuid4().hex)


def generate_string():
    """Generate and return a random unique string.

    :return: Random unique string.
    :rtype: str | unicode
    """
    return uuid4().hex


def generate_service_mount():
    """Generate and return a random service name.

    :return: Random service name.
    :rtype: str | unicode
    """
    return '/test_{}'.format(uuid4().hex)


def clean_doc(obj):
    """Return the document(s) with all extra system keys stripped.

    :param obj: document(s)
    :type obj: list | dict | c8.cursor.Cursor
    :return: Document(s) with the system keys stripped
    :rtype: list | dict
    """
    if isinstance(obj, (Cursor, list, deque)):
        docs = [clean_doc(d) for d in obj]
        return sorted(docs, key=lambda doc: doc['_key'])

    if isinstance(obj, dict):
        return {
            field: value for field, value in obj.items()
            if field in {'_key', '_from', '_to'} or not field.startswith('_')
        }


def extract(key, items):
    """Return the sorted values from dicts using the given key.

    :param key: Dictionary key
    :type key: str | unicode
    :param items: Items to filter.
    :type items: [dict]
    :return: Set of values.
    :rtype: [str | unicode]
    """
    return sorted(item[key] for item in items)


def assert_raises(exception):
    """Assert that the given exception is raised.

    :param exception: Expected exception.
    :type: Exception
    """
    return pytest.raises((
        exception,
        AsyncExecuteError,
        BatchExecuteError,
    ))
