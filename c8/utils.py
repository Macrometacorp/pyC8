from __future__ import absolute_import, unicode_literals

import csv
import json
import logging
from collections import deque
from contextlib import contextmanager

from six import string_types

from c8.cursor import Cursor
from c8.exceptions import DocumentParseError


@contextmanager
def suppress_warning(logger_name):
    """Suppress logger messages.

    :param logger_name: Full name of the logger.
    :type logger_name: str | unicode
    """
    logger = logging.getLogger(logger_name)
    original_log_level = logger.getEffectiveLevel()
    logger.setLevel(logging.CRITICAL)
    yield
    logger.setLevel(original_log_level)


def get_col_name(doc):
    """Return the collection name from input.

    :param doc: Document ID or body with "_id" field.
    :type doc: str | unicode | dict
    :return: Collection name.
    :rtype: [str | unicode]
    :raise c8.exceptions.DocumentParseError: If document ID is missing.
    """
    try:
        doc_id = doc["_id"] if isinstance(doc, dict) else doc
    except KeyError:
        raise DocumentParseError('field "_id" required')
    return doc_id.split("/", 1)[0]


def get_doc_id(doc):
    """Return the document ID from input.

    :param doc: Document ID or body with "_id" field.
    :type doc: str | unicode | dict
    :return: Document ID.
    :rtype: str | unicode
    :raise c8.exceptions.DocumentParseError: If document ID is missing.
    """
    try:
        return doc["_id"] if isinstance(doc, dict) else doc
    except KeyError:
        raise DocumentParseError('field "_id" required')


def is_none_or_int(obj):
    """Check if obj is None or an integer.

    :param obj: Object to check.
    :type obj: object
    :return: True if object is None or an integer.
    :rtype: bool
    """
    return obj is None or (isinstance(obj, int) and obj >= 0)


def is_none_or_str(obj):
    """Check if obj is None or a string.

    :param obj: Object to check.
    :type obj: object
    :return: True if object is None or a string.
    :rtype: bool
    """
    return obj is None or isinstance(obj, string_types)


def json_reader(filepath):
    try:
        file = open(filepath)
        return json.load(file)
    except json.JSONDecodeError:
        raise Exception("Invalid JSON file")


def csv_reader(filepath):
    try:
        loaded = csv.DictReader(open(filepath, newline=""))
        return loaded
    except csv.Error:
        raise csv.Error


def group_csv_key_values(data):
    data_dict = {}
    index = 0
    for row in data:
        for column, value in row.items():
            data_dict.setdefault(column, {index: value})
            temp_dict = data_dict.get(column)
            temp_dict.update({index: value})
        index += 1
    return data_dict


def get_documents_from_file(data, index):
    documents = []
    for key in data.keys():
        first_key = key
        break
    for counter in range(len(data[first_key])):
        document = {}
        for key in data.keys():
            document[key] = data[key][index]
        index += 1
        documents.append(document)
    return documents, index


def clean_doc(obj):
    """Return the document(s) with all extra system keys stripped.
    :param obj: document(s)
    :type obj: list | dict | c8.cursor.Cursor
    :return: Document(s) with the system keys stripped
    :rtype: list | dict
    """
    if isinstance(obj, (Cursor, list, deque)):
        docs = [clean_doc(d) for d in obj]
        return docs

    if isinstance(obj, dict):
        return {
            field: value
            for field, value in obj.items()
            if field in {"_key", "_from", "_to"} or not field.startswith("_")
        }
