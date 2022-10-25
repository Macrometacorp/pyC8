from conftest import test_data_document

"""
Tests need to be run in sequence since we first create collection, after that we fill
collection with test data data, run tests and check for the results.
"""

SQL_COLLECTION = "testsqlcollection"


def test_create_redis_collection(get_client_instance):
    create_collection_response = get_client_instance.create_collection(SQL_COLLECTION)
    insert_document_response = get_client_instance.insert_document(
        SQL_COLLECTION, silent=True, document=test_data_document()
    )
    # Response from platform
    assert insert_document_response is True
    assert SQL_COLLECTION == create_collection_response.name


def test_sql_endpoint(get_client_instance):
    cursor = get_client_instance.execute_query(
        "SELECT * FROM {}".format(SQL_COLLECTION), sql=True
    )
    docs = [doc for doc in cursor]

    entries = ("_id", "_key", "_rev")
    for doc in docs:
        for key in entries:
            if key in doc:
                del doc[key]

    assert test_data_document() == docs


def test_delete_sql_collection(get_client_instance):
    response = get_client_instance.delete_collection(SQL_COLLECTION)
    # Response from platform
    assert response is True
