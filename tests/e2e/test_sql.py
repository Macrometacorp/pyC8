from conftest import get_client_instance, test_data_document

"""
To run end to end test .env file in /e2e is needed.
File needs to contain:
FEDERATION_URL
TENANT_EMAIL
API_KEY
FABRIC

Make sure that nba collection exists on fabric.
"""

SQL_COLLECTION = "testsqlcollection"


def test_create_redis_collection():
    client = get_client_instance()
    create_collection_response = client.create_collection(
        SQL_COLLECTION
    )
    insert_document_response = client.insert_document(
        SQL_COLLECTION,
        silent=True,
        document=test_data_document()
    )
    # Response from platform
    assert True == insert_document_response
    assert SQL_COLLECTION == create_collection_response.name


def test_sql_endpoint():
    client = get_client_instance()
    cursor = client.execute_query(
        'SELECT * FROM {}'.format(SQL_COLLECTION),
        sql=True
    )
    docs = [doc for doc in cursor]

    entries = ('_id', '_key', '_rev')
    for doc in docs:
        for key in entries:
            if key in doc:
                del doc[key]

    assert test_data_document() == docs


def test_delete_sql_collection():
    client = get_client_instance()
    response = client.delete_collection(SQL_COLLECTION)
    # Response from platform
    assert True == response
