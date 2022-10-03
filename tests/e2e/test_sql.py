import os

from dotenv import load_dotenv

from c8 import C8Client
from conftest import response_content


"""
To run end to end test .env file in /e2e is needed.
File needs to contain:
FEDERATION_URL
TENANT_EMAIL
API_KEY
FABRIC

Make sure that nba collection exists on fabric.
"""


def test_sql_endpoint():
    load_dotenv()
    client = C8Client(protocol='https',
                      host=os.environ.get('FEDERATION_URL'), port=443,
                      email=os.environ.get('TENANT_EMAIL'),
                      apikey=os.environ.get('API_KEY'),
                      geofabric=os.environ.get('FABRIC')
                      )

    cursor = client.execute_query('SELECT * FROM newnba', sql=True)
    docs = [doc for doc in cursor]

    entries = ('_id', '_key', '_rev')
    for doc in docs:
        for key in entries:
            if key in doc:
                del doc[key]

    assert response_content() == docs
