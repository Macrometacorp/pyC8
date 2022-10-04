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


def test_redis_set():
    load_dotenv()
    client = C8Client(protocol='https',
                      host=os.environ.get('FEDERATION_URL'),
                      port=443,
                      email=os.environ.get('TENANT_EMAIL'),
                      apikey=os.environ.get('API_KEY'),
                      geofabric=os.environ.get('FABRIC')
                      )

    response = client.redis_set("foo", "bar", "DinoRedisDB")
    # Idea!!!
    # client.Redis.redis_set()
    print(response)
