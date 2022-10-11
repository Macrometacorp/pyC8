import os

from dotenv import load_dotenv

from c8 import C8Client


def get_client_instance():
    load_dotenv()
    client = C8Client(protocol='https',
                      host=os.environ.get('FEDERATION_URL'),
                      port=443,
                      email=os.environ.get('TENANT_EMAIL'),
                      apikey=os.environ.get('API_KEY'),
                      geofabric=os.environ.get('FABRIC')
                      )
    return client


def response_content():
    return [
        {
            'age': 25,
            'college': 'Texas',
            'height': '6-2',
            'name': 'Avery Bradley',
            'number': 0,
            'position': 'PG',
            'salary': 7730337,
            'team': 'Boston Celtics',
            'weight': 180,
        },
        {
            'age': 25,
            'college': 'Marquette',
            'height': '6-6',
            'name': 'Jae Crowder',
            'number': 99,
            'position': 'SF',
            'salary': 6796117,
            'team': 'Boston Celtics',
            'weight': 235,
        },
        {
            'age': 27,
            'college': 'Boston University',
            'height': '6-5',
            'name': 'John Holland',
            'number': 30,
            'position': 'SG',
            'salary': 7730337,
            'team': 'Boston Celtics',
            'weight': 205,
        },
        {
            'age': 22,
            'college': 'Georgia State',
            'height': '6-5',
            'name': 'R.J. Hunter',
            'number': 28,
            'position': 'SG',
            'salary': 1148640,
            'team': 'Boston Celtics',
            'weight': 185,
        },
        {
            'age': 29,
            'college': 'Boston Celtics',
            'height': '6-10',
            'name': 'Jonas Jerebko',
            'number': 8,
            'position': 'PF',
            'salary': 5000000,
            'team': 'Boston Celtics',
            'weight': 231,
        }
       ]
