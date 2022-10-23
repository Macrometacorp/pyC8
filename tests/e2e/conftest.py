import os
import pytest

from dotenv import load_dotenv

from c8 import C8Client


load_dotenv()
client = C8Client(protocol='https',
                  host=os.environ.get('FEDERATION_URL'),
                  port=443,
                  email=os.environ.get('TENANT_EMAIL'),
                  apikey=os.environ.get('API_KEY'),
                  geofabric=os.environ.get('FABRIC')
                  )

mm_client = C8Client(protocol='https',
                  host=os.environ.get('FEDERATION_URL'),
                  port=443,
                  email=os.environ.get('MM_TENANT_EMAIL'),
                  apikey=os.environ.get('MM_API_KEY'),
                  geofabric=os.environ.get('FABRIC')
                  )


@pytest.fixture
def get_client_instance():
    return client

@pytest.fixture
def get_mm_client_instance():
    return mm_client

def test_data_document():
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


def test_data_billing_plan():
    return {
        "name": "TestPyC8",
        "planId": "1",
        "description": "New billing plan.",
        "featureGates": [
            "KV"
        ],
        "attribution": "Macrometa",
        "label": "TestSDK",
        "pricing": "Custom pricing",
        "isBundle": True,
        "metadata": {
            "key": "value"
        },
        "metrics": [
            {
                "name": "kv-reads",
                "value": "c8db_service_kv_read_requests_count",
                "metricType": "counter"
            }
        ],
        "active": True,
        "demo": False
    }


def test_data_update_plan():
    return {
        "description": "updated plan",
        "attribution": "Macrometa",
        "label": "TestSDK",
        "pricing": "Custom pricing",
        "isBundle": True,
        "metrics": [
            {
                "name": "kv-reads",
                "value": "c8db_service_kv_read_requests_count",
                "metricType": "counter"
            },
            {
                "name": "doc-reads",
                "value": "c8db_service_doc_read_requests_count",
                "metricType": "counter",
                "stripeCategory": "apiOperations",
                "dashboardCategory": "apis",
                "service": "doc",
                "operation": "reads"
            }
        ],
        "active": True,
        "demo": False
    }


def test_update_tenant_billing_plan():
    return {
        "attribution": "Macrometa",
        "plan": "TestPyC8",
        "tenant": "edgar.garcia_macrometa.com",
        "payment_method_id": "pm_1KRHKj2eZvKYlo2CHkt3ra77"
    }
