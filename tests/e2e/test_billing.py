from datetime import date, timedelta

import pytest

from c8.billing.core import BillingServerError
from tests.helpers import assert_raises


@pytest.fixture
def get_dates():
    end_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    return start_date, end_date


@pytest.mark.vcr
def test_get_account(get_client_instance):
    tenant_name = get_client_instance._tenant.name
    # Test get billing account details for valid tenant
    resp = get_client_instance.billing.get_account(tenant_name)
    assert resp["data"]["tenant"] == tenant_name
    # Test get billing account details for no tenant (should give result for tenant invoking the request)
    resp = get_client_instance.billing.get_account()
    assert resp["data"]["tenant"] == tenant_name
    # Test get billing account details for invalid tenant
    with assert_raises(BillingServerError):
        get_client_instance.billing.get_account("invalid")


@pytest.mark.vcr
def test_update_contact(get_client_instance):
    contact = {
        "firstname": "John",
        "lastname": "Doe",
        "email": "john@acme.com",
        "phone": "404-555-8726",
        "line1": "1388 Villa Drive",
        "line2": "Suite 240C",
        "city": "South Bend",
        "state": "IN",
        "country": "US",
        "zipcode": "46601",
    }
    # Test get billing update contact
    resp = get_client_instance.billing.update_contact(
        tenant=get_client_instance._tenant.name, contact=contact
    )
    assert resp["data"] == contact

    # Test get billing update contact with invalid tenant
    with assert_raises(BillingServerError):
        get_client_instance.billing.update_contact("invalid")


@pytest.mark.vcr
def test_get_previous_payments(get_client_instance):
    # Test get billing account details for tenant having no stripe account
    with assert_raises(BillingServerError) as err:
        get_client_instance.billing.get_previous_payments(months=1)
    assert "Method Not Allowed" in str(err.value)


@pytest.mark.vcr
def test_get_previous_invoices(get_client_instance):
    # Test get billing account details for tenant having no stripe account
    with assert_raises(BillingServerError) as err:
        get_client_instance.billing.get_previous_invoices(months=1)
    assert "Method Not Allowed" in str(err.value)


@pytest.mark.vcr
def test_get_current_invoice(get_client_instance):
    # Test get billing account details for tenant having no stripe account
    with assert_raises(BillingServerError) as err:
        get_client_instance.billing.get_current_invoice()
    assert "Method Not Allowed" in str(err.value)


@pytest.mark.vcr
def test_get_specific_invoice(get_client_instance):
    # Test get billing account details for tenant having no stripe account
    with assert_raises(BillingServerError) as err:
        get_client_instance.billing.get_specific_invoice(year=2022, month=10)
    assert "Method Not Allowed" in str(err.value)


def test_get_usage(get_client_instance, get_dates):
    # Test get billing usage
    start_date, end_date = get_dates
    resp = get_client_instance.billing.get_usage(
        start_date=start_date, end_date=end_date
    )
    assert resp["data"][0]["tenant"] == get_client_instance._tenant.name

    # Test get billing usage with invalid tenant
    with assert_raises(BillingServerError):
        get_client_instance.billing.get_usage("invalid")


def test_get_usage_region(get_client_instance, get_dates):
    # Test get billing usage
    start_date, end_date = get_dates
    region = get_client_instance.get_local_dc(False)
    resp = get_client_instance.billing.get_usage_region(
        region=region, start_date=start_date, end_date=end_date
    )
    assert resp["data"][0]["tenant"] == get_client_instance._tenant.name

    # Test get billing usage with invalid tenant
    with assert_raises(BillingServerError):
        get_client_instance.billing.get_usage_region(region=region, tenant="invalid")
