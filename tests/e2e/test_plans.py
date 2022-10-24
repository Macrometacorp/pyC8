import pytest
from conftest import (
    get_mm_client_instance,
    test_data_billing_plan,
    test_data_update_plan,
    test_update_tenant_billing_plan,
)

from c8.plan.core import PlansServerError
from tests.helpers import assert_raises


@pytest.fixture
def plans_setup_client():
    client = get_mm_client_instance()
    return client


def test_list_billing_plans(plans_setup_client):
    billing_details = test_data_billing_plan()
    billing_plan_name = billing_details["name"]
    plan_details = {
        "planId": billing_details["planId"],
        "description": billing_details["description"],
        "pricing": billing_details["pricing"],
        "isBundle": billing_details["isBundle"],
        "metadata": billing_details["metadata"],
        "demo": billing_details["demo"],
        "metrics": billing_details["metrics"],
    }

    # Test create billing plan
    resp = plans_setup_client.plan().create_billing_plan(
        name=billing_details["name"],
        features_gates=billing_details["featureGates"],
        attribution=billing_details["attribution"],
        label=billing_details["label"],
        active=billing_details["active"],
        plan_details=plan_details,
    )

    for i in test_data_billing_plan():
        assert i in resp

    # Test list billing plan details
    resp = plans_setup_client.plan().list_billing_plan_details(
        plan_name=billing_plan_name
    )
    for i in test_data_billing_plan():
        assert i in resp

    # Test update billing plan
    billing_details_update = test_data_update_plan()
    update_plan = {
        "description": billing_details_update["description"],
        "pricing": billing_details_update["pricing"],
        "isBundle": billing_details_update["isBundle"],
        "demo": billing_details_update["demo"],
        "metrics": billing_details_update["metrics"],
    }
    resp = plans_setup_client.plan().modify_billing_plan(
        plan_name=billing_plan_name,
        attribution=billing_details_update["attribution"],
        active=billing_details_update["active"],
        label=billing_details_update["label"],
        plan_details=update_plan,
    )
    for x in test_data_update_plan():
        assert x in resp[0]

    # Test update tenant billing plan
    update_data = test_update_tenant_billing_plan()
    with assert_raises(PlansServerError) as err:
        plans_setup_client.plan().update_tenant_billing_plan(
            attribution=update_data["attribution"],
            plan=update_data["plan"],
            payment_method_id=update_data["payment_method_id"],
            tenant=update_data["tenant"],
        )
    assert err.value.error_code == 404

    # Test list billing plans
    resp = plans_setup_client.plan().list_billing_plans()
    for x in range(len(resp)):
        assert "active" in resp[x]
        assert "featureGates" in resp[x]
        assert "name" in resp[x]

    # Test remove billing plan
    resp = plans_setup_client.plan().remove_billing_plan(plan_name=billing_plan_name)
    for x in test_data_update_plan():
        assert x in resp

    resp = plans_setup_client.plan().list_billing_plans()
    for x in range(len(resp)):
        assert billing_plan_name not in resp[x]["name"]
