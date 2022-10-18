from conftest import test_data_billing_plan, test_data_update_plan, test_update_tenant_billing_plan


def test_list_billing_plans(client):
    # Test create billing plan
    resp = client.create_billing_plan(billing_plan_definition=test_data_billing_plan())
    resp.pop('created')
    billing_plan_name = resp['name']
    assert resp == test_data_billing_plan()

    # Test list billing plan details
    resp = client.list_billing_plan_details(plan_name=billing_plan_name)
    resp.pop('created')
    assert resp == test_data_billing_plan()

    # Test update billing plan
    resp = client.modify_billing_plan(plan_name=billing_plan_name,
                                      billing_plan_definition=test_data_update_plan())
    resp[0].pop('created')
    assert resp[0] == test_data_update_plan()

    # Test update tenant billing plan
    # update_tenant_billing = client.update_tenant_billing_plan(test_update_tenant_billing_plan())
    # print(update_tenant_billing)

    # Test list billing plans
    resp = client.list_billing_plans()
    for x in range(len(resp)):
        assert 'active' in resp[x]
        assert 'featureGates' in resp[x]
        assert 'name' in resp[x]

    # Test remove billing plan
    resp = client.remove_billing_plan(plan_name=billing_plan_name)
    resp.pop('created')
    assert resp == test_data_update_plan()
