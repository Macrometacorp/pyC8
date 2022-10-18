from c8.plan.core import build_request, PlansServerError
from c8.api import APIWrapper


class PlanInterface(APIWrapper):
    """Plan API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def list_billing_plans(self):
        request = build_request(method='GET', endpoint='/plan')

        def response_handler(response):
            if not response.is_success and request is not None:
                raise PlansServerError(response, request)
            return response.body

        return self._execute(request, response_handler)

    def list_billing_plan_details(self, plan_name):
        request = build_request(method='GET', endpoint='/plan/{}'.format(plan_name))

        def response_handler(response):
            if not response.is_success and request is not None:
                raise PlansServerError(response, request)
            return response.body

        return self._execute(request, response_handler)

    def create_billing_plan(self, data):
        request = build_request(method='POST', endpoint='/plan', data=data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise PlansServerError(response, request)
            return response.body

        return self._execute(request, response_handler)

    def modify_billing_plan(self, plan_name, data):
        request = build_request(method='PATCH', endpoint='/plan/{}'.format(plan_name), data=data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise PlansServerError(response, request)
            return response.body

        return self._execute(request, response_handler)

    def remove_billing_plan(self, plan_name):
        request = build_request(method='DELETE', endpoint='/plan/{}'.format(plan_name))

        def response_handler(response):
            if not response.is_success and request is not None:
                raise PlansServerError(response, request)
            return response.body

        return self._execute(request, response_handler)

    def update_tenant_billing_plan(self, data):
        request = build_request(method='POST', endpoint='/plan/update', data=data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise PlansServerError(response, request)
            return response.body

        return self._execute(request, response_handler)
