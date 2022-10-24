from c8.api import APIWrapper
from c8.executor import DefaultExecutor
from c8.plan.core import PlansServerError, build_request


class PlanInterface(APIWrapper):
    """Plan API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, connection):
        super().__init__(connection, executor=DefaultExecutor(connection))

    def response_handler_generic(self, request):
        def response_handler(response):
            if not response.is_success and request is not None:
                raise PlansServerError(response, request)
            return response.body

        return response_handler

    def list_billing_plans(self):
        """
        Fetch a list of billing plans available
        :returns: list of billing plans.
        :rtype: list
        :raise c8.plan.core.PlansServerError If list fails.
        """
        request = build_request(method="GET", endpoint="/plan")

        response_handler = self.response_handler_generic(request)

        return self._execute(request, response_handler)

    def list_billing_plan_details(self, plan_name):
        """
        Given a valid plan_name, fetch details of the specific billing plan
        :param plan_name: Name of the existing billing plan
        :type plan_name: str
        :returns: list of billing plans.
        :rtype: list
        :raise c8.plan.core.PlansServerError If list fails.
        """
        request = build_request(method="GET", endpoint="/plan/{}".format(plan_name))

        response_handler = self.response_handler_generic(request)

        return self._execute(request, response_handler)

    def create_billing_plan(
        self, name, features_gates, attribution, label, active, plan_details=None
    ):
        """
        Given a valid billing_plan_definition, create a new billing plan
        :param name: Name of the new billing plan
        :type name: str
        :param features_gates: Features gates associated with the plan Ex. KV, DOCS, DYNAMO, CEP
        :type name: dict
        :param attribution: Attribute to define primary key with label
        :type name: str
        :param label: Attribute to define primary key with attribution
        :type name: str
        :param active: Set if the plan is active or not
        :type name: bool
        :param plan_details: Set of details about the plan such as 'planId', 'description', 'pricing', 'isBundle',
         'metadata', 'demo', 'metrics'
        :type name: dict
        :returns: billing plan definition
        :rtype: dict
        :raise c8.plan.core.PlansServerError If create fails.
        """

        data = self.create_plan_details(
            name=name,
            features_gates=features_gates,
            attribution=attribution,
            label=label,
            active=active,
            plan_details=plan_details,
        )

        request = build_request(method="POST", endpoint="/plan", data=data)

        response_handler = self.response_handler_generic(request)

        return self._execute(request, response_handler)

    def modify_billing_plan(
        self,
        plan_name,
        attribution,
        label,
        name=None,
        features_gates=None,
        active=None,
        plan_details=None,
    ):
        """
        Given a valid billing_plan_definition, modify an existing billing plan
        :param plan_name: Current name for the plan
        :type plan_name: str
        :param name: New name of the current billing plan
        :type name: str
        :param features_gates: Features gates associated with the plan Ex. KV, DOCS, DYNAMO, CEP
        :type features_gates: dict
        :param attribution: Attribute to define primary key with label
        :type attribution: str
        :param label: Attribute to define primary key with attribution
        :type label: str
        :param active: Set if the plan is active or not
        :type active: bool
        :param plan_details: Set of details about the plan such as 'planId', 'description', 'pricing', 'isBundle',
         'metadata', 'demo', 'metrics'
        :type plan_details: dict
        :returns: billing plan definition
        :rtype: dict
        :raise c8.plan.core.PlansServerError If create fails.
        """
        data = self.create_plan_details(
            name=name,
            features_gates=features_gates,
            attribution=attribution,
            label=label,
            active=active,
            plan_details=plan_details,
        )
        request = build_request(
            method="PATCH", endpoint="/plan/{}".format(plan_name), data=data
        )

        response_handler = self.response_handler_generic(request)

        return self._execute(request, response_handler)

    def remove_billing_plan(self, plan_name):
        """
        Given a valid plan_name, remove an existing billing plan.
        :param plan_name: Name of the existing billing plan
        :type plan_name: str
        :returns: definition of the billing plan.
        :rtype: list
        :raise c8.plan.core.PlansServerError If remove fails.
        """
        request = build_request(method="DELETE", endpoint="/plan/{}".format(plan_name))

        response_handler = self.response_handler_generic(request)

        return self._execute(request, response_handler)

    def update_tenant_billing_plan(
        self, attribution, plan, payment_method_id, tenant=None
    ):
        """
        Update the billing plan for a tenant.
        Note: If tenant name is not specified, the tenant invoking the API is used to update billing plan.
        :param attribution: Attribute to define primary key with label
        :type attribution: str
        :param plan: Name of the existing billing plan
        :type plan: str
        :param tenant: Name of the existing tenant
        :type tenant: str
        :param payment_method_id: Stripe payment method ID
        :type payment_method_id: str
        :returns: definition of the billing plan.
        :rtype: list
        :raise c8.plan.core.PlansServerError If update fails.
        """
        plan_details = {}
        if attribution is not None:
            plan_details["attribution"] = attribution
        if plan is not None:
            plan_details["plan"] = plan
        if payment_method_id is not None:
            plan_details["payment_method_id"] = payment_method_id
        if tenant is not None:
            plan_details["tenant"] = tenant

        request = build_request(
            method="POST", endpoint="/plan/update", data=plan_details
        )

        response_handler = self.response_handler_generic(request)

        return self._execute(request, response_handler)

    def create_plan_details(
        self, name, features_gates, attribution, label, active, plan_details
    ):
        data = {}
        if name is not None:
            data["name"] = name
        if features_gates is not None:
            data["featureGates"] = features_gates
        if attribution is not None:
            data["attribution"] = attribution
        if label is not None:
            data["label"] = label
        if active is not None:
            data["active"] = active

        keys = [
            "planId",
            "description",
            "pricing",
            "isBundle",
            "metadata",
            "demo",
            "metrics",
        ]
        for key in keys:
            if key in plan_details:
                data[key] = plan_details[key]
        return data
