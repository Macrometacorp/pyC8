from c8.api import APIWrapper
from c8.billing.core import BillingServerError, build_request
from c8.executor import DefaultExecutor


class BillingInterface(APIWrapper):

    """Billing API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection):
        super(BillingInterface, self).__init__(
            connection, executor=DefaultExecutor(connection)
        )

    def __repr__(self):
        return "<BillingInterface> for {}".format(self._conn.fabric_name)

    def execute(self, request):
        def response_handler(response):
            if not response.is_success and request is not None:
                raise BillingServerError(response, request)
            return response.body

        return self._execute(request, response_handler, custom_prefix="/_api/billing")

    def get_account(self, tenant=None):
        """
        Get account details (plan, contact and payment settings) for given tenant.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :returns: Account details of the given tenant.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        request = build_request(method="GET", endpoint="/account", tenant=tenant)
        return self.execute(request)

    def update_contact(self, tenant=None, contact={}):
        """
        Update contact details for given tenant.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :param contact: Contact object
        :type tenant: dict
        :returns: Updated contact details of the given tenant.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        data = {}
        attributes = [
            "firstname",
            "lastname",
            "email",
            "phone",
            "line1",
            "line2",
            "city",
            "state",
            "country",
            "zipcode",
        ]

        for attribute in attributes:
            if attribute in contact:
                data[attribute] = contact[attribute]

        request = build_request(
            method="PUT", endpoint="/contact", tenant=tenant, data=data
        )
        return self.execute(request)

    def get_previous_payments(self, tenant=None, months=10):
        """
        Fetch all payments details of the tenant for specified number of previous months.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :param limit: Number of previous months for which payment details are required.
        :type limit: int
        :returns: Payment details of the given tenant for the specified number of previous months.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        params = {"limit": months}
        request = build_request(
            method="GET", endpoint="/payments", tenant=tenant, params=params
        )
        return self.execute(request)

    def get_previous_invoices(self, tenant=None, months=3):
        """
        Fetch all invoice details of the tenant for specified number of previous months.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :param limit: Number of previous months for which invoices details are required.
        :type limit: int
        :returns: Invoice details of the given tenant for the specified number of previous months.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        params = {"limit": months}
        request = build_request(
            method="GET", endpoint="/invoices", tenant=tenant, params=params
        )
        return self.execute(request)

    def get_current_invoice(self, tenant=None):
        """
        Fetch invoice of the tenant for the current month.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :returns: Invoice details of the given tenant for the current month.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        request = build_request(
            method="GET", endpoint="/invoice/current", tenant=tenant
        )
        return self.execute(request)

    def get_specific_invoice(self, year, month, tenant=None):
        """
        Fetch invoice for a tenant, in a specific month and year.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param year: Year for which the invoice detail is required (in YYYY format).
        :type year: int
        :param month: Month for which the invoice detail is required. Valid values [1..12].
        :type month: int
        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :returns: Invoice detail of the given tenant for the specified year and month.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        request = build_request(
            method="GET", endpoint="/invoices/{}/{}".format(year, month), tenant=tenant
        )
        return self.execute(request)

    def get_usage(self, tenant=None, start_date=None, end_date=None):
        """
        Fetch usage of a tenant in a specific date range. If no query parameters are specified, usage from
        start date of the month to current date is returned.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :param start_date: Start date in 'YYYY-MM-DD' format.
        :type start_date: str
        :param end_date: End date in 'YYYY-MM-DD' format.
        :type end_date: str
        :returns: Usage details of the given tenant for the specified date range.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        params = {}
        if start_date is not None and end_date is not None:
            params = {"startDate": start_date, "endDate": end_date}

        request = build_request(
            method="GET", endpoint="/usage", tenant=tenant, params=params
        )
        return self.execute(request)

    def get_usage_region(self, region, tenant=None, start_date=None, end_date=None):
        """
        Fetch usage of a tenant in a specific date range for a specific region. If no query parameters are specified,
        usage from start date of the month to current date is returned.
        Note: If tenant name is not specified, the tenant invoking the API is used.
              This API is not applicable for system tenants.

        :param region: Name of the region.
        :type region: str
        :param tenant: Tenant name/id (not the display name)
        :type tenant: str
        :param start_date: Start date in 'YYYY-MM-DD' format.
        :type start_date: str
        :param end_date: End date in 'YYYY-MM-DD' format.
        :type end_date: str
        :returns: Usage details of the given tenant for the specified date range.
        :rtype: dict
        :raise c8.billing.core.BillingServerError If list fails.
        """
        params = {}
        if start_date is not None and end_date is not None:
            params = {"startDate": start_date, "endDate": end_date}

        request = build_request(
            method="GET",
            endpoint="/region/{}/usage".format(region),
            tenant=tenant,
            params=params,
        )
        return self.execute(request)
