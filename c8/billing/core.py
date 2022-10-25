from c8.request import Request
from c8.exceptions import C8ServerError


def build_request(method, endpoint, tenant, data=None, params=None):
    headers = {"tenant": tenant}
    request = Request(
        method=method,
        endpoint=endpoint,
        headers=headers,
        data=data,
        params=params
    )
    return request


class BillingServerError(C8ServerError):
    """Billing Server Error"""

