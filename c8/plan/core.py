from c8.request import Request
from c8.exceptions import C8ServerError


def build_request(method, endpoint, data=None):
    request = Request(
        method=method,
        endpoint=endpoint,
        data=data
    )
    return request


class PlansServerError(C8ServerError):
    """Plans Server Error"""
