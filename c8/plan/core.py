from c8.request import Request
from c8.exceptions import C8ServerError


def build_request(method, endpoint, data=None):
    if method == 'GET' or method == 'DELETE':
        request = Request(
            method=method,
            endpoint=endpoint,
        )
        return request
    elif method == 'POST' or method == 'PATCH':
        request = Request(
            method=method,
            endpoint=endpoint,
            data=data
        )
        return request


class PlansServerError(C8ServerError):
    """Plans Server Error"""
