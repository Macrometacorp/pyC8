from c8.request import Request
from c8.exceptions import C8ServerError


def build_request(method, endpoint, data=None):
    request = Request(
        method=method,
        endpoint=endpoint,
        data=data
    )
    return request


class JWTServerError(C8ServerError):
    """JWT Server Error"""
