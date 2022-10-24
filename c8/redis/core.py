from json import dumps

from c8.exceptions import C8ServerError
from c8.request import Request


def build_request(collection, data):
    request = Request(method="post", endpoint="/redis/" + collection, data=dumps(data))
    return request


class RedisServerError(C8ServerError):
    """Redis Server Error"""
