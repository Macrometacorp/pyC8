from json import dumps
from c8.request import Request
from c8.exceptions import C8ServerError


def build_request(collection, data):
    request = Request(
        method='post',
        endpoint='/redis/' + collection,
        data=dumps(data)
    )
    return request


def response_handler(request, response):
    if not response.is_success:
        raise RedisServerError(response, request)
    return response.body


class RedisServerError(C8ServerError):
    """Redis Server Error"""

