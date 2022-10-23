from c8.JWT.core import JWTServerError, build_request
from c8.api import APIWrapper
from c8.executor import DefaultExecutor


class JwtInterface(APIWrapper):
    """Plan API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, connection):
        super().__init__(connection, executor=DefaultExecutor(connection))

    def response_handler_generic(self, request):
        def response_handler(response):
            if not response.is_success and request is not None:
                raise JWTServerError(response, request)
            return response.body

        return response_handler

    def get_jwt_for_user(self, password, email=None, username=None, tenant=None):
        """" Obtain a JWT Authentication for a user.

        After obtaining the token, REST API calls may be invoked by passing the obtained token
        in the REST header. Either email or both tenant and username are required. Add the following
        to your header when you make REST calls: "Authorization: bearer TOKEN"

        :param email: email address to obtain the jwt
        :type email: str
        :param password: password of the current tenant
        :type password: str
        :param username: username of the current tenant
        :type username: str
        :param tenant: value of the current tenant
        :type tenant: str
        :returns: jwt with tenant and username
        :rtype: dict
        :raise c8.jwt.core.JwtServerError If create fails.
        """
        data = self.create_request_payload(password=password, email=email, username=username, tenant=tenant)

        request = build_request(method="POST", endpoint="/_open/auth", data=data)

        response_handler = self.response_handler_generic(request)

        return self._execute(request, response_handler, custom_prefix='')

    def create_request_payload(self, password, email, username, tenant):
        request_payload = {'password': password}
        if email is not None:
            request_payload['email'] = email
        if username is not None and tenant is not None:
            request_payload['username'] = username
            request_payload['tenant'] = tenant
        return request_payload
