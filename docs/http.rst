Using Custom HTTP Clients
-------------------------

pyC8 lets you use your own HTTP clients for sending API requests to
C8 Data Fabric server. The default implementation uses the requests_ library.

Your HTTP client must inherit :class:`c8.http.HTTPClient` and implement its
abstract method :func:`c8.http.HTTPClient.send_request`. The method must
return valid (fully populated) instances of :class:`c8.response.Response`.

For example, let's say your HTTP client needs:

* Automatic retries
* Additional HTTP header called ``x-my-header``
* SSL certificate verification disabled
* Custom logging

Your ``CustomHTTPClient`` class might look something like this:

.. testcode::

    import logging

    from requests.adapters import HTTPAdapter
    from requests import Session

    from c8.response import Response
    from c8.http import HTTPClient


    class CustomHTTPClient(HTTPClient):
        """My custom HTTP client with cool features."""

        def __init__(self):
            self._session = Session()

            # Initialize your logger.
            self._logger = logging.getLogger('my_logger')

            # Add your request headers.
            self._session.headers.update({'x-my-header': 'true'})

            # Enable retries.
            adapter = HTTPAdapter(max_retries=5)
            self._session.mount('https://', adapter)

        def send_request(self,
                         method,
                         url,
                         params=None,
                         data=None,
                         headers=None,
                         auth=None):

            # Add your own debug statement.
            self._logger.debug('Sending request to {}'.format(url))

            # Send a request.
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                headers=headers,
                auth=auth,
                verify=False  # Disable SSL verification
            )
            self._logger.debug('Got {}'.format(response.status_code))

            # Return an instance of c8.response.Response per spec.
            return Response(
                method=response.request.method,
                url=response.url,
                headers=response.headers,
                status_code=response.status_code,
                status_text=response.reason,
                raw_body=response.text,
            )

Then you would inject your client as follows:

.. testsetup::

    import logging

    from requests.adapters import HTTPAdapter
    from requests import Session

    from c8.response import Response
    from c8.http import HTTPClient

    class CustomHTTPClient(HTTPClient):
        """Custom HTTP client."""

        def __init__(self):
            self._session = Session()

            # Initialize logger.
            self._logger = logging.getLogger('my_logger')

            # Add request headers.
            self._session.headers.update({'x-my-header': 'true'})

            # Add retries.
            adapter = HTTPAdapter(max_retries=5)
            self._session.mount('https://', adapter)

        def send_request(self,
                         method,
                         url,
                         params=None,
                         data=None,
                         headers=None,
                         auth=None):
            # Add your own debug statement.
            self._logger.debug('Sending request to {}'.format(url))

            # Send a request without SSL verification.
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                headers=headers,
                auth=auth,
                verify=False  # No SSL verification
            )
            self._logger.debug('Got {}'.format(response.status_code))

            # You must return an instance of c8.response.Response.
            return Response(
                method=response.request.method,
                url=response.url,
                headers=response.headers,
                status_code=response.status_code,
                status_text=response.reason,
                raw_body=response.text,
            )

.. testcode::

    from c8 import C8Client

    # from my_module import CustomHTTPClient

    client = C8Client(
        protocol='https',
        host='gdn1.macrometa.io',
        port=443,
        http_client=CustomHTTPClient()
    )

For more information on how to configure a ``requests.Session`` object, refer
to `requests documentation`_.

.. _requests: https://github.com/requests/requests
.. _requests documentation: http://docs.python-requests.org/en/master/user/advanced/#session-objects
