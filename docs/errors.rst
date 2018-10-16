Error Handling
--------------

All pyC8 exceptions inherit :class:`c8.exceptions.C8Error`,
which splits into subclasses :class:`c8.exceptions.C8ServerError` and
:class:`c8.exceptions.C8ClientError`.

Server Errors
=============

:class:`c8.exceptions.C8ServerError` exceptions lightly wrap non-2xx
HTTP responses coming from C8 Data Fabric. Each exception object contains the error
message, error code and HTTP request response details.

**Example:**

.. testcode::

    from c8 import C8Client, C8ServerError, DocumentInsertError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = db.collection('students')

    try:
        students.insert({'_key': 'John'})
        students.insert({'_key': 'John'})  # duplicate key error

    except DocumentInsertError as exc:

        assert isinstance(exc, C8ServerError)
        assert exc.source == 'server'

        exc.message           # Exception message usually from C8 Data Fabric
        exc.error_message     # Raw error message from C8 Data Fabric
        exc.error_code        # Error code from C8 Data Fabric
        exc.url               # URL (API endpoint)
        exc.http_method       # HTTP method (e.g. "POST")
        exc.http_headers      # Response headers
        exc.http_code         # Status code (e.g. 200)

        # You can inspect the C8 Data Fabric response directly.
        response = exc.response
        response.method       # HTTP method (e.g. "POST")
        response.headers      # Response headers
        response.url          # Full request URL
        response.is_success   # Set to True if HTTP code is 2XX
        response.body         # JSON-deserialized response body
        response.raw_body     # Raw string response body
        response.status_text  # Status text (e.g "OK")
        response.status_code  # Status code (e.g. 200)
        response.error_code   # Error code from C8 Data Fabric

        # You can also inspect the request sent to C8 Data Fabric.
        request = exc.request
        request.method        # HTTP method (e.g. "post")
        request.endpoint      # API endpoint starting with "/_api"
        request.headers       # Request headers
        request.params        # URL parameters
        request.data          # Request payload
        request.read          # Read collections (used for transactions only)
        request.write         # Write collections (used for transactions only)
        request.command       # C8Sh command (used for transactions only)

See :ref:`Response` and :ref:`Request` for reference.

Client Errors
=============

:class:`c8.exceptions.C8ClientError` exceptions originate from
pyC8 client itself. They do not contain error codes nor HTTP request
response details.

**Example:**

.. testcode::

    from c8 import C8Client, C8ClientError, DocumentParseError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = db.collection('students')

    try:
        students.get({'_id': 'invalid_id'})  # malformed document

    except DocumentParseError as exc:

        assert isinstance(exc, C8ClientError)
        assert exc.source == 'client'

        # Only the error message is set.
        error_message = exc.message
        assert exc.error_code is None
        assert exc.error_message is None
        assert exc.url is None
        assert exc.http_method is None
        assert exc.http_code is None
        assert exc.http_headers is None
        assert exc.response is None
        assert exc.request is None

Exceptions
==========

Below are all exceptions from pyC8.

.. automodule:: c8.exceptions
    :members:
