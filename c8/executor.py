from __future__ import absolute_import, unicode_literals

from collections import OrderedDict
from uuid import uuid4

from c8.exceptions import AsyncExecuteError, BatchExecuteError, BatchStateError
from c8.job import AsyncJob, BatchJob
from c8.request import Request
from c8.response import Response
from c8.utils import suppress_warning

__all__ = [
    "DefaultExecutor",
    "AsyncExecutor",
    "BatchExecutor",
]


class Executor(object):  # pragma: no cover
    """Base class for API executors.

    API executors dictate how API requests are executed depending on the
    execution context (i.e. "default", "async", "batch", "transaction").

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    context = None

    def __init__(self, connection):
        self._conn = connection

    def execute(self, request, response_handler):
        """Execute an API request.

        :param request: HTTP request.
        :type request: c8.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result or job.
        :rtype: str | unicode | bool | int | list | dict | c8.job.Job
        """
        raise NotImplementedError


class DefaultExecutor(Executor):
    """Default API executor.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    context = "default"

    def __init__(self, connection):
        super(DefaultExecutor, self).__init__(connection)

    def execute(self, request, response_handler, custom_prefix=None):
        """Execute an API request and return the result.

        :param request: HTTP request.
        :type request: c8.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :param custom_prefix: Custom url-path value
        :type custom_prefix: str
        :return: API execution result.
        :rtype: str | unicode | bool | int | list | dict
        """
        response = self._conn.send_request(request, custom_prefix=custom_prefix)
        return response_handler(response)


class AsyncExecutor(Executor):
    """Async API Executor.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param return_result: If set to True, API executions return instances of
        :class:`c8.job.AsyncJob` and results can be retrieved from server
        once available. If set to False, API executions return None and no
        results are stored on server.
    :type return_result: bool
    """

    context = "async"

    def __init__(self, connection, return_result):
        super(AsyncExecutor, self).__init__(connection)
        self._return_result = return_result

    def execute(self, request, response_handler):
        """Execute an API request asynchronously.

        :param request: HTTP request.
        :type request: c8.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: Async job or None if **return_result** parameter was set to
            False during initialization.
        :rtype: c8.job.AsyncJob | None
        """
        if self._return_result:
            request.headers["x-c8-async"] = "store"
        else:
            request.headers["x-c8-async"] = "true"

        resp = self._conn.send_request(request)
        if not resp.is_success:
            raise AsyncExecuteError(resp, request)
        if not self._return_result:
            return None

        job_id = resp.headers["x-c8-async-id"]
        return AsyncJob(self._conn, job_id, response_handler)


class BatchExecutor(Executor):
    """Batch API executor.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param return_result: If set to True, API executions return instances of
        :class:`c8.job.BatchJob` that are populated with results on commit.
        If set to False, API executions return None and no results are tracked
        client-side.
    :type return_result: bool
    """

    context = "batch"

    def __init__(self, connection, return_result):
        super(BatchExecutor, self).__init__(connection)
        self._return_result = return_result
        self._queue = OrderedDict()
        self._committed = False

    @property
    def jobs(self):
        """Return the queued batch jobs.

        :return: Batch jobs or None if **return_result** parameter was set to
            False during initialization.
        :rtype: [c8.job.BatchJob] | None
        """
        if not self._return_result:
            return None
        return [job for _, job in self._queue.values()]

    def execute(self, request, response_handler):
        """Place the request in the batch queue.

        :param request: HTTP request.
        :type request: c8.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: Batch job or None if **return_result** parameter was set to
            False during initialization.
        :rtype: c8.job.BatchJob | None
        :raise c8.exceptions.BatchStateError: If batch was already
            committed.
        """
        if self._committed:
            raise BatchStateError("batch already committed")

        job = BatchJob(response_handler)
        self._queue[job.id] = (request, job)
        return job if self._return_result else None

    def commit(self):
        """Execute the queued requests in a single batch API request.

        If **return_result** parameter was set to True during initialization,
        :class:`c8.job.BatchJob` instances are populated with results.

        :return: Batch jobs or None if **return_result** parameter was set to
            False during initialization.
        :rtype: [c8.job.BatchJob] | None
        :raise c8.exceptions.BatchStateError: If batch state is invalid
            (e.g. batch was already committed or size of response from server
            did not match the expected).
        :raise c8.exceptions.BatchExecuteError: If commit fails.
        """
        if self._committed:
            raise BatchStateError("batch already committed")

        self._committed = True

        if len(self._queue) == 0:
            return self.jobs

        # Boundary used for multipart request
        boundary = uuid4().hex

        # Buffer for building the batch request payload
        buffer = []
        for req, job in self._queue.values():
            buffer.append("--{}".format(boundary))
            buffer.append("Content-Type: application/x-c8-batchpart")
            buffer.append("Content-Id: {}".format(job.id))
            buffer.append("\r\n{}".format(req))
        buffer.append("--{}--".format(boundary))

        request = Request(
            method="post",
            endpoint="/batch",
            headers={
                "Content-Type": "multipart/form-data; boundary={}".format(boundary)
            },
            data="\r\n".join(buffer),
        )
        with suppress_warning("requests.packages.urllib3.connectionpool"):
            resp = self._conn.send_request(request)

        if not resp.is_success:
            raise BatchExecuteError(resp, request)

        if not self._return_result:
            return None

        raw_resps = resp.raw_body.split("--{}".format(boundary))[1:-1]
        if len(self._queue) != len(raw_resps):
            raise BatchStateError(
                "expecting {} parts in batch response but got {}".format(
                    len(self._queue), len(raw_resps)
                )
            )
        for raw_resp in raw_resps:
            # Parse and breakdown the batch response body
            resp_parts = raw_resp.strip().split("\r\n")
            raw_content_id = resp_parts[1]
            raw_body = resp_parts[-1]
            raw_status = resp_parts[3]
            job_id = raw_content_id.split(" ")[1]
            _, status_code, status_text = raw_status.split(" ", 2)

            # Update the corresponding batch job
            queued_req, queued_job = self._queue[job_id]
            queued_job._response = Response(
                method=queued_req.method,
                url=self._conn.url_prefix + queued_req.endpoint,
                headers={},
                status_code=int(status_code),
                status_text=status_text,
                raw_body=raw_body,
            )
            queued_job._status = "done"

        return self.jobs
