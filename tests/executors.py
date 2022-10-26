import time

from c8.executor import AsyncExecutor, BatchExecutor
from c8.job import BatchJob


class TestAsyncExecutor(AsyncExecutor):
    def __init__(self, connection):
        super(TestAsyncExecutor, self).__init__(
            connection=connection, return_result=True
        )

    def execute(self, request, response_handler):
        job = AsyncExecutor.execute(self, request, response_handler)
        while job.status() != "done":
            time.sleep(0.01)
        return job.result()


class TestBatchExecutor(BatchExecutor):
    def __init__(self, connection):
        super(TestBatchExecutor, self).__init__(
            connection=connection, return_result=True
        )

    def execute(self, request, response_handler):
        self._committed = False
        self._queue.clear()

        job = BatchJob(response_handler)
        self._queue[job.id] = (request, job)
        self.commit()
        return job.result()
