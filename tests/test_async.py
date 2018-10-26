from __future__ import absolute_import, unicode_literals

import time

import pytest
from six import string_types

from c8.fabric import AsyncFabric
from c8.exceptions import (
    AsyncExecuteError,
    AsyncJobClearError,
    AsyncJobResultError,
    AsyncJobStatusError,
    AsyncJobListError,
    AsyncJobCancelError,
    C8QLQueryExecuteError
)
from c8.job import AsyncJob
from tests.helpers import extract


def wait_on_job(job):
    """Block until the async job is done."""
    while job.status() != 'done':
        time.sleep(.05)  # pragma: no cover
    return job


def wait_on_jobs(fabric):
    """Block until all async jobs are finished."""
    while len(fabric.async_jobs('pending')) > 0:
        time.sleep(.05)  # pragma: no cover


def test_async_wrapper_attributes(fabric, col, username):
    async_fabric = fabric.begin_async_execution()
    assert isinstance(async_fabric, AsyncFabric)
    assert async_fabric.username == username
    assert async_fabric.context == 'async'
    assert async_fabric.fabric_name == fabric.name
    assert async_fabric.name == fabric.name
    assert repr(async_fabric) == '<AsyncFabric {}>'.format(fabric.name)

    async_col = async_fabric.collection(col.name)
    assert async_col.username == username
    assert async_col.context == 'async'
    assert async_col.fabric_name == fabric.name
    assert async_col.name == col.name

    async_c8ql = async_fabric.c8ql
    assert async_c8ql.username == username
    assert async_c8ql.context == 'async'
    assert async_c8ql.fabric_name == fabric.name

    job = async_c8ql.execute('INVALID QUERY')
    assert isinstance(job, AsyncJob)
    assert isinstance(job.id, string_types)
    assert repr(job) == '<AsyncJob {}>'.format(job.id)


def test_async_execute_without_result(fabric, col, docs):
    # Insert test documents asynchronously with return_result set to False
    async_col = fabric.begin_async_execution(return_result=False).collection(
        col.name)

    # Ensure that no jobs were returned
    assert async_col.insert(docs[0]) is None
    assert async_col.insert(docs[1]) is None
    assert async_col.insert(docs[2]) is None

    # Ensure that the operations went through
    wait_on_jobs(fabric)
    assert extract('_key', col.all()) == ['1', '2', '3']


def test_async_execute_error_in_result(fabric, col, docs):
    fabric.collection(col.name).import_bulk(docs)
    async_fabric = fabric.begin_async_execution(return_result=True)

    # Test async execution of a bad C8QL query
    job = wait_on_job(async_fabric.c8ql.execute('INVALID QUERY'))
    with pytest.raises(C8QLQueryExecuteError) as err:
        job.result()
    assert err.value.error_code == 1501


def test_async_get_job_status(fabric, bad_fabric):
    async_fabric = fabric.begin_async_execution(return_result=True)

    # Test get status of a pending job
    job = async_fabric.c8ql.execute('RETURN SLEEP(0.1)', count=True)
    assert job.status() == 'pending'

    # Test get status of a finished job
    assert wait_on_job(job).status() == 'done'
    assert job.result().count() == 1

    # Test get status of a missing job
    with pytest.raises(AsyncJobStatusError) as err:
        job.status()
    assert err.value.error_code == 404

    # Test get status from invalid job
    bad_job = wait_on_job(async_fabric.c8ql.execute('INVALID QUERY'))
    bad_job._conn = bad_fabric._conn
    with pytest.raises(AsyncJobStatusError) as err:
        bad_job.status()
    assert err.value.error_code == 1228


def test_async_get_job_result(fabric, bad_fabric):
    async_fabric = fabric.begin_async_execution(return_result=True)

    # Test get result from a pending job
    job = async_fabric.c8ql.execute('RETURN SLEEP(0.1)', count=True)
    with pytest.raises(AsyncJobResultError) as err:
        job.result()
    assert err.value.http_code == 204
    assert '{} not done'.format(job.id) in str(err.value)

    # Test get result from a finished job
    assert wait_on_job(job).result().count() == 1

    # Test get result from a cleared job
    with pytest.raises(AsyncJobResultError) as err:
        job.result()
    assert err.value.error_code == 404

    # Test get result from an invalid job
    bad_job = async_fabric.c8ql.execute('INVALID QUERY')
    bad_job._conn = bad_fabric._conn
    with pytest.raises(AsyncJobResultError) as err:
        bad_job.result()
    assert err.value.error_code == 1228


def test_async_cancel_job(fabric, bad_fabric):
    async_fabric = fabric.begin_async_execution(return_result=True)

    # Start a long running request to ensure that job can be cancelled
    job = async_fabric.c8ql.execute('RETURN SLEEP(5)')

    # Test cancel a pending job
    assert job.cancel() is True

    # Test cancel a missing job
    job._id = 'invalid_id'
    with pytest.raises(AsyncJobCancelError) as err:
        job.cancel(ignore_missing=False)
    assert err.value.error_code == 404
    assert job.cancel(ignore_missing=True) is False

    # Test cancel an invalid job
    job = async_fabric.c8ql.execute('RETURN SLEEP(5)')
    job._conn = bad_fabric._conn
    with pytest.raises(AsyncJobCancelError) as err:
        job.cancel()
    assert err.value.error_code == 1228


def test_async_clear_job(fabric, bad_fabric):
    async_fabric = fabric.begin_async_execution(return_result=True)

    job = async_fabric.c8ql.execute('RETURN 1')

    # Test clear finished job
    assert job.clear(ignore_missing=True) is True

    # Test clear missing job
    with pytest.raises(AsyncJobClearError) as err:
        job.clear(ignore_missing=False)
    assert err.value.error_code == 404
    assert job.clear(ignore_missing=True) is False

    # Test clear with an invalid job
    job._conn = bad_fabric._conn
    with pytest.raises(AsyncJobClearError) as err:
        job.clear()
    assert err.value.error_code == 1228


def test_async_execute_errors(bad_fabric):
    bad_async_fabric = bad_fabric.begin_async_execution(return_result=False)
    with pytest.raises(AsyncExecuteError) as err:
        bad_async_fabric.c8ql.execute('RETURN 1')
    assert err.value.error_code == 1228

    bad_async_fabric = bad_fabric.begin_async_execution(return_result=True)
    with pytest.raises(AsyncExecuteError) as err:
        bad_async_fabric.c8ql.execute('RETURN 1')
    assert err.value.error_code == 1228


def test_async_clear_jobs(fabric, bad_fabric, col, docs):
    async_fabric = fabric.begin_async_execution(return_result=True)
    async_col = async_fabric.collection(col.name)

    job1 = wait_on_job(async_col.insert(docs[0]))
    job2 = wait_on_job(async_col.insert(docs[1]))
    job3 = wait_on_job(async_col.insert(docs[2]))

    # Test clear all async jobs
    assert fabric.clear_async_jobs() is True
    for job in [job1, job2, job3]:
        with pytest.raises(AsyncJobStatusError) as err:
            job.status()
        assert err.value.error_code == 404

    # Set up test documents again
    job1 = wait_on_job(async_col.insert(docs[0]))
    job2 = wait_on_job(async_col.insert(docs[1]))
    job3 = wait_on_job(async_col.insert(docs[2]))

    # Test clear jobs that have expired
    past = int(time.time()) - 1000000
    assert fabric.clear_async_jobs(threshold=past) is True
    for job in [job1, job2, job3]:
        assert job.status() == 'done'

    # Test clear jobs that have not expired yet
    future = int(time.time()) + 1000000
    assert fabric.clear_async_jobs(threshold=future) is True
    for job in [job1, job2, job3]:
        with pytest.raises(AsyncJobStatusError) as err:
            job.status()
        assert err.value.error_code == 404

    # Test clear job with bad fabric
    with pytest.raises(AsyncJobClearError) as err:
        bad_fabric.clear_async_jobs()
    assert err.value.error_code == 1228


def test_async_list_jobs(fabric, col, docs):
    async_fabric = fabric.begin_async_execution(return_result=True)
    async_col = async_fabric.collection(col.name)

    job1 = wait_on_job(async_col.insert(docs[0]))
    job2 = wait_on_job(async_col.insert(docs[1]))
    job3 = wait_on_job(async_col.insert(docs[2]))

    # Test list async jobs that are done
    job_ids = fabric.async_jobs(status='done')
    assert job1.id in job_ids
    assert job2.id in job_ids
    assert job3.id in job_ids

    # Test list async jobs that are pending
    job4 = async_fabric.c8ql.execute('RETURN SLEEP(0.1)')
    assert fabric.async_jobs(status='pending') == [job4.id]
    wait_on_job(job4)  # Make sure the job is done

    # Test list async jobs with invalid status
    with pytest.raises(AsyncJobListError) as err:
        fabric.async_jobs(status='bad_status')
    assert err.value.error_code == 400

    # Test list jobs with count
    job_ids = fabric.async_jobs(status='done', count=1)
    assert len(job_ids) == 1
    assert job_ids[0] in [job1.id, job2.id, job3.id, job4.id]
