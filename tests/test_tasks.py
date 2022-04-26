import os
from unittest import mock

import pytest
import responses
from prefect import flow
from responses import matchers

from prefect_dbtcloud.exceptions import (
    DbtCloudConfigurationException,
    DbtCloudRunCanceled,
    DbtCloudRunFailed,
    DbtCloudRunTimedOut,
    GetDbtCloudRunFailed,
    TriggerDbtCloudRunFailed,
)
from prefect_dbtcloud.tasks import run_job
from prefect_dbtcloud.utils import dbtCloudClient


def test_run_without_account_id_raises():
    @flow
    def test_flow():
        return run_job(cause="cause")

    msg_match = "dbt Cloud Account ID cannot be None."
    with pytest.raises(DbtCloudConfigurationException, match=msg_match):
        test_flow().result().result()


def test_run_without_job_id_raises():
    @flow
    def test_flow():
        return run_job(cause="cause", account_id=123)

    msg_match = "dbt Cloud Job ID cannot be None."
    with pytest.raises(DbtCloudConfigurationException, match=msg_match):
        test_flow().result().result()


def test_run_without_token_raises():
    @flow
    def test_flow():
        return run_job(cause="cause", account_id=123, job_id=123)

    msg_match = "dbt Cloud token cannot be None."
    with pytest.raises(DbtCloudConfigurationException, match=msg_match):
        test_flow().result().result()


def test_run_with_cause_none_raises():
    @flow
    def test_flow():
        return run_job(cause=None, account_id=123, job_id=123, token="abc")

    msg_match = "Cause cannot be None."
    with pytest.raises(DbtCloudConfigurationException, match=msg_match):
        test_flow().result().result()


@responses.activate
def test_run_job_failed_raises():
    account_id = 123
    job_id = 123
    url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"

    responses.add(
        responses.POST,
        url,
        status=123,
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(cause="abc", account_id=account_id, job_id=job_id, token="abc")

    with pytest.raises(TriggerDbtCloudRunFailed):
        test_flow().result().result()


@responses.activate
def test_run_job_with_wait_failed_raises():
    account_id = 123
    job_id = 123
    trigger_url = (
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    )
    get_run_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/123/"

    responses.add(
        responses.POST,
        trigger_url,
        status=200,
        json={"data": {"id": 123}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        get_run_url,
        status=123,
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="abc",
            account_id=account_id,
            job_id=job_id,
            token="abc",
            wait_for_job_run_completion=True,
        )

    with pytest.raises(GetDbtCloudRunFailed):
        test_flow().result().result()


@responses.activate
def test_run_job_with_wait_status_20_raises():
    account_id = 123
    job_id = 123
    trigger_url = (
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    )
    get_run_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/123/"

    responses.add(
        responses.POST,
        trigger_url,
        status=200,
        json={"data": {"id": 123}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        get_run_url,
        status=200,
        json={"data": {"status": 20, "finished_at": "2019-08-24T14:15:22Z"}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="abc",
            account_id=account_id,
            job_id=job_id,
            token="abc",
            wait_for_job_run_completion=True,
        )

    with pytest.raises(DbtCloudRunFailed):
        test_flow().result().result()


@responses.activate
def test_run_job_with_wait_status_30_raises():
    account_id = 123
    job_id = 123
    trigger_url = (
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    )
    get_run_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/123/"

    responses.add(
        responses.POST,
        trigger_url,
        status=200,
        json={"data": {"id": 123}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        get_run_url,
        status=200,
        json={"data": {"status": 30, "finished_at": "2019-08-24T14:15:22Z"}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="abc",
            account_id=account_id,
            job_id=job_id,
            token="abc",
            wait_for_job_run_completion=True,
        )

    with pytest.raises(DbtCloudRunCanceled):
        test_flow().result().result()


@responses.activate
def test_run_job_timeout_expires_raises():
    account_id = 123
    job_id = 123
    trigger_url = (
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    )
    get_run_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/123/"

    responses.add(
        responses.POST,
        trigger_url,
        status=200,
        json={"data": {"id": 123}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        get_run_url,
        status=200,
        json={"data": {"finished_at": None}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="abc",
            account_id=account_id,
            job_id=job_id,
            token="abc",
            wait_for_job_run_completion=True,
            max_wait_time=5,
        )

    with pytest.raises(DbtCloudRunTimedOut):
        test_flow().result().result()


@responses.activate
def test_run_job_trigger_job():
    account_id = 123
    job_id = 123
    url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"

    responses.add(
        responses.POST,
        url,
        json={"data": {"foo": "bar"}},
        status=200,
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(cause="abc", account_id=account_id, job_id=job_id, token="abc")

    response = test_flow().result().result()

    assert response == {"foo": "bar"}


@responses.activate
def test_run_job_trigger_job_with_custom_domain():
    account_id = 123
    job_id = 123
    api_domain = "cloud.corp.getdbt.com"
    url = f"https://{api_domain}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"

    responses.add(
        responses.POST,
        url,
        json={"data": {"foo": "bar"}},
        status=200,
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="abc",
            account_id=account_id,
            job_id=job_id,
            token="abc",
            api_domain=api_domain,
        )

    response = test_flow().result().result()

    assert response == {"foo": "bar"}


@responses.activate
def test_dbt_cloud_run_job_trigger_job_with_wait():
    account_id = 1234
    job_id = 1234

    trigger_url = (
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    )
    get_run_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/123/"

    responses.add(
        responses.POST,
        trigger_url,
        status=200,
        json={"data": {"id": 123}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        get_run_url,
        status=200,
        json={"data": {"id": 1, "status": 10, "finished_at": "2019-08-24T14:15:22Z"}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/123/artifacts/",
        status=200,
        json={"data": ["manifest.json", "run_results.json", "catalog.json"]},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="foo",
            account_id=account_id,
            job_id=job_id,
            token="foo",
            wait_for_job_run_completion=True,
        )

    r = test_flow().result().result()

    assert r == {
        "id": 1,
        "status": 10,
        "finished_at": "2019-08-24T14:15:22Z",
        "artifact_urls": [
            f"{get_run_url}artifacts/manifest.json",
            f"{get_run_url}artifacts/run_results.json",
            f"{get_run_url}artifacts/catalog.json",
        ],
    }


@responses.activate
def test_dbt_cloud_run_job_trigger_job_with_wait_custom():
    account_id = 1234
    job_id = 1234

    trigger_run_url = (
        f"https://cloud.corp.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    )

    get_run_url = f"https://cloud.corp.getdbt.com/api/v2/accounts/{account_id}/runs/1/"

    responses.add(
        responses.POST,
        trigger_run_url,
        status=200,
        json={"data": {"id": 1}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        get_run_url,
        status=200,
        json={"data": {"id": 1, "status": 10, "finished_at": "2019-08-24T14:15:22Z"}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        f"https://cloud.corp.getdbt.com/api/v2/accounts/{account_id}/runs/1/artifacts/",
        status=200,
        json={"data": ["manifest.json", "run_results.json", "catalog.json"]},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="foo",
            account_id=account_id,
            job_id=job_id,
            token="foo",
            wait_for_job_run_completion=True,
            api_domain="cloud.corp.getdbt.com",
        )

    r = test_flow().result().result()

    assert r == {
        "id": 1,
        "status": 10,
        "finished_at": "2019-08-24T14:15:22Z",
        "artifact_urls": [
            f"{get_run_url}artifacts/manifest.json",
            f"{get_run_url}artifacts/run_results.json",
            f"{get_run_url}artifacts/catalog.json",
        ],
    }


@responses.activate
def test_dbt_cloud_run_job_trigger_job_with_fail_on_artifacts():
    account_id = 1234
    job_id = 1234

    responses.add(
        responses.POST,
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/",
        status=200,
        json={"data": {"id": 1}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/1/",
        status=200,
        json={"data": {"id": 1, "status": 10, "finished_at": "2019-08-24T14:15:22Z"}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    responses.add(
        responses.GET,
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/1/artifacts/",
        status=123,
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    @flow
    def test_flow():
        return run_job(
            cause="foo",
            account_id=account_id,
            job_id=job_id,
            token="foo",
            wait_for_job_run_completion=True,
            api_domain="cloud.getdbt.com",
        )

    r = test_flow().result().result()

    assert r == {
        "id": 1,
        "status": 10,
        "finished_at": "2019-08-24T14:15:22Z",
        "artifact_urls": [],
    }


@responses.activate
@mock.patch.dict(os.environ, {"ACCT_ID": "123", "JOB": "123", "TKN": "abc"})
def test_run_job_with_env_vars():

    responses.add(
        responses.POST,
        "https://cloud.getdbt.com/api/v2/accounts/123/jobs/123/run/",
        status=200,
        json={"data": {"id": 1}},
        match=[matchers.header_matcher(dbtCloudClient.get_agent_header())],
    )

    account_id_env_var_name = "ACCT_ID"
    job_id_env_var_name = "JOB"
    token_env_var_name = "TKN"

    @flow
    def test_flow():
        return run_job(
            account_id_env_var_name=account_id_env_var_name,
            job_id_env_var_name=job_id_env_var_name,
            token_env_var_name=token_env_var_name,
            wait_for_job_run_completion=False,
            cause="test",
        )

    r = test_flow().result().result()

    assert r == {"id": 1}
