"""
Tasks to interact with dbt Cloud
"""
import os
from typing import Dict, Optional

from prefect import get_run_logger, task

from prefect_dbtcloud.exceptions import (
    DbtCloudConfigurationException,
    DbtCloudListArtifactsFailed,
)
from prefect_dbtcloud.utils import dbtCloudClient


@task
def run_job(
    cause: str,
    api_domain: Optional[str] = None,
    account_id: Optional[int] = None,
    job_id: Optional[int] = None,
    token: Optional[str] = None,
    additional_args: Optional[Dict] = None,
    account_id_env_var_name: Optional[str] = "ACCOUNT_ID",
    job_id_env_var_name: Optional[str] = "JOB_ID",
    token_env_var_name: Optional[str] = "DBT_CLOUD_TOKEN",
    wait_for_job_run_completion: Optional[bool] = False,
    max_wait_time: Optional[int] = None,
) -> Dict:
    """
    Run a dbt Cloud job.

    Args:
        cause: A string describing the reason for triggering the job run.
        api_domain (str, optional): Custom domain for API call.
        account_id: dbt Cloud account ID.
            Can also be passed as an env var.
        job_id: dbt Cloud job ID
        token: dbt Cloud token.
            Please note that this token must have access at least
            to the dbt Trigger Job API.
        additional_args: additional information to pass to the Trigger Job API.
            For a list of the possible information,
            have a look at:
            https://docs.getdbt.com/dbt-cloud/api-v2#operation/triggerRun
        account_id_env_var_name:
            the name of the env var that contains the dbt Cloud account ID.
            Defaults to DBT_CLOUD_ACCOUNT_ID.
            Used only if account_id is None.
        job_id_env_var_name:
            the name of the env var that contains the dbt Cloud job ID
            Default to DBT_CLOUD_JOB_ID.
            Used only if job_id is None.
        token_env_var_name:
            the name of the env var that contains the dbt Cloud token
            Default to DBT_CLOUD_TOKEN.
            Used only if token is None.
        wait_for_job_run_completion:
            Whether the task should wait for the job run completion or not.
            Default to False.
        max_wait_time: The number of seconds to wait for the dbt Cloud
            job to finish.
            Used only if wait_for_job_run_completion = True.

    Returns:
        if wait_for_job_run_completion = False, then returns the trigger run result.
            The trigger run result is the dict under the "data" key.
            Have a look at the Response section at:
            https://docs.getdbt.com/dbt-cloud/api-v2#operation/triggerRun

        if wait_for_job_run_completion = True, then returns the get job result.
            The get job result is the dict under the "data" key.
            Links to the dbt artifacts are
            also included under the `artifact_urls` key.
            Have a look at the Response section at:
            https://docs.getdbt.com/dbt-cloud/api-v2#operation/getRunById

    Raises:

    """
    if account_id is None and account_id_env_var_name in os.environ:
        account_id = int(os.environ[account_id_env_var_name])

    if account_id is None:
        raise DbtCloudConfigurationException(
            """
            dbt Cloud Account ID cannot be None.
            Please provide an Account ID or the name of the env var that contains it.
            """
        )

    if job_id is None and job_id_env_var_name in os.environ:
        job_id = int(os.environ[job_id_env_var_name])

    if job_id is None:
        raise DbtCloudConfigurationException(
            """
            dbt Cloud Job ID cannot be None.
            Please provide a Job ID or the name of the env var that contains it.
            """
        )

    if api_domain is None:
        api_domain = "cloud.getdbt.com"

    if token is None and token_env_var_name in os.environ:
        token = os.environ.get(token_env_var_name)

    if token is None:
        raise DbtCloudConfigurationException(
            """
            dbt Cloud token cannot be None.
            Please provide a token or the name of the env var that contains it.
            """
        )

    if cause is None:
        raise DbtCloudConfigurationException(
            """
            Cause cannot be None.
            Please provide a cause to trigger the dbt Cloud job.
            """
        )

    dbt_cloud_client = dbtCloudClient(
        account_id=account_id, token=token, api_domain=api_domain
    )

    job_run = dbt_cloud_client.trigger_job_run(
        job_id=job_id,
        cause=cause,
        additional_args=additional_args,
    )

    if wait_for_job_run_completion:

        job_run_id = job_run["id"]
        job_run_result = dbt_cloud_client.wait_for_job_run(
            run_id=job_run_id,
            max_wait_time=max_wait_time,
        )

        artifact_links = []
        try:
            artifact_links = dbt_cloud_client.list_run_artifact_links(run_id=job_run_id)

            # Note: artifacts do not exist in Prefect 2.0
            # markdown = f"Artifacts for dbt Cloud run {job_run_id} of job {job_id}\n"
            # for link, name in artifact_links:
            #     markdown += f"- [{name}]({link})\n"
            # create_markdown_artifact(markdown)

        except DbtCloudListArtifactsFailed as err:
            logger = get_run_logger()
            logger.warning(
                f"Unable to retrieve artifacts generated by dbt Cloud job run: {err}"
            )

        job_run_result["artifact_urls"] = [link for link, _ in artifact_links]

        return job_run_result

    else:
        return job_run
