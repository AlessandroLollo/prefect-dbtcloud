"""
Tasks to interact with dbt Cloud
"""
import os
from typing import Dict, List, Optional

from prefect import get_run_logger, task

from prefect_dbtcloud.exceptions import (
    DbtCloudConfigurationException,
    DbtCloudListArtifactsFailed,
)
from prefect_dbtcloud.utils import dbtCloudClient


@task
def create_job(
    execute_steps: List[str],
    environment_id: int,
    project_id: int,
    name: str,
    api_domain: Optional[str] = None,
    account_id: Optional[int] = None,
    token: Optional[str] = None,
    account_id_env_var_name: Optional[str] = "ACCOUNT_ID",
    token_env_var_name: Optional[str] = "DBT_CLOUD_TOKEN",
    dbt_version: Optional[str] = None,
    triggers: Optional[Dict] = None,
    settings: Optional[Dict] = None,
    generate_docs: Optional[bool] = False,
    schedule: Optional[Dict] = None,
) -> Dict:
    """
    Create a dbt Cloud job.

    Args:
        execute_steps:
            a `List` of dbt commands that the job
            will execute.
        environment_id:
            the ID of the environment that will be used
            to run the job.
        project_id:
            the ID of the project where the job will
            be created.
        name:
            the name of the job.
        api_domain:
            Custom domain for API call.
        account_id:
            dbt Cloud account ID.
            Can also be passed as an env var.
        token:
            dbt Cloud token.
            Please note that this token must have access at least
            to the dbt Trigger Job API.
        account_id_env_var_name:
            the name of the env var that contains the dbt Cloud account ID.
            Defaults is `'ACCOUNT_ID'`.
            Used only if `account_id` is `None`.
        token_env_var_name:
            the name of the env var that contains the dbt Cloud token
            Default to `'DBT_CLOUD_TOKEN'`.
            Used only if `token` is `None`.
        dbt_version:
            the dbt version to use to run the job.
            If provided, it will override the one defined
            in the environment used to run the job.
        triggers: an object describing which trigger types
            will be enabled for the job.
        settings: an object containing settings to be applied
            to the job when running.
        generate_docs: whether to run `dbt docs generate` or not,
            after the job has been executed.
        schedule: an object that contains the run schedule
            specification for the job.

    Raises:
        DbtCloudConfigurationException:
            if `execute_steps` is not specified.
        DbtCloudConfigurationException:
            if the `environment_id` is not specified.
        DbtCloudConfigurationException:
            if the `project_id` is not specified.
        DbtCloudConfigurationException:
            if the `name` is not specified.
        DbtCloudConfigurationException:
            if the `account_id` is not specified.
        DbtCloudConfigurationException:
            if the `token` is not specified.

    Returns:
        When `wait_for_job_run_completion` is `False`, then returns
            the trigger run result.
            The trigger run result is the dict under the `data` key.
            Please refer to the dbt Cloud Trigger Run API documentation
            for more information regarding the returned payload.
            When `wait_for_job_run_completion` is `True`, then returns
            the get job result.
            The get job result is the dict under the `data` key.
            Links to the dbt artifacts are also included under the `artifact_urls` key.
            Please refere to the dbt Cloud Get Run API documentation
            for more information regarding the returned payload.
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

    if not execute_steps:
        raise DbtCloudConfigurationException(
            """
            Steps executed by dbt Cloud job cannot be None or empty.
            Please provide the steps the job will execute.
            """
        )

    if not project_id:
        raise DbtCloudConfigurationException(
            """
            dbt Cloud Project ID cannot be None.
            Please provide an Project ID or the name of the env var that contains it.
            """
        )

    if not environment_id:
        raise DbtCloudConfigurationException(
            """
            dbt Cloud Environment ID cannot be None.
            Please provide an Environment ID or the name of the env var
            that contains it.
            """
        )

    if not name:
        raise DbtCloudConfigurationException(
            """
            dbt Cloud Job name cannot be None.
            Please provide an Job name or the name of the env var that contains it.
            """
        )

    dbt_cloud_client = dbtCloudClient(
        account_id=account_id, token=token, api_domain=api_domain
    )

    return dbt_cloud_client.create_job(
        project_id=project_id,
        environment_id=environment_id,
        name=name,
        execute_steps=execute_steps,
        dbt_version=dbt_version,
        triggers=triggers,
        settings=settings,
        generate_docs=generate_docs,
        schedule=schedule,
    )


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
        api_domain: Custom domain for API call.
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
            Defaults is `'ACCOUNT_ID'`.
            Used only if `account_id` is `None`.
        job_id_env_var_name:
            the name of the env var that contains the dbt Cloud job ID
            Default to `'JOB_ID'`.
            Used only if `job_id` is `None`.
        token_env_var_name:
            the name of the env var that contains the dbt Cloud token
            Default to `'DBT_CLOUD_TOKEN'`.
            Used only if `token` is `None`.
        wait_for_job_run_completion:
            Whether the task should wait for the job run completion or not.
            Default is `False`.
        max_wait_time: The number of seconds to wait for the dbt Cloud
            job to finish.
            Used only if `wait_for_job_run_completion` is `True`.

    Raises:
        DbtCloudConfigurationException:
            if the `account_id` is not specified.
        DbtCloudConfigurationException:
            if the `job_id` is not specified.
        DbtCloudConfigurationException:
            if the `token` is not specified.
        DbtCloudConfigurationException:
            if the `cause` is not specified.

    Returns:
        When `wait_for_job_run_completion` is `False`, then returns
            the trigger run result.
            The trigger run result is the dict under the `data` key.
            Please refer to the dbt Cloud Trigger Run API documentation
            for more information regarding the returned payload.
            When `wait_for_job_run_completion` is `True`, then returns
            the get job result.
            The get job result is the dict under the `data` key.
            Links to the dbt artifacts are also included under the `artifact_urls` key.
            Please refere to the dbt Cloud Get Run API documentation
            for more information regarding the returned payload.
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
        except DbtCloudListArtifactsFailed as err:
            logger = get_run_logger()
            logger.warning(
                f"Unable to retrieve artifacts generated by dbt Cloud job run: {err}"
            )

        job_run_result["artifact_urls"] = [link for link, _ in artifact_links]

        return job_run_result

    else:
        return job_run
