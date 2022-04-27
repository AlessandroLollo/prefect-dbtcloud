"""
Utilities and client to interact with dbt Cloud.
"""
from time import sleep
from typing import Dict, List, Optional, Tuple

import prefect
from requests import Session

from prefect_dbtcloud.exceptions import (
    DbtCloudCreateJobFailed,
    DbtCloudListArtifactsFailed,
    DbtCloudRunCanceled,
    DbtCloudRunFailed,
    DbtCloudRunTimedOut,
    GetDbtCloudRunFailed,
    TriggerDbtCloudRunFailed,
)


class dbtCloudClient:
    """
    A client that exposes methods to call dbt Cloud APIs
    to trigger job runs and retrieve job run information.

    """

    # dbt Cloud Create Job API
    # https://docs.getdbt.com/dbt-cloud/api-v2#operation/createJob
    __DBT_CLOUD_CREATE_JOB_API_ENDPOINT_V2 = (
        "https://{apiDomain}/api/v2/accounts/{accountId}/jobs/"
    )

    # dbt Cloud Trigger Job API
    # https://docs.getdbt.com/dbt-cloud/api-v2#operation/triggerRun
    __DBT_CLOUD_TRIGGER_JOB_API_ENDPOINT_V2 = (
        "https://{apiDomain}/api/v2/accounts/{accountId}/jobs/{jobId}/run/"
    )

    # dbt Cloud Get Run API
    # https://docs.getdbt.com/dbt-cloud/api-v2#operation/getRunById
    __DBT_CLOUD_GET_RUN_API_ENDPOINT_V2 = (
        "https://{apiDomain}/api/v2/accounts/{accountId}/runs/{runId}/"
    )

    # dbt Cloud List Run Artifacts API
    # https://docs.getdbt.com/dbt-cloud/api-v2#operation/listArtifactsByRunId
    __DBT_CLOUD_LIST_RUN_ARTIFACTS_ENDPOINT_V2 = (
        "https://{apiDomain}/api/v2/accounts/{accountId}/runs/{runId}/artifacts/"
    )

    # dbt Cloud Get Run Artifact API
    # https://docs.getdbt.com/dbt-cloud/api-v2#operation/getArtifactsByRunId
    __DBT_CLOUD_GET_RUN_ARTIFACT_ENDPOINT_V2 = (
        "https://{apiDomain}/api/v2/accounts/{accountId}/runs/{runId}/artifacts/{path}"
    )

    __USER_AGENT_HEADER = {"user-agent": f"prefect-{prefect.__version__}"}

    def __init__(
        self,
        account_id: int,
        token: str,
        api_domain: Optional[str] = "cloud.getdbt.com",
    ) -> "dbtCloudClient":
        """
        Build a `dbtCloudClient` object.

        Args:
            account_id: The identifier of the dbt Cloud account.
            token: The API token to use to authenticate on dbt Cloud.
            api_domain: The URL of the dbt Cloud account.
                Defaults to "cloud.getdbt.com"

        Returns:
            A `dbtCloudClient` configured to interact with the
            specified dbt Cloud account
        """
        self.account_id = account_id
        self.api_domain = api_domain
        self.token = token
        self.session = Session()
        self.session.headers = {
            "Authorization": f"Bearer {self.token}",
            **self.__USER_AGENT_HEADER,
        }

    @classmethod
    def get_agent_header(cls) -> Dict:
        """
        Return the user agent header
        the client uses when performing API requests
        to dbt Cloud.

        Returns:
            User agent header
        """
        return cls.__USER_AGENT_HEADER

    def dbt_cloud_create_job_api_endpoint_v2(self) -> str:
        """
        Return the URL of the Create Job API.

        Returns:
            The URL of the Create Job API.
        """
        return self.__DBT_CLOUD_CREATE_JOB_API_ENDPOINT_V2.format(
            apiDomain=self.api_domain, accountId=self.account_id
        )

    def dbt_cloud_trigger_job_api_endpoint_v2(self, job_id: int) -> str:
        """
        Return the URL of the Trigger Job API.

        Args:
            job_id: The identifier of the job to trigger.

        Returns:
            The URL of the Trigger Job API for the given `job_id`.
        """
        return self.__DBT_CLOUD_TRIGGER_JOB_API_ENDPOINT_V2.format(
            apiDomain=self.api_domain, accountId=self.account_id, jobId=job_id
        )

    def dbt_cloud_get_run_api_endpoint_v2(self, run_id: int) -> str:
        """
        Return the URL of the Get Run API.

        Args:
            run_id: the identifier of the job run to retrieve.

        Returns:
            The URL of the Get Run API for the given `run_id`.
        """
        return self.__DBT_CLOUD_GET_RUN_API_ENDPOINT_V2.format(
            apiDomain=self.api_domain, accountId=self.account_id, runId=run_id
        )

    def dbt_cloud_list_run_artifacts_endpoint_v2(self, run_id: int) -> str:
        """
        Return the URL of the List Run Artifacts API.

        Args:
            run_id: the identifier of the job run to retrieve.

        Returns:
            The URL of the List Run Artifacts API for the given `run_id`.
        """
        return self.__DBT_CLOUD_LIST_RUN_ARTIFACTS_ENDPOINT_V2.format(
            apiDomain=self.api_domain, accountId=self.account_id, runId=run_id
        )

    def dbt_cloud_get_run_artifact_endpoint_v2(self, run_id: int, path: str) -> str:
        """
        Return the URL of the Get Run Artifact API.

        Args:
            run_id: the identifier of the job run to retrieve.

        Returns:
            The URL of the Get Run Artifact API for the given `run_id`.
        """
        return self.__DBT_CLOUD_GET_RUN_ARTIFACT_ENDPOINT_V2.format(
            apiDomain=self.api_domain,
            accountId=self.account_id,
            runId=run_id,
            path=path,
        )

    def create_job(
        self,
        project_id: int,
        environment_id: int,
        name: str,
        execute_steps: List[str],
        dbt_version: Optional[str] = None,
        triggers: Optional[Dict] = None,
        settings: Optional[Dict] = None,
        generate_docs: Optional[bool] = False,
        schedule: Optional[Dict] = None,
    ):
        """
        Create a new dbt Cloud job.

        Args:
            project_id: the ID of the project where the
                job will be created.
            environment_id: the ID of the environment that will
                be used to run the job.
            name: the name of the job.
            execute_steps: The list of dbt commands the job
                will execute when triggered.
            dbt_version: the dbt version to use to run the job.
                If provided, it will override the one defined
                in the environment.
            triggers: an object describing which trigger types
                will be enabled for the job.
            settings: an object containing settings to be applied
                to the job when running.
            generate_docs: whether to run `dbt docs generate` or not,
                after the job has been executed.
            schedule: an object that contains the run schedule
                specification for the job.

        Returns:
            The create job result, namely the `data` key in the API response.

        Raises:
            `DbtCloudCreateJobFailed`: when the response code != 200
        """
        url = self.dbt_cloud_create_job_api_endpoint_v2()

        data = {}
        data["project_id"] = project_id
        data["environment_id"] = environment_id
        data["name"] = name
        data["execute_steps"] = execute_steps

        if dbt_version:
            data["dbt_version"] = dbt_version

        if triggers:
            data["triggers"] = triggers

        if settings:
            data["settings"] = settings

        if generate_docs:
            data["generate_docs"] = generate_docs

        if schedule:
            data["schedule"] = schedule

        with self.session.post(url, data=data) as create_job_request:

            if create_job_request.status_code != 200:
                raise DbtCloudCreateJobFailed(create_job_request.reason)

        return create_job_request.json()["data"]

    def trigger_job_run(
        self,
        job_id: int,
        cause: str,
        additional_args: Optional[Dict] = None,
    ) -> dict:
        """
        Trigger a dbt Cloud job run

        Args:
            job_id: dbt Cloud job ID
            cause: the reason describing why the job run is being triggered
            additional_args: additional information to pass to the Trigger Job Run API

        Returns:
            The trigger run result, namely the `data` key in the API response

        Raises:
            `TriggerDbtCloudRunFailed`: when the response code is != 200
        """
        data = additional_args if additional_args else {}
        data["cause"] = cause

        url = self.dbt_cloud_trigger_job_api_endpoint_v2(job_id=job_id)
        with self.session.post(url, data=data) as trigger_request:

            if trigger_request.status_code != 200:
                raise TriggerDbtCloudRunFailed(trigger_request.reason)

        return trigger_request.json()["data"]

    def wait_for_job_run(self, run_id: int, max_wait_time: int = None) -> Dict:
        """
        Get a dbt Cloud job run.
        Please note that this function will fail if any call to dbt Cloud APIs fail.

        Args:
            run_id: dbt Cloud job run ID
            max_wait_time: the number od seconds to wait for the job to complete

        Returns:
            The job run result, namely the "data" key in the API response

        Raises:
            `DbtCloudRunFailed`: if "finished_at" is not None
                and the result status == 20
            `DbtCloudRunCanceled`: if "finished_at" is not None
                and the result status == 30
            `DbtCloudRunTimedOut`: if run does not finish
                before provided max_wait_time
        """
        wait_time_between_api_calls = 10
        elapsed_wait_time = 0
        url = self.dbt_cloud_get_run_api_endpoint_v2(run_id=run_id)

        while not max_wait_time or elapsed_wait_time <= max_wait_time:
            with self.session.get(url) as get_run_request:

                if get_run_request.status_code != 200:
                    raise GetDbtCloudRunFailed(get_run_request.reason)

            result = get_run_request.json()["data"]

            if result["finished_at"]:
                if result["status"] == 10:
                    return result
                elif result["status"] == 20:
                    raise DbtCloudRunFailed(f"Job run with ID: {run_id} failed.")
                elif result["status"] == 30:
                    raise DbtCloudRunCanceled(f"Job run with ID: {run_id} cancelled.")

            sleep(wait_time_between_api_calls)
            elapsed_wait_time += wait_time_between_api_calls

        raise DbtCloudRunTimedOut(
            f"Max attempts reached while checking status of job run with ID: {run_id}"
        )

    def list_run_artifact_links(
        self,
        run_id: int,
    ) -> List[Tuple[str, str]]:
        """
        Lists URLs that can be used to download artifacts from a dbt run

        Args:
            run_id: dbt Cloud job run ID

        Returns:
            List of artifact download URLs

        Raises:
            `DbtCloudListArtifactsFailed`: if API to list dbt artifacts fails

        """
        url = self.dbt_cloud_list_run_artifacts_endpoint_v2(run_id=run_id)
        with self.session.get(url) as list_run_artifacts_response:

            if list_run_artifacts_response.status_code != 200:
                raise DbtCloudListArtifactsFailed(list_run_artifacts_response.reason)

        artifact_paths = list_run_artifacts_response.json().get("data")
        return [
            (
                self.dbt_cloud_get_run_artifact_endpoint_v2(
                    run_id=run_id, path=artifact_path
                ),
                artifact_path,
            )
            for artifact_path in artifact_paths
        ]
