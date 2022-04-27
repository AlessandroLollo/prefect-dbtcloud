# prefect-dbtcloud

## Welcome!

Collection of tasks to interact with dbt Cloud

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-dbtcloud` with `pip`:

```bash
pip install prefect-dbtcloud
```

### Write and run a flow

```python
from prefect import flow
from prefect_dbtcloud.tasks import (
    create_job,
    run_job
)

# Create a new job
@flow
def create_new_job():
    create_job(
        account_id=111,                   #  Your dbt Cloud Account ID.
        project_id=222,                   #  The ID of the project where to create the job.
        environment_id=333,               #  The ID of the environment to use to run the job.
        name="new-job",                   #  The name of the job.
        execute_steps=["dbt run"],        #  The list of dbt commands the job will execute.
        generate_docs=True                #  Whether dbt should generate docs or not.
    )

# Trigger job run
@flow
def trigger_dbt_cloud_job_run():
    run_job(
        cause="The cause",                 #  A string describing why you're triggering the job.
        account_id=111,                    #  Your dbt Cloud Account ID.
        job_id=678,                        #  The ID of the Job to run.
        token="The secret token",          #  The API token to use to authenticate on dbt Cloud.
        wait_for_job_run_completion=False  #  Whether to wait for job completion or not.
    )

create_new_job()
trigger_dbt_cloud_job_run()
```

## Resources

If you encounter any bugs while using `prefect-dbtcloud`, feel free to open an issue in the [prefect-dbtcloud](https://github.com/AlessandroLollo/prefect-dbtcloud) repository.

If you have any questions or issues while using `prefect-dbtcloud`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-dbtcloud` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/AlessandroLollo/prefect-dbtcloud.git

cd prefect-dbtcloud/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
