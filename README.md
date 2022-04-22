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
    goodbye_prefect_dbtcloud,
    hello_prefect_dbtcloud,
)


@flow
def example_flow():
    hello_prefect_dbtcloud
    goodbye_prefect_dbtcloud

example_flow()
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
