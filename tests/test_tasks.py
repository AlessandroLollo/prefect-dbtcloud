from prefect import flow

from prefect_dbtcloud.tasks import (
    goodbye_prefect_dbtcloud,
    hello_prefect_dbtcloud,
)


def test_hello_prefect_dbtcloud():
    @flow
    def test_flow():
        return hello_prefect_dbtcloud()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-dbtcloud!"


def goodbye_hello_prefect_dbtcloud():
    @flow
    def test_flow():
        return goodbye_prefect_dbtcloud()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-dbtcloud!"
