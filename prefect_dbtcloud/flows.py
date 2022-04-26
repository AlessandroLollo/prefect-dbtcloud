"""This is an example flows module"""
from prefect import flow

from prefect_dbtcloud.tasks import goodbye_prefect_dbtcloud, hello_prefect_dbtcloud


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_dbtcloud)
    print(goodbye_prefect_dbtcloud)
