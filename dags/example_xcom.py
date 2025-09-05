"""
## XCom Example DAG

This DAG demonstrates how to use XComs (cross-communications) in Airflow to pass data between tasks.
It consists of two tasks:

1. **push_values**: Pushes two XCom values - `secret_name` and `file_location`
2. **consume_values**: Pulls the XCom values from the previous task and uses them

XComs allow tasks to exchange small pieces of data, which is useful for passing information like
file paths, configuration values, or processing results between tasks.

For more information on XComs in Airflow, see the documentation:
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
"""

import airflow.providers.microsoft.azure.fs.adls
from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    dag_id="xcom_example_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["example", "xcoms"],
)
def xcom_example():
    @task
    def push_values(**context):
        """
        This task pushes two XCom values with keys 'secret_name' and 'file_location'.

        XComs can be pushed using the TaskInstance (ti) object's xcom_push method,
        which is accessible through the context dictionary.
        """
        # Push values using explicit xcom_push
        context["ti"].xcom_push(key="secret_name", value="my_api_secret")
        context["ti"].xcom_push(
            key="file_location", value="/path/to/important/file.txt"
        )

        print("XCom values have been pushed!")

    @task
    def consume_values(**context):
        """
        This task consumes the XCom values pushed by the push_values task.

        XComs can be pulled using the TaskInstance (ti) object's xcom_pull method,
        which is accessible through the context dictionary.
        """
        # Pull values from the push_values task
        secret_name = context["ti"].xcom_pull(task_ids="push_values", key="secret_name")
        file_location = context["ti"].xcom_pull(
            task_ids="push_values", key="file_location"
        )

        # Pull the value that was automatically pushed via return value
        another_value = context["ti"].xcom_pull(
            task_ids="push_values", key="another_value"
        )

        print(f"Retrieved secret name: {secret_name}")
        print(f"Retrieved file location: {file_location}")
        print(f"Retrieved another value: {another_value}")

        # Using the pulled values to do something
        print(
            f"Now I can use the secret '{secret_name}' to access the file at '{file_location}'"
        )

    # Set up the task dependencies
    # In TaskFlow API, we can set dependencies by calling the first task
    # and passing its result to the second task
    push_task = push_values()
    consume_task = consume_values()

    # Set the dependency: push_task must run before consume_task
    push_task >> consume_task


# Instantiate the DAG
xcom_example()
