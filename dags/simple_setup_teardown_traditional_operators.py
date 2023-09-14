from airflow.decorators import dag
from pendulum import datetime
from airflow.operators.python import PythonOperator


FAIL_WORKER_ON_CLUSTER = True


def return_message(msg, fail_task=False):
    if fail_task:
        raise Exception("Worker failed!")
    return msg


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["toy", "setup/teardown", "traditional operators", "webinar"],
)
def simple_setup_teardown_traditional_operators():
    upstream_task_obj = PythonOperator(
        task_id="upstream_task",
        python_callable=return_message,
        op_kwargs={"msg": "hi"},
    )

    spin_up_cluster_obj = PythonOperator(
        task_id="spin_up_cluster",
        python_callable=return_message,
        op_kwargs={"msg": "Cluster is up!"},
    )

    worker_task_on_cluster_obj = PythonOperator(
        task_id="worker_task_on_cluster",
        python_callable=return_message,
        op_kwargs={
            "msg": "Working on it using the cluster!",
            "fail_task": FAIL_WORKER_ON_CLUSTER,
        },
    )

    tear_down_cluster_obj = PythonOperator(
        task_id="tear_down_cluster",
        python_callable=return_message,
        op_kwargs={"msg": "Cluster is down!"},
    )

    long_running_task_NOT_on_cluster_obj = PythonOperator(
        task_id="long_running_task_NOT_on_cluster",
        python_callable=return_message,
        op_kwargs={"msg": "Working on it but taking my time!"},
    )

    downstream_task_obj = PythonOperator(
        task_id="downstream_task",
        python_callable=return_message,
        op_kwargs={"msg": "hi"},
    )

    spin_up_cluster_obj >> long_running_task_NOT_on_cluster_obj >> downstream_task_obj

    (
        upstream_task_obj
        >> spin_up_cluster_obj
        >> worker_task_on_cluster_obj
        >> tear_down_cluster_obj
        >> downstream_task_obj
    )

    # define setup and teardown tasks and dependency
    tear_down_cluster_obj.as_teardown(setups=[spin_up_cluster_obj])


simple_setup_teardown_traditional_operators()
