"""
## Simple DAG with no setup or teardown for demonstration purposes

"""

from airflow.decorators import dag, task
from pendulum import datetime

FAIL_WORKER_ON_CLUSTER = False


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["toy", "webinar"],
)
def simple_dag_no_setup_teardown():
    @task
    def upstream_task():
        return "hi"

    @task
    def spin_up_cluster():
        return "Cluster is up!"

    @task
    def worker_task_on_cluster():
        if FAIL_WORKER_ON_CLUSTER:
            raise Exception("Worker failed!")
        return "Working on it using the cluster!"

    @task
    def tear_down_cluster():
        return "Cluster is down!"

    @task
    def long_running_task_NOT_on_cluster():
        return "Working on it but taking my time!"

    @task
    def downstream_task():
        return "hi"

    upstream_task_obj = upstream_task()
    downstream_task_obj = downstream_task()
    spin_up_cluster_obj = spin_up_cluster()

    spin_up_cluster_obj >> long_running_task_NOT_on_cluster() >> downstream_task_obj

    (
        upstream_task_obj
        >> spin_up_cluster_obj
        >> worker_task_on_cluster()
        >> tear_down_cluster()
        >> downstream_task_obj
    )


simple_dag_no_setup_teardown()
