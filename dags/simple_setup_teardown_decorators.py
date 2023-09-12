from airflow.decorators import dag, task, task_group, setup, teardown
from pendulum import datetime

FAIL_WORKER_ON_CLUSTER = False


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["setup/teardown", "toy", "decorators"],
)
def simple_setup_teardown_decorators():
    @task
    def upstream_task():
        return "hi"

    @setup
    def spin_up_cluster():
        return "Cluster is up!"

    @task
    def worker_task_on_cluster():
        if FAIL_WORKER_ON_CLUSTER:
            raise Exception("Worker failed!")
        return "Working on it using the cluster!"

    @teardown
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
    tear_down_cluster_obj = tear_down_cluster()

    spin_up_cluster_obj >> long_running_task_NOT_on_cluster() >> downstream_task_obj

    (
        upstream_task_obj
        >> spin_up_cluster_obj
        >> worker_task_on_cluster()
        >> tear_down_cluster_obj
        >> downstream_task_obj
    )

    # define setup and teardown dependency
    spin_up_cluster_obj >> tear_down_cluster_obj


simple_setup_teardown_decorators()
