"""
## Structure DAG showing a create table pattern with no setup/ teardown tasks

This is a structure DAG that demonstrates a pattern of creating a table with
data quality checks but no setup/ teardown tasks.
"""

from airflow.decorators import dag, task, task_group
from pendulum import datetime


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["syntax", "data quality"],
)
def structure_create_table_no_setup_teardown():
    @task
    def upstream_task():
        return "hi"

    @task_group
    def create_table():
        @task
        def create_tmp():
            return "Created a tmp table!"

        @task_group
        def test_tmp():
            @task
            def test_cols():
                return "Ran crucial data quality checks on columns!"

            @task
            def test_table():
                return "Ran crucial data quality checks on the table!"

            test_cols()
            test_table()

        @task
        def swap():
            return "Swapped the tmp table with the real table!"

        @task
        def add_docs():
            return "Added documentation!"

        @task
        def drop_tmp():
            return "Dropped the tmp table!"

        @task
        def done():
            return "New table is ready!"

        swap_obj = swap()

        create_tmp() >> test_tmp() >> swap_obj >> [add_docs(), drop_tmp()] >> done()

        @task_group
        def validate():
            @task
            def test_cols():
                return "Ran additional data quality checks on columns!"

            @task
            def test_table():
                return "Ran additional data quality checks on the table!"

            @task(trigger_rule="all_done")
            def sql_check_done():
                return "Additional data quality checks are done!"

            [test_cols(), test_table()] >> sql_check_done()

        swap_obj >> validate()

    @task
    def downstream_task():
        return "hi"

    upstream_task() >> create_table() >> downstream_task()


structure_create_table_no_setup_teardown()
