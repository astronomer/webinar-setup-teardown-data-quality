"""
## Toy DAG to show a simple setup/teardown workflow

This DAG shows a simple setup/teardown workflow pipeline with mock tasks.
"""

from airflow.decorators import dag, task, task_group, setup, teardown
from pendulum import datetime


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["setup/teardown", "syntax", "data quality"],
)
def structure_create_table_setup_teardown():
    @task
    def upstream_task():
        return "hi"

    @task_group
    def create_table():
        @setup
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

        @teardown
        def drop_tmp():
            return "Dropped the tmp table!"

        @task
        def done():
            return "New table is ready!"

        create_tmp_obj = create_tmp()
        swap_obj = swap()
        drop_tmp_obj = drop_tmp()

        create_tmp_obj >> test_tmp() >> swap_obj >> [add_docs(), drop_tmp_obj] >> done()

        # define setup/ teardown relationship
        create_tmp_obj >> drop_tmp_obj

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


structure_create_table_setup_teardown()
