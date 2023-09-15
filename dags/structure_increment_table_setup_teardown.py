"""
## Explore a pattern of incrementally updating a table with data quality checks and setup/teardown tasks

This is a structure DAG that demonstrates a pattern of incrementally updating a table. 
It includes three sets of data quality checks and three setup/ teardown workflows.
This DAG is meant to be used as a pattern blueprint to create your own ETL/ELT pipelines.
"""

from airflow.decorators import dag, task, task_group
from pendulum import datetime
from airflow.models.baseoperator import chain

TMP_EMPTY = False
TABLE_EXISTS = True
SCHEMA_CHANGED = False


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["syntax", "data quality", "setup/teardown", "webinar"],
)
def structure_increment_table_setup_teardown():
    @task
    def upstream_task():
        """Any upstream action needed."""
        return "hi"

    @task_group
    def increment_table():
        @task
        def create_empty():
            """Create a new empty table with the schema specified."""
            return "Created an empty table!"

        @task
        def fetch_col_types():
            """Fetch column types from the new empty table."""
            return "Fetched column types!"

        create_empty_obj = create_empty()
        fetch_col_types_obj = fetch_col_types()
        chain(create_empty_obj, fetch_col_types_obj)

        @task
        def assess():
            """Assess whether to increment or fully reload the table."""

            # logic that assesses whether to increment or fully reload the table
            # for example based on user input or whether the schema has changed
            # (the fetch_col_types task can be used to get the columns of the empty
            # target table)

            if not TABLE_EXISTS:
                return "full_reload"

            if SCHEMA_CHANGED:
                return "full_reload"

            else:
                return "increment"

        @task
        def create_tmp():
            """Create a tmp table with the same schema as the target table containing
            the data to be merged into the target table."""
            return "Created a tmp table!"

        create_tmp_obj = create_tmp()

        @task_group
        def check_for_empty_tmp():
            @task
            def get_data():
                "Retrieves the top 1 row from the tmp table."
                return "Got one row from the tmp table!"

            @task.branch
            def check_data():
                """Checks whether the tmp table is empty. I.e. no new data is to be added."""
                if TMP_EMPTY:
                    return "increment_table.check_for_empty_tmp.no_data"
                else:
                    return "increment_table.check_for_empty_tmp.has_data"

            @task(
                trigger_rule="none_failed_or_skipped",
            )
            def no_data():
                """Task to catch the case where no new data is to be added."""
                return "The tmp table is empty!"

            @task(
                trigger_rule="none_failed_or_skipped",
            )
            def has_data():
                """Task to catch the case where new data is to be added."""
                return "The tmp table has data!"

            get_data_obj = get_data()
            check_data_obj = check_data()
            has_data_obj = has_data()
            no_data_obj = no_data()

            chain(get_data_obj, check_data_obj, [has_data_obj, no_data_obj])

            return [get_data_obj, check_data_obj, has_data_obj, no_data_obj]

        check_for_empty_tmp_obj = check_for_empty_tmp()
        get_data_obj = check_for_empty_tmp_obj[0]
        has_data_obj = check_for_empty_tmp_obj[2]
        no_data_obj = check_for_empty_tmp_obj[3]

        @task_group
        def test_tmp():
            @task
            def test_cols():
                """Run data quality checks on the columns of the tmp table.
                These checks halt the pipeline if they fail."""
                return "Ran crucial data quality checks on columns!"

            @task
            def test_table():
                """Run data quality checks on the full tmp table. These checks halt the
                pipeline if they fail."""
                return "Ran crucial data quality checks on the table!"

            test_cols()
            test_table()

        @task.branch
        def choose(**context):
            """Choose the downstream path based on the return value of the assess task."""
            assessment_result = context["ti"].xcom_pull(
                task_ids="increment_table.assess"
            )

            print(f"Assessment result: {assessment_result}")

            if assessment_result == "increment":
                return "increment_table.clone_dest"
            if assessment_result == "full_reload":
                return "increment_table.full_reload"
            else:
                raise Exception(
                    f"Assessment returned an unknown result: {assessment_result}!"
                )

        choose_obj = choose()

        chain(
            fetch_col_types_obj,
            assess(),
            create_tmp_obj,
            get_data_obj,
            has_data_obj,
            test_tmp(),
            choose_obj,
        )

        @task
        def clone_dest():
            """Create an empty clone of the destination table with the same schema."""
            return "Cloned the destination table!"

        clone_dest_obj = clone_dest()

        @task
        def migrate_clone():
            """Migrate the data from the destination table into the cloned table."""
            return "Migrated the cloned table!"

        @task
        def merge_clone():
            """Task to upsert the data from the tmp table into the cloned table."""
            return "Merged the cloned table!"

        @task_group
        def test_clone():
            @task
            def test_cols():
                """Run data quality checks on the columns of the cloned table.
                These checks halt the pipeline if they fail."""
                return "Ran crucial data quality checks on columns!"

            @task
            def test_table():
                """Run data quality checks on the full cloned table.
                These checks halt the pipeline if they fail."""
                return "Ran crucial data quality checks on the table!"

            test_cols()
            test_table()

        @task
        def swap():
            """Swap the cloned table with the destination table."""
            return "Swapped the cloned table with the real table!"

        @task
        def drop_clone():
            """Drop the cloned table."""
            return "Dropped the cloned table!"

        drop_clone_obj = drop_clone()

        @task(
            trigger_rule="none_failed_or_skipped",
        )
        def drop_tmp():
            """Drop the tmp table."""
            return "Dropped the tmp table!"

        @task
        def full_reload():
            """Task to fully reload the destination from the tmp table."""
            return "Fully reloaded the table!"

        drop_tmp_obj = drop_tmp()

        chain(
            choose_obj,
            clone_dest_obj,
            migrate_clone(),
            merge_clone(),
            test_clone(),
            swap(),
            drop_clone_obj,
            drop_tmp_obj,
        )
        chain(no_data_obj, drop_tmp_obj)
        chain(choose_obj, full_reload(), drop_tmp_obj)

        @task
        def add_docs():
            """Add documentation to the destination table."""
            return "Added documentation!"

        @task
        def done():
            """Task to signal that the table is ready."""
            return "New table is ready!"

        done_obj = done()

        chain(drop_tmp_obj, add_docs(), done_obj)

        @task
        def drop_empty():
            """Drop the empty table."""
            return "Dropped the empty table!"

        drop_empty_obj = drop_empty()
        fetch_col_types_obj >> drop_empty_obj

        @task_group
        def validate():
            @task
            def test_cols():
                """Run additional data quality checks on the columns of the destination table.
                These checks do not halt the pipeline if they fail."""
                return "Ran additional data quality checks on columns!"

            @task
            def test_table():
                """Run additional data quality checks the full destination table.
                These checks do not halt the pipeline if they fail."""
                return "Ran additional data quality checks on the table!"

            @task(trigger_rule="all_done")
            def sql_check_done():
                """Task to signal that all data quality checks are done. That will
                always succeed so that the pipeline can continue."""
                return "Additional data quality checks are done!"

            [test_cols(), test_table()] >> sql_check_done()

        chain(drop_tmp_obj, validate())

        # Create setup/teardown relationships
        drop_empty_obj.as_teardown(setups=[create_empty_obj])
        drop_tmp_obj.as_teardown(setups=[create_tmp_obj])
        drop_clone_obj.as_teardown(setups=[clone_dest_obj])

    @task
    def downstream_task():
        """Any downstream action needed."""
        return "hi"

    upstream_task() >> increment_table() >> downstream_task()


structure_increment_table_setup_teardown()
