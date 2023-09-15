"""
## Use setup/ teardown with data quality checks during creation of a Snowflake table

This DAG demonstrates a table creation pattern which includes both halting and 
non-halting data quality checks. Setup/ teardown tasks are used to create and
drop temporary tables.

To use this DAG you will need to load the `parks.csv` file into an S3 bucket
at the S3_DATA_LOCATION specified below. You will also need to set the `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_KEY_ID` environment variables to the appropriate values and provide a
Snowflake connection with the name `snowflake_default`.
"""

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)
import os

SNOWFLAKE_CONN_ID = "snowflake_default"
TABLE_NAME = "national_parks"
SCHEMA_NAME = "TAMARAFINGERLIN"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY_ID = os.getenv("AWS_SECRET_KEY_ID")
S3_DATA_LOCATION = "s3://toy-datasets-stage/parks.csv"

@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["setup/teardown", "data quality"],
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID, "conn_id": SNOWFLAKE_CONN_ID},
)
def create_table_setup_teardown_snowflake():
    @task
    def upstream_task():
        return "hi"

    @task_group
    def create_table():
        create_tmp = SnowflakeOperator(
            task_id="create_tmp",
            sql=f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}_tmp (
                    park_code varchar(4) PRIMARY KEY,
                    park_name varchar(255),
                    state varchar(255),
                    acres int,
                    latitude float,
                    longitude float
                );""",
        )

        load_data_into_tmp = SnowflakeOperator(
            task_id="load_data_into_tmp",
            sql=f"""
                COPY INTO {TABLE_NAME}_tmp 
                FROM 's3://toy-datasets-stage/parks.csv'
                credentials = (aws_key_id='{AWS_ACCESS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY_ID}')
                ON_ERROR = 'continue'
                ;
                """,
        )

        @task_group
        def test_tmp():
            SQLColumnCheckOperator(
                task_id="test_cols",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}_tmp",
                column_mapping={"park_code": {"unique_check": {"equal_to": 0}}},
                accept_none="True",
            )

            SQLTableCheckOperator(
                task_id="test_table",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}_tmp",
                checks={"row_count_check": {"check_statement": "COUNT(*) > 30"}},
            )

        swap = SnowflakeOperator(
            task_id="swap",
            sql=f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                        park_code varchar(4) PRIMARY KEY,
                        park_name varchar(255),
                        state varchar(255),
                        acres int,
                        latitude float,
                        longitude float
                    );
                ALTER TABLE {TABLE_NAME} SWAP WITH {TABLE_NAME}_tmp;
                """,
        )

        drop_tmp = SnowflakeOperator(
            task_id="drop_tmp",
            sql=f"""
                DROP TABLE {TABLE_NAME}_tmp;
                """,
        )

        @task
        def done():
            return "New table is ready!"

        chain(
            create_tmp,
            load_data_into_tmp,
            test_tmp(),
            swap,
            [drop_tmp, done()]
        )

        # define setup/ teardown relationship
        drop_tmp.as_teardown(setups=[create_tmp, load_data_into_tmp])

        @task_group
        def validate():
            test_cols = SQLColumnCheckOperator(
                task_id="test_cols",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}",
                column_mapping={"park_name": {"unique_check": {"equal_to": 0}}},
                accept_none="True",
            )

            test_table = SQLTableCheckOperator(
                task_id="test_table",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}",
                checks={"row_count_check": {"check_statement": "COUNT(*) > 50"}},
            )

            @task(trigger_rule="all_done")
            def sql_check_done():
                return "Additional data quality checks are done!"

            [test_cols, test_table] >> sql_check_done()

        swap >> validate()

    @task
    def downstream_task():
        return "hi"

    upstream_task() >> create_table() >> downstream_task()


create_table_setup_teardown_snowflake()
