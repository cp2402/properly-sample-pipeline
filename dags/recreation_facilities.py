from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from pipeline.ingest_postal_locations import ingest_postal_locations
from pipeline.ingest_property_sales import ingest_property_sales
from pipeline.ingest_recreation_facilities import ingest_recreation_facilities
from pipeline.load_postal_locations import load_postal_locations
from pipeline.load_property_sales import load_property_sales
from pipeline.load_recreation_facilities import load_recreation_facilities

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "recreation_facilities",
    default_args=default_args,
    description="To process and load property sales and recreation faciliities data",
    schedule_interval="@weekly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    with TaskGroup(group_id="ingest_raw_to_data_lake") as ingest_raw_to_data_lake:

        ingest_property_sales_task = PythonOperator(
            task_id="ingest_property_sales",
            python_callable=ingest_property_sales,
            provide_context=True,
        )

        ingest_postal_locations_task = PythonOperator(
            task_id="ingest_postal_locations",
            python_callable=ingest_postal_locations,
            provide_context=True,
        )

        ingest_recreation_facilities_task = PythonOperator(
            task_id="ingest_recreation_facilities",
            python_callable=ingest_recreation_facilities,
            provide_context=True,
        )

    with TaskGroup(group_id="load_to_dwh_src") as load_to_dwh_src:

        create_src_datasets = PostgresOperator(
            task_id="create_src_datasets",
            sql=[
                "sql/create/src_postal_locations.sql",
                "sql/create/src_property_sales.sql",
                "sql/create/src_recreation_facilities.sql",
            ],
        )

        load_postal_locations_task = PythonOperator(
            task_id="load_postal_locations",
            python_callable=load_postal_locations,
            provide_context=True,
        )

        load_recreation_facilities_task = PythonOperator(
            task_id="load_recreation_facilities",
            python_callable=load_recreation_facilities,
            provide_context=True,
        )

        load_property_sales_task = PythonOperator(
            task_id="load_property_sales",
            python_callable=load_property_sales,
            provide_context=True,
        )

        create_src_datasets >> [
            load_postal_locations_task,
            load_recreation_facilities_task,
            load_property_sales_task,
        ]

    with TaskGroup(group_id="transform_to_dwh_stg") as transform_to_dwh_stg:

        create_stg_tables = PostgresOperator(
            task_id="create_stg_tables",
            sql=[
                "sql/create/stg_recreation_locations.sql",
                "sql/create/stg_recreation_proximities.sql",
            ],
        )

        transform_stg_tables = PostgresOperator(
            task_id="transform_stg_tables",
            sql=[
                "TRUNCATE TABLE stg_recreation_proximities CASCADE",
                "sql/transform/load_stg_recreation_proximities.sql",
            ],
        )

        create_stg_tables >> transform_stg_tables

    with TaskGroup(group_id="transform_to_dwh_analytics") as transform_to_dwh_analytics:

        create_dwh_tables = PostgresOperator(
            task_id="create_dwh_tables",
            sql=[
                "sql/create/dwh_facilities_closest.sql",
                "sql/create/dwh_facilities_count.sql",
                "sql/create/dwh_sales_facilities.sql",
            ],
        )

        transform_dwh_tables = PostgresOperator(
            task_id="transform_dwh_tables",
            sql=[
                "TRUNCATE TABLE dwh_sales_facilities CASCADE",
                "sql/transform/load_dwh_sales_facilities.sql",
            ],
        )

        create_dwh_tables >> transform_dwh_tables

    (
        ingest_raw_to_data_lake
        >> load_to_dwh_src
        >> transform_to_dwh_stg
        >> transform_to_dwh_analytics
    )
