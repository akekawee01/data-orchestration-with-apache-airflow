from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

@task(task_id="push")
def etl(ti):
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

    customers_df = pg_hook.get_df("SELECT * FROM customers;")
    orders_df = pg_hook.get_df("SELECT * FROM orders;")

    # If you want to add a new column to the DataFrame with some result, for example, let's add a column "age"

    customers_df["birthdate"] = pd.to_datetime(customers_df["birthdate"]).dt.strftime("%d-%m-%Y")
    print(customers_df["birthdate"])
    print(orders_df)
    # Save DataFrame to a local Parquet file
    parquet_file = "/tmp/customers.parquet"
    customers_df.to_parquet(parquet_file, index=False)

    order_parquet_file = "/tmp/orders.parquet"
    orders_df.to_parquet(order_parquet_file, index=False)
    # Upload Parquet file to S3 using S3Hook

    s3_hook = S3Hook(aws_conn_id="my_aws_connection")
    s3_bucket = "pea-watt"
    s3_key = "akeeee/2025-10-03/customers.parquet"
    s3_hook.load_file(
        filename=parquet_file,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

    s3_key = "akeeee/2025-10-03/orders.parquet"
    s3_hook.load_file(
        filename=order_parquet_file,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )


    ti.xcom_push(key="prefix", value="akeeee/2025-10-03/")
    return "hello"

@task(task_id="wait_for_customers_file")
def _wait_for_customers_file():
    s3_bucket = "pea-watt"
    s3_key = "akeeee/2025-10-03/customers.parquet"
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")

    sensor = S3KeySensor(
        task_id="s3_key_sensor",
        bucket_name=s3_bucket,
        bucket_key=s3_key,
        aws_conn_id="my_aws_connection",
        poke_interval=30,
        timeout=600,
    )

    sensor.execute(context={})
    print(f"File customers of {s3_key} is available in bucket {s3_bucket}.")

@task(task_id="wait_for_orders_file")
def _wait_for_orders_file():
    s3_bucket = "pea-watt"
    s3_key = "akeeee/2025-10-03/orders.parquet"
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")

    sensor = S3KeySensor(
        task_id="s3_key_sensor_orders",
        bucket_name=s3_bucket,
        bucket_key=s3_key,
        aws_conn_id="my_aws_connection",
        poke_interval=30,
        timeout=600,
    )

    sensor.execute(context={})
    print(f"File orders of {s3_key} is available in bucket {s3_bucket}.")


@task(task_id="pull")
def _list_files(ti):
    prefix = ti.xcom_pull(task_ids="push", key="prefix")
    s3_bucket = "pea-watt"
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")



    objects = s3_hook.list_keys(
    bucket_name=s3_bucket,
    prefix="akeeee/2025-10-03/"
    )

    print("Files under prefix:")
    for obj in objects:
        print("=============")
        print(obj)




@dag(
    dag_id="workshop",
    start_date=timezone.datetime(2025,10,2),
    schedule=None,
)
def main():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    start >> etl() >> _wait_for_customers_file() >> _wait_for_orders_file() >> _list_files() >> end


main()
