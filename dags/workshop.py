from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
@task(task_id="push")
def etl():
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

    s3_hook = S3Hook(aws_conn_id="my_aws_connection")
    s3_bucket = "pea-watt"
    s3_key = "akeeee/2025-10-03/orders.parquet"
    s3_hook.load_file(
        filename=order_parquet_file,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )


    ti.xcom_push(key="prefix", value="akeeee/2025-10-03/")

@task(task_id="pull")
def _pull(ti):
    prefix = ti.xcom_pull(task_ids="push", key="prefix")
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

    start >> etl() >> end


main()
