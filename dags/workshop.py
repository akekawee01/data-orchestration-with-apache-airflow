from airflow.utils import timezone
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
@task
def etl():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

    customers_df = pg_hook.get_df("SELECT * FROM customers;")

    # If you want to add a new column to the DataFrame with some result, for example, let's add a column "age"

    # Calculate age from birthdate
    customers_df["birthdate"] = customers_df["birthdate"].apply(
        lambda x: datetime.now().year - pd.to_datetime(x, dayfirst=True).year
    )
    # Save DataFrame to a local Parquet file
    parquet_file = "/tmp/customers.parquet"
    customers_df.to_parquet(parquet_file, index=False)

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
