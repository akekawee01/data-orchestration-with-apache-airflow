from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
import pandas as pd

@task
def etl():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

    customers_df = pg_hook.get_df("SELECT * FROM customers;")


    customers_df = pd.read_csv(...)
    # Format the datetime from "12 May 1990" to "1990-05-12"
    df["birthdate"] = pd.to_datetime(df["birthdate"], dayfirst=True)
    print(df["birthdate"])



@dag(
    dag_id="workshop",
    start_date=timezone.datetime(2025,10,2),
    schedule=None,
):
def main():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    start >> etl() >> end


main()
