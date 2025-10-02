from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def _hello()
    print("Hello World")

with DAG(
    "everyday",
    start_date=timezone.datetime(2025,10,2),
    schedule="15 14 * * *",
):

    hello = PythonOperator(
        task_id="hello",
        python_callable=_hello,
    )

    word = BashOperator(
        task_id="world",
        bash_command='echo "World!"',
    )

    hello >> world