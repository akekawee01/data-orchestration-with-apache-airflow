from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.provider.standard.operators.empty import EmpytyOperator

with DAG(
    "my_first_dag",
    start_date=timezone.datetime(2025,10,2),
    schedule=None,
):

    t1 = EmpytyOperator(task_id="t1")
    t2 = EmpytyOperator(task_id="t2")