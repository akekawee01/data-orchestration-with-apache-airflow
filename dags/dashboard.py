from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    "dashboard",
    start_date=timezone.datetime(2025,10,2),
    schedule="15 14 * * *",
):

    api = BashOperator(
        task_id="api",
        bash_command='echo extract api',
    )

    database = BashOperator(
        task_id="database",
        bash_command='echo "extract database!"',
    )

    marketplace = BashOperator(
        task_id="marketplace",
        bash_command='echo "extract marketplace!"',
    )

    tranform_clean = BashOperator(
        task_id="tranform_clean",
        bash_command='echo "transform and clean!"',
    )

    load_wh = BashOperator(
        task_id="load_wh",
        bash_command='echo "load to data warehouse!"',
    )

    generate_report = BashOperator(
        task_id="generate_report",
        bash_command='echo "generate report!"',
    )

    
    [api >> database >> marketplace] >> tranform_clean >> load_wh >> generate_report
    