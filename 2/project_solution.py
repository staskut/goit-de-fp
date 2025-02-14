from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# DAG Definition
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="staskut_athlete_datalake_etl2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["staskut"]
)

# Define Spark Jobs
landing_to_bronze = SparkSubmitOperator(
    task_id="landing_to_bronze",
    application="dags/staskut/landing_to_bronze.py",
    conn_id="spark-default",
    dag=dag,
    verbose=1,
    conf={"spark.submit.deployMode": "client"},
)

bronze_to_silver = SparkSubmitOperator(
    task_id="bronze_to_silver",
    application="dags/staskut/bronze_to_silver.py",
    conn_id="spark-default",
    dag=dag,
    verbose=1,
    conf={"spark.submit.deployMode": "client"},
)

silver_to_gold = SparkSubmitOperator(
    task_id="silver_to_gold",
    application="dags/staskut/silver_to_gold.py",
    conn_id="spark-default",
    dag=dag,
    verbose=1,
    conf={"spark.submit.deployMode": "client"},
)

# Define Task Dependencies
landing_to_bronze >> bronze_to_silver >> silver_to_gold