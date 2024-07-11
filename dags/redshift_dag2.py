from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from query_redshift_cls import sql_executor
import os

AF_Home = os.environ['AIRFLOW_HOME']

with DAG(
    dag_id=f"etl_hercules_2",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    sqltask = sql_executor(
        task_id='dim_term',
        filedir = AF_Home+"/"+"sql" ,
        filename = "HERCULES.DIM_TERM.sql",
        schemaname = "Hercules",
        tablename='dim_term',
        modulename = 'workday', 
        env = 'prod',
        conn_id=Variable.get("conn_etl")
        )