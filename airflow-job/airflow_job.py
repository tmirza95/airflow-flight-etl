from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from datetime import datetime, timedelta
import uuid

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14)
}

with DAG (
    dag_id="spark_job_dag",
    default_args = default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    env = Variable.get("env")
    gcs_bucket = Variable.get("gcs_bucket")
    bq_project = Variable.get("bq_project")
    bq_dataset = Variable.get("bq_dataset")
    tables = Variable.get("tables", deserialize_json=True)
    
    transformed_table = tables.get('transformed_table')

    route_insights_table = tables.get('route_insights_table')

    origin_insights_table = tables.get('origin_insights_table') 

    # Generate a unique batch ID using UUID
    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity


    file_sensor = GCSObjectExistenceSensor(
        task_id = 'wait_for_files',
        bucket = gcs_bucket,
        google_cloud_conn_id = 'google_cloud_default',
        object = f'airflow-project-flights/source_{env}/flight_booking.csv',
        timeout = 300,
        mode = 'poke',
        poke_interval = 30 
    )

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket}/airflow-project-1/spark-job/spark_transformation_job.py",
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "environment_config": {
            "execution_config": {
                "service_account": "699202646224-compute@developer.gserviceaccount.com",
                "network_uri": "projects/graphite-willow-482416-h2/global/networks/default",
                "subnetwork_uri": "projects/graphite-willow-482416-h2/regions/us-east1/subnetworks/default",
            }
        },
        "runtime_config": {
            "version": 2.2,
        }
    }

    spark_task = DataprocCreateBatchOperator(
        task_id = 'run_spark_on_dataproc_serverless',
        batch_id = batch_id,
        batch = batch,
        region = 'us-central1',
        project_id = 'graphite-willow-482416-h2',
        gcp_conn_id="google_cloud_default",
    )

    file_sensor >> spark_task