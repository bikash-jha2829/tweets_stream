from datetime import datetime, timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# [END import_module]


# [START instantiate_dag]

dag = DAG(
    'spark_pi',
    default_args={'max_active_runs': 1},
    description='submit spark-pi as consumer on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

t1 = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace="default",
    application_file="container/spark-pi.yaml",
    do_xcom_push=True,
    dag=dag,
)

t2 = SparkKubernetesSensor(
    task_id='spark_pi_monitor',
    namespace="default",
    application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
    dag=dag,
)
t1 >> t2