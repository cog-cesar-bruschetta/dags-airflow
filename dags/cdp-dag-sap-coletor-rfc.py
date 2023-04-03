from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Error notifications to SNS
from airflow_notify_sns import airflow_notify_sns

from airflow.exceptions import AirflowException
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

try:
    dag_config = Variable.get("sap_coletor_rfc", deserialize_json=True)
except Exception as ex:
    raise AirflowException("Variable sap_coletor_rfc must be set.")  
    ## Exemple of a DAG config to runs a SAP RFC function
    ## {
    ##     "retry_count": 3,
    ##     "retry_delay_minutes": 5,
    ##     "schedule": "0 0 * * *",
    ##     "dag_max_active_runs": 1,
    ##     "sap_host": "127.0.01",
    ##     "sap_user": "user",
    ##     "sap_password": "123qwe",
    ##     "sap_client": 200,
    ##     "namespace": "airflow",
    ##     "image": "502940995437.dkr.ecr.us-east-1.amazonaws.com/sodimac-datalake/coletor-sap-rfc:latest"
    ## }


DATALAKE_PREFIX_PATH = "s3://sodimac-production-silver/origin=kubernetes/database=silver_sap"

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Cognitivo.ai',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': int(dag_config.get("retry_count", 3)),
    'retry_delay': timedelta(
        minutes=int(
            dag_config.get("retry_delay_minutes", 5)
        )
    ),
    'on_failure_callback': airflow_notify_sns
}
# [END default_args]s

def _generate_k8s_operator(dag_instance, RFC_NAME): 
    command = """ <<EOF 
        sap_server:
        host: {SAP_HOST}
        username: {SAP_USER}
        password: {SAP_PASSWORD}
        client: {SAP_CLIENT}

        rfc_execute:
        name: {RFC_NAME}
        params:
            IV_DATA: {{ ds }}

        save_parquet:
        path: "{DATALAKE_PREFIX_PATH}/{RFC_NAME_LOWER}"
        EOF
    """.format({
        "SAP_HOST": dag_config.get("sap_host"),
        "SAP_USER": dag_config.get("sap_user"),
        "SAP_PASSWORD": dag_config.get("sap_password"),
        "SAP_CLIENT": dag_config.get("sap_client"),
        "RFC_NAME": RFC_NAME,
        "RFC_NAME_LOWER": RFC_NAME.lower().replace('delta', ''),
        "DATALAKE_PREFIX_PATH": DATALAKE_PREFIX_PATH,
    })

    name = f"coletor_rfc_{RFC_NAME}"
    return KubernetesPodOperator(
        dag=dag_instance,
        task_id=name,
        name=name,
        namespace=dag_config.get("namespace", "default"),
        image=dag_config.get("image"),
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        cmds=[command],
    )

# [START instantiate_dag]
with DAG(
    'cdp-dag-sap-coletor-rfc',
    default_args=default_args,
    description='Run coletor RFC of data in SAP',
    schedule_interval = dag_config.get("schedule"),
    sla_miss_callback=airflow_notify_sns,
    max_active_runs=int(dag_config.get("dag_max_active_runs", 1))
) as dag:

    dag_start = DummyOperator(task_id="dag_start", dag=dag)
    dag_end = DummyOperator(task_id="dag_end", dag=dag)

    rfc_task_A155 = _generate_k8s_operator(dag, "ZFSD_A155_LISTA_PRECO_DELTA")
    rfc_task_A071 = _generate_k8s_operator(dag, "ZFSD_A071_LISTA_PRECO_DELTA")
            
    dag_start >> rfc_task_A155 >> rfc_task_A071 >> dag_end

# [END instantiate_dag]