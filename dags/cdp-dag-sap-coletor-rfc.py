import boto3
import json

from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from botocore.exceptions import ClientError

# Error notifications to SNS
# from airflow_notify_sns import airflow_notify_sns
airflow_notify_sns = lambda x : True

from airflow.exceptions import AirflowException
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes.client import models as k8s

try:
    dag_config = Variable.get("sap_coletor_rfc", deserialize_json=True)
except Exception as ex:
    raise AirflowException("Variable sap_coletor_rfc must be set.")  


def get_secret(secret_name, region_name = "us-east-1"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.
    return secret


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

    secrets_config = json.loads(get_secret(
        secret_name=dag_config["secret_name"]
    ))

    name = f"coletor_rfc_{RFC_NAME}"
    SAP_PARAMS = json.dumps({
        "IV_DATA": "{{ ds }}"
    })
    return KubernetesPodOperator(
        dag=dag_instance,
        task_id=name,
        name=name,
        namespace=dag_config.get("namespace", "default"),
        image=dag_config.get("image"),
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        env_vars=[
            k8s.V1EnvVar(
                name="SAP_HOST", value=secrets_config["sap_host"]
            ),
            k8s.V1EnvVar(
                name="SAP_USER", value=secrets_config["sap_user"]
            ),
            k8s.V1EnvVar(
                name="SAP_PASSWORD", value=secrets_config["sap_password"]
            ),
            k8s.V1EnvVar(
                name="SAP_CLIENT", value=str(secrets_config["sap_client"])
            ),
            k8s.V1EnvVar(
                name="RFC_NAME", value=RFC_NAME
            ),
            k8s.V1EnvVar(
                name="RFC_NAME_LOWER", value=RFC_NAME.lower().replace('_delta', '')
            ),
            k8s.V1EnvVar(
                name="DATALAKE_PREFIX_PATH", value=DATALAKE_PREFIX_PATH
            ),
            k8s.V1EnvVar(
                name="SAP_PARAMS", value=SAP_PARAMS
            ),
        ],
        cmds=["/bin/bash", "-c", "make-config-file && cat /tmp/config.yaml | run-coletor"],
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