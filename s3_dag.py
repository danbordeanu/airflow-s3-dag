from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.models import Variable
from datetime import datetime
import requests

default_args = {
    'owner': 'data_scientist',
    'start_date': datetime(2023, 6, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_science_workflow', default_args=default_args, schedule_interval=None)

# create new S3 destination
def create_new_destination():
    url = 'https://api.almeriaindustries.com/api/user-registry/v1/destinations'
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {Variable.get("auth_token")}',
        'Content-Type': 'application/json',
    }
    payload = {
        "Alias": "mybucket",
        "Type": "s3",
        "Destination": "uploaddir",
        "AccessKey": "AKIXXX",
        "BucketName": "mybucketname",
        "Region": "eu-north-1",
        "SecretKey": "i9mvob"
    }

    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()


# call Almeria API to create SFT gateway
def deploy_sftp_gateway():
    url = 'https://api.almeriaindustries.com/api/user-registry/v1/release'
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {Variable.get("auth_token")}',
        'Content-Type': 'application/json',
    }
    payload = {
        "chart": "orangegrove/ingester-sftp",
        "destination": "my-sftp-destination",
        "externalSFTPPort": "22",
        "name": "my-release",
        "password": "SecurePassword",
        "username": "john.smith",
    }

    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()  # Raises an exception for unsuccessful API response

sftp_gateway_deploy_task = PythonOperator(
    task_id='sftp_gateway_deploy',
    python_callable=deploy_sftp_gateway,
    dag=dag,
)

def get_load_balancer_ip():
    url = 'https://api.almeriaindustries.com/api/user-registry/v1/release'
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {Variable.get("auth_token")}',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    load_balancer_ip = data['data'][0]['Network']['loadBalancerIP']
    external_sftp_port = data['data'][0]['Network']['externalSFTPPort']

    return load_balancer_ip, external_sftp_port

def execute_data_processing():
    # Your code to execute the data processing tasks and generate the output file
    pass

def transfer_to_sftp():
    load_balancer_ip, external_sftp_port = get_load_balancer_ip()
    sftp_transfer_task = SFTPOperator(
        task_id='sftp_transfer',
        ssh_conn_id='sftp_connection',
        local_filepath='/path/to/output/file.csv',  # Update with your output file path
        remote_filepath='/path/on/sftp/file.csv',
        operation='put',
        host=load_balancer_ip,
        port=int(external_sftp_port),  # Update with the appropriate port number if needed
        username='username',  # Update with the appropriate username
        password='password',  # Update with the appropriate password
        dag=dag,
    )
    return sftp_transfer_task

with dag:
    deploy_sftp_gateway_task = PythonOperator(
        task_id='deploy_sftp_gateway',
        python_callable=deploy_sftp_gateway,
    )

    get_load_balancer_ip_task = PythonOperator(
        task_id='get_load_balancer_ip',
        python_callable=get_load_balancer_ip,
    )

    execute_data_processing_task = PythonOperator(
        task_id='execute_data_processing',
        python_callable=execute_data_processing,
    )

    sftp_transfer_task = transfer_to_sftp()

    deploy_sftp_gateway_task >> get_load_balancer_ip_task >> execute_data_processing_task >> sftp_transfer_task
