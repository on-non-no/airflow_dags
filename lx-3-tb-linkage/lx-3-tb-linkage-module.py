import pendulum
import warnings
import requests
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_nifi_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="UTC"),
    catchup=False,
) as dag:

    def get_nifi_token(url_nifi_api, username, password):
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

        payload = {
            "username": username,
            "password": password
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url_nifi_api + "/access/token", data=payload, headers=headers, verify=False)

        return response.content.decode('utf-8')

    def get_processor(url_nifi_api, processor_id, token):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(f"{url_nifi_api}/processors/{processor_id}", headers=headers, verify=False)

        if response.status_code != 200:
            raise Exception(f"Error fetching processor: {response.content.decode('utf-8')}")

        return json.loads(response.content)

    def update_processor_status(processor_id, new_state, url_nifi_api, token):
        processor = get_processor(url_nifi_api, processor_id, token)

        put_dict = {
            "revision": processor["revision"],
            "state": new_state,
            "disconnectedNodeAcknowledged": True,
        }

        payload = json.dumps(put_dict).encode("utf8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        response = requests.put(
            url_nifi_api + f"/processors/{processor_id}/run-status",
            headers=headers,
            data=payload,
            verify=False
        )

        return response.content.decode('utf-8')

    def t1():
        url_nifi_api = "https://nifi.lx-3-wise.svc.cluster.local:8443/nifi-api"
        username = "wise"
        password = "~aA0262461400"

        token = get_nifi_token(url_nifi_api, username, password)
        print(token)
        return token

    def t2(**kwargs):
        processor_id = "064ee4b0-0194-1000-94a5-f93313507777"
        new_state = "RUNNING"
        token = kwargs['ti'].xcom_pull(task_ids='py_t1')
        response = update_processor_status(processor_id, new_state, "nifi.lx-3-wise.svc.cluster.local:8443/nifi-api", token)
        print(response)
        return response

    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=t1
    )

    py_t2 = PythonOperator(
        task_id='py_t2',
        python_callable=t2,
        provide_context=True
    )

    py_t1 >> py_t2
