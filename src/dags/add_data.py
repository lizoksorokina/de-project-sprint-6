from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import vertica_python


VERTICA_CONN_INFO = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv2025011437',
    'password': 'edGGFqlUiHVIwSv',
    'database': 'dwh',
    'autocommit': True,
}


def copy_to_vertica(file_path: str, table_name: str):
    with open(file_path, 'r') as f:
        next(f)  # пропускаем заголовок
        conn = vertica_python.connect(**VERTICA_CONN_INFO)
        cur = conn.cursor()
        cur.copy(f"""
            COPY {table_name} FROM STDIN DELIMITER ',' ENCLOSED BY '\"'
        """, f)
        cur.close()
        conn.close()

default_args = {
    'start_date': datetime(2022, 7, 13)
}

with DAG('staging_load_to_vertica',
         schedule_interval=None,
         default_args=default_args,
         catchup=False) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    load_users = PythonOperator(
        task_id='load_users',
        python_callable=copy_to_vertica,
        op_kwargs={'file_path': '/data/users.csv', 'table_name': 'STV2025011437__STAGING.users '}
    )

    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=copy_to_vertica,
        op_kwargs={'file_path': '/data/groups.csv', 'table_name': 'STV2025011437__STAGING.groups'}
    )

    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=copy_to_vertica,
        op_kwargs={'file_path': '/data/dialogs.csv', 'table_name': 'STV2025011437__STAGING.dialogs'}
    )

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=copy_to_vertica,
        op_kwargs={'file_path': '/data/group_log.csv', 'table_name': 'STV2025011437__STAGING.group_log'}
    )

    start >> [load_users, load_groups, load_dialogs, load_group_log] >> end