import psycopg2
import json
import glob
import os
from typing import List
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

host = "redshift-cluster-1.ch9yux0jr29i.us-east-1.redshift.amazonaws.com"
dbname = "dev"
user = "awsuser"
password = "kD36cC9lA9k7Jii9mokn"
port = "5439"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

aws_access_key_id = "ASIA255IGVKUMQXAHV5J"
aws_secret_access_key = "Knxbhfc8myuUxFWehPed/E+PXGE1ZpZcTke66U5E"
aws_session_token = "FwoGZXIvYXdzEBsaDHia1WQKIZUX4lk7lyLOAUPpsXhutoUSEGrAafrDWnfXSBc59yXyZmCToJHEMCcuyfh5qyneL/4+ZmBQtFD5/KLXd3F6J3dJElqU/4+3aUP0s3tiXGwMmYg3j64KvsxQGVmFx3FqrmqVWIh1n7LC98JTBtTedqoqRuKBkzHPesGc2W78dw1Ikkz01eSZWbGcm6mlbKOW5FXTxU23HRMlop0oJgAWEX2sD3tmUa9FfVs8U/MwmDypocsCf3Vo7R9iWNvIiAt0P7T4B+6668VAj5qvGvRVuuKFhVVYN2YGKPjC+5wGMi3K7pPsghchdIlCqNMaPG3fc+TrnxH5FUlnKbylPbmPMz++Rw/mx+M9qi+CahM="


def _create_tables():

    table_drop_housingprice = "DROP TABLE IF EXISTS housingprice"

    table_create_housingprice = """ 
        CREATE TABLE IF NOT EXISTS housingprice (
            Transaction_id text
            , Price int
            , Date_of_Transfer datetime
            , Property_Type text
            , Old_or_New text
            , Duration text
            , Town_or_City text
            , District text
            , County text
            , PPDCategory_Type text
            , Record_Status_monthly_file_only text
        )
        """


    create_table_queries = [
        table_drop_housingprice,
        table_create_housingprice,
    ]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _copy_tables():
    copy_table_query = """
        COPY housingprice FROM 's3://tands525/price_paid_records01.csv'
        ACCESS_KEY_ID 'ASIA255IGVKUMQXAHV5J'
        SECRET_ACCESS_KEY 'Knxbhfc8myuUxFWehPed/E+PXGE1ZpZcTke66U5E'
        SESSION_TOKEN 'FwoGZXIvYXdzEBsaDHia1WQKIZUX4lk7lyLOAUPpsXhutoUSEGrAafrDWnfXSBc59yXyZmCToJHEMCcuyfh5qyneL/4+ZmBQtFD5/KLXd3F6J3dJElqU/4+3aUP0s3tiXGwMmYg3j64KvsxQGVmFx3FqrmqVWIh1n7LC98JTBtTedqoqRuKBkzHPesGc2W78dw1Ikkz01eSZWbGcm6mlbKOW5FXTxU23HRMlop0oJgAWEX2sD3tmUa9FfVs8U/MwmDypocsCf3Vo7R9iWNvIiAt0P7T4B+6668VAj5qvGvRVuuKFhVVYN2YGKPjC+5wGMi3K7pPsghchdIlCqNMaPG3fc+TrnxH5FUlnKbylPbmPMz++Rw/mx+M9qi+CahM='
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
    """

with DAG(
    "etl",
    start_date=timezone.datetime(2022, 12, 15),
    schedule="@weekly",
    tags=["workshop"],
    catchup=False,
) as dag:

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )
    
    # process = PythonOperator(
    #     task_id="process",
    #     python_callable=_process,
    # )

    # [get_files, create_tables] >> process
    create_tables >> copy_tables