import psycopg2
import json
import glob
import os
from datetime import datetime
from typing import List
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

curr_date = datetime.today().strftime('%Y-%m-%d')

host = "redshift-cluster-1.ch9yux0jr29i.us-east-1.redshift.amazonaws.com"
dbname = "dev"
user = "awsuser"
password = "kD36cC9lA9k7Jii9mokn"
port = "5439"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

aws_access_key_id = 'ASIA255IGVKUIS5PWJ5G'
aws_secret_access_key = 'r2EKh8d4HBu89pvssxkYqpVeg+cbHImtf/blBlaj'
aws_session_token = 'FwoGZXIvYXdzECAaDFM3rp2XL67jIWcFTiLOAQnRq9rT9HWwTuT2iBD7X3oY/33V1yHfbVgac1r0hQlDA/+d5G8E5C5PZrM4tV/1nDk607gLOER02q5kTXldvxB3Bp2QpqY8wGTNzgzL5CZdobKNLWb48Nnz6yh6dGEI1VYdjX9e8fUS/QiwDhlH0JcDD2lYVAbeOsEMvgw+zokKLdwUWTt838ybbE3CGhKtjMj/Zt1WXVs0Xfa9EHfMgf1OwG1jntL1xHd7mQtghrxIiVgYFfmdHykm4VLA1qdc1bwmvBdIWxYklS+pTQCEKObL/JwGMi23KSH6KeLIeY2notXsZMjOJ3jhZQp7/9bEZItOYEDRJDYnYBKGHT8Mpovbi/8='

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
        ACCESS_KEY_ID 'ASIA255IGVKUIS5PWJ5G'
        SECRET_ACCESS_KEY 'r2EKh8d4HBu89pvssxkYqpVeg+cbHImtf/blBlaj'
        SESSION_TOKEN 'FwoGZXIvYXdzECAaDFM3rp2XL67jIWcFTiLOAQnRq9rT9HWwTuT2iBD7X3oY/33V1yHfbVgac1r0hQlDA/+d5G8E5C5PZrM4tV/1nDk607gLOER02q5kTXldvxB3Bp2QpqY8wGTNzgzL5CZdobKNLWb48Nnz6yh6dGEI1VYdjX9e8fUS/QiwDhlH0JcDD2lYVAbeOsEMvgw+zokKLdwUWTt838ybbE3CGhKtjMj/Zt1WXVs0Xfa9EHfMgf1OwG1jntL1xHd7mQtghrxIiVgYFfmdHykm4VLA1qdc1bwmvBdIWxYklS+pTQCEKObL/JwGMi23KSH6KeLIeY2notXsZMjOJ3jhZQp7/9bEZItOYEDRJDYnYBKGHT8Mpovbi/8='
        CSV
        DELIMITER ','
        IGNOREHEADER 1
    """

    for query in copy_table_query:
        cur.execute(query.format(curr_date, aws_access_key_id, aws_secret_access_key, aws_session_token))        
        conn.commit()

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
    
    copy_tables = PythonOperator(
        task_id="copy_tables",
        python_callable=_copy_tables,
    )

    # [get_files, create_tables] >> process
    create_tables >> copy_tables