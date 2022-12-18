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

    table_create_countbytype = """ 
        CREATE TABLE IF NOT EXISTS countbytype (
            Transaction_id text
            , Property_Type text
        )
        """

    table_create_detachedhouse_price = """ 
        CREATE TABLE IF NOT EXISTS avgpricebyloc (
            Transaction_id text
            , Property_Type text
            , Price int
            , District text
            , Town_or_City text
        )
        """

    table_create_avgpricebyloc = """ 
        CREATE TABLE IF NOT EXISTS detachedhouse_price (
            Transaction_id text
            , Property_Type text
            , price int
        )
        """

    create_table_queries = [
        table_drop_housingprice,
        table_create_housingprice,
        table_create_countbytype,
        table_create_detachedhouse_price,
        table_create_avgpricebyloc
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
        IGNOREHEADER 1
    """

    cur.execute(copy_table_query)
    conn.commit()

    conn.close()

def _insert_tables():
    insert_dwh_countbytype ="""
        INSERT INTO countbytype 
        SELECT Transaction_id
            , Property_Type
        FROM housingprice
        """

    insert_dwh_avgpricebyloc ="""
        INSERT INTO avgpricebyloc 
        SELECT Transaction_id
            , Property_Type
            , Price
            , District
            , Town_or_City
        FROM housingprice
        """

    insert_dwh_detachedhouse_price ="""
        INSERT INTO detachedhouse_price 
        SELECT Transaction_id
            , Property_Type
            , Price
        FROM housingprice
        WHERE property_type = 'D'
        """

    insert_table_queries = [
        insert_dwh_countbytype,
        insert_dwh_avgpricebyloc,
        insert_dwh_detachedhouse_price,
    ]

    for query in insert_table_queries:
        cur.execute(query)
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

    insert_tables = PythonOperator(
    task_id="insert_tables",
    python_callable=_insert_tables,
    )

    # [get_files, create_tables] >> process
    create_tables >> copy_tables >> insert_tables