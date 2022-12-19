import psycopg2
import json
import glob
import os
from typing import List
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


host = "redshift-endpoint"
dbname = "dbname"
user = "user"
password = "password"
port = "5439"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()


def _create_tables():

    table_drop_housingprice = "DROP TABLE IF EXISTS housingprice"
    table_drop_countbytype = "DROP TABLE IF EXISTS countbytype"
    table_drop_avgpricebyloc = "DROP TABLE IF EXISTS avgpricebyloc"
    table_drop_detachedhouse_price = "DROP TABLE IF EXISTS detachedhouse_price"

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
            cnt_by_type int
            , Property_Type text
        )
        """

    table_create_avgpricebyloc = """ 
        CREATE TABLE IF NOT EXISTS avgpricebyloc (
            AVG_Price int
            , District text
        )
        """

    table_create_detachedhouse_price = """ 
        CREATE TABLE IF NOT EXISTS detachedhouse_price (
            Transaction_id text
            , Property_Type text
            , price int
        )
        """

    create_table_queries = [
        table_drop_housingprice,
        table_drop_countbytype,
        table_drop_avgpricebyloc,
        table_drop_detachedhouse_price,
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
        COPY housingprice FROM 's3_URI'
        ACCESS_KEY_ID 'aws_access_key'
        SECRET_ACCESS_KEY 'aws_secret_access_key'
        SESSION_TOKEN 'aws_session_token'
        CSV
        IGNOREHEADER 1
    """

    cur.execute(copy_table_query)
    conn.commit()

    conn.close()

def _insert_tables():
    insert_dwh_countbytype ="""
        INSERT INTO countbytype 
        SELECT COUNT(Transaction_id)
	        , property_type
        FROM housingprice
        GROUP BY Property_Type
        """

    insert_dwh_avgpricebyloc ="""
        INSERT INTO avgpricebyloc 
        SELECT AVG(Price)
	        , District
        FROM housingprice
        WHERE property_type = 'D'
        GROUP BY  District
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