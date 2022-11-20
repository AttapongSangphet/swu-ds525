import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_create = """ 
        CREATE TABLE IF NOT EXISTS actors (
            id int,
            login text,
            display_login text,
            gravatar_id text,
            url text,
            avatar_url text,
            PRIMARY KEY (id)
        );
        CREATE TABLE IF NOT EXISTS orgs (
            id int,
            login text,
            gravatar_id text,
            url text,
            avatar_url text,
            PRIMARY KEY (id)
        );
        CREATE TABLE IF NOT EXISTS repos (
            id int,
            name text,
            url text,
            PRIMARY KEY (id)
        );
        CREATE TABLE IF NOT EXISTS events (
            id text,
            repo_id int,
            org_id int,
            actor_id int,
            type text,
            public boolean,
            created_at text,
            payload_issue_id int,
            payload_action text,
            payload_issue text,
            payload_comment text,
            payload_push_id int,
            payload_size int,
            payload_distinct_size text,
            payload_ref text,
            payload_head text,
            payload_before text,
            payload_commits text,
            payload_ref_type text,
            payload_master_branch text,
            payload_description text,
            payload_pusher_type text,
            payload_release_id int,
            payload_release text,
            PRIMARY KEY (id),
            CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id),
            CONSTRAINT fk_repo FOREIGN KEY(repo_id) REFERENCES repos(id),
            CONSTRAINT fk_org FOREIGN KEY(org_id) REFERENCES orgs(id)
        );
    """

    create_table_queries = [
        table_create,
    ]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r", encoding="utf-8") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into actors table
                insert_statement = f"""
                    INSERT INTO actors (
                        id,
                        login,
                        display_login,
                        gravatar_id,
                        url,
                        avatar_url
                    ) VALUES ({each["actor"]["id"]}, '{each["actor"]["login"]}', '{each["actor"]["display_login"]}', '{each["actor"]["gravatar_id"]}', '{each["actor"]["url"]}', '{each["actor"]["avatar_url"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)


                # Insert data into orgs table
                try:
                    insert_statement = f"""
                        INSERT INTO orgs (
                            id,
                            login,
                            gravatar_id,
                            url,
                            avatar_url
                        ) VALUES ({each["org"]["id"]}, '{each["org"]["login"]}', '{each["org"]["gravatar_id"]}', '{each["org"]["url"]}', '{each["org"]["avatar_url"]}')
                        ON CONFLICT (id) DO NOTHING
                """
                    # print(insert_statement)
                    cur.execute(insert_statement)###
                except KeyError:
                    pass


                # Insert data into repos table
                insert_statement = f"""
                    INSERT INTO repos (
                        id,
                        name,
                        url
                    ) VALUES ('{each["repo"]["id"]}', '{each["repo"]["name"]}', '{each["repo"]["url"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into  events table

                try:
                    insert_statement = f"""
                        INSERT INTO events (
                            id,
                            type,
                            public,
                            created_at,
                            actor_id,
                            repo_id,
                            org_id
                        ) VALUES ('{each["id"]}', '{each["type"]}', '{each["public"]}', '{each["created_at"]}', '{each["actor"]["id"]}', '{each["repo"]["id"]}', '{each["org"]["id"]}')
                        ON CONFLICT (id) DO NOTHING
                    """
                    # print(insert_statement)
                    cur.execute(insert_statement)
                except:
                    insert_statement = f"""
                        INSERT INTO events (
                            id,
                            type,
                            public,
                            created_at,
                            actor_id,
                            repo_id
                        ) VALUES ('{each["id"]}', '{each["type"]}', '{each["public"]}', '{each["created_at"]}', '{each["actor"]["id"]}', '{each["repo"]["id"]}')
                        ON CONFLICT (id) DO NOTHING
                    """
                    # print(insert_statement)
                    cur.execute(insert_statement)


                conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 10, 15),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )
    
    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    # [get_files, create_tables] >> process
    get_files >> create_tables >> process