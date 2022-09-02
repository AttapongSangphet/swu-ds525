import glob
import json
import os
from typing import List

from cassandra.cluster import Cluster


table_drop = "DROP TABLE events"

table_create = """
    CREATE TABLE IF NOT EXISTS events
    (
        event_id text,
        created_at text,
        type text,
        actor_id text,
        action text,
        PRIMARY KEY (
            type,
            created_at
        )
    )
"""

create_table_queries = [
    table_create,
]
drop_table_queries = [
    table_drop,
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def get_files(filepath: str) -> List[str]:
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


def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                #print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables here
                query = f"""
                INSERT INTO events (event_id, type, created_at, actor_id) VALUES ('{each["id"]}', '{each["type"]}', '{each["created_at"]}', '{each["actor"]["id"]}')
                """
                session.execute(query)


event_types = ['IssuesEvent','PullRequestReviewCommentEvent','CreateEvent','PullRequestEvent','PushEvent','PublicEvent'
            ,'WatchEvent','DeleteEvent','PullRequestReviewEvent','ReleaseEvent', 'IssueCommentEvent']

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)

    process(session, filepath="../data")
    # Select data in Cassandra and print them to stdout
    query = """
    SELECT count(event_id), type from events GROUP BY type ALLOW FILTERING
    """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)

    for event_type in event_types:
        # Select data in Cassandra and print them to stdout
        query = """
        SELECT * from events WHERE created_at <= '2022-08-17T16:00:00Z' and type = '"""+event_type+"""' ALLOW FILTERING;
        """
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)

        for row in rows:
            print(row)



if __name__ == "__main__":
    main()