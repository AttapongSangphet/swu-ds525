import psycopg2


table_drop_events = "DROP TABLE IF EXISTS events"
table_drop_actors = "DROP TABLE IF EXISTS actors"
table_drop_repos = "DROP TABLE IF EXISTS repos"
table_drop_orgs = "DROP TABLE IF EXISTS orgs"

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
drop_table_queries = [
    table_drop_events, table_drop_actors, table_drop_repos, table_drop_orgs
]


def drop_tables(cur, conn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()