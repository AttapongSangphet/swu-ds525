import psycopg2


drop_table_queries = [
    "DROP TABLE IF EXISTS events",
    "DROP TABLE IF EXISTS actors",
    "DROP TABLE IF EXISTS repos",
    "DROP TABLE IF EXISTS orgs",
    "DROP TABLE IF EXISTS staging_events"
]
create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id text,
        type text,
        created_at text,
        public boolean,
        actor_id int,
        actor_login text,
        actor_display_login text,
        actor_gravatar_id text,
        actor_url text,
        actor_avatar_url text,
        repo_id int,
        repo_name text,
        repo_url text,
        org_id int,
        org_login text,
        org_gravatar_id text,
        org_url text,        
        org_avatar_url text        
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS actors (
        id int,
        login text,
        display_login text,
        gravatar_id text,
        url text,
        avatar_url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS repos (
        id int,
        name text,
        url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orgs (
        id int,
        login text,
        gravatar_id text,
        url text,
        avatar_url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id text,
        type text,
        public boolean,
        created_at text
    )
    """,
]
copy_table_queries = [
    """
    COPY staging_events FROM 's3://tands525/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::751435164328:role/LabRole'
    JSON 's3://tands525/events_json_path.json'
    REGION 'us-east-1'
    """,
]
insert_table_queries = [
    """
    INSERT INTO
      actors (
        id,
        login,
        display_login,
        gravatar_id,
        url,
        avatar_url
      )
    SELECT
      DISTINCT actor_id,
        actor_login,
        actor_display_login,
        actor_gravatar_id,
        actor_url,
        actor_avatar_url
    FROM
      staging_events
    WHERE
      actor_id NOT IN (SELECT DISTINCT id FROM actors)
    """,
        """
    INSERT INTO
      repos (
        id,
        name,
        url
      )
    SELECT
      DISTINCT repo_id,
        repo_name,
        repo_url
    FROM
      staging_events
    WHERE
      repo_id NOT IN (SELECT DISTINCT id FROM repos)
    """,
        """
    INSERT INTO
      orgs (
        id,
        login,
        gravatar_id,
        url,
        avatar_url
      )
    SELECT
      DISTINCT org_id,
        org_login,
        org_gravatar_id,
        org_url,        
        org_avatar_url     
    FROM
      staging_events
    WHERE
      org_id NOT IN (SELECT DISTINCT id FROM orgs)
    """,
        """
    INSERT INTO
      events (
        id,
        type,
        created_at,
        public
      )
    SELECT
      DISTINCT id ,
        type,
        created_at,
        public
    FROM
      staging_events
    WHERE
      id NOT IN (SELECT DISTINCT id FROM events)
    """,
]


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-tands525.ch9yux0jr29i.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "hT51cr6y"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_tables(cur, conn)
    insert_tables(cur, conn)

    query = "select * from events"
    cur.execute(query)
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()