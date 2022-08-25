import psycopg2


table_drop = "DROP TABLE IF EXISTS Event, Actor, Repo, Org"

table_create = """ 
    CREATE TABLE IF NOT EXISTS Event (
        E_id int NOT NULL,
        R_id int NOT NULL,
        O_id int NOT NULL,
        A_id int NOT NULL,
        E_type varchar(250),
        E_Public varchar(250),
        E_create_at varchar(250),
        PRIMARY KEY (E_id)
    );
    CREATE TABLE IF NOT EXISTS Actor (
        A_id int NOT NULL,
        login varchar(250),
        display_login varchar(250),
        A_gravatar_id integer,
        A_url varchar(250),
        A_avatar_url varchar(250),
        PRIMARY KEY (A_id)
    );
    CREATE TABLE IF NOT EXISTS Org (
        O_id int NOT NULL,
        login varchar(250),
        O_gravatar_id integer,
        O_url varchar(250),
        O_avatar_url varchar(250),
        PRIMARY KEY (O_id)
    );
    CREATE TABLE IF NOT EXISTS Repo (
        R_id int NOT NULL,
        R_name varchar(250),
        R_url varchar(250),
        PRIMARY KEY (R_id)
    );
    ALTER TABLE Event
    ADD FOREIGN KEY (R_id) REFERENCES Repo(R_id);

    ALTER TABLE Event
    ADD FOREIGN KEY (O_id) REFERENCES Org(O_id);

    ALTER TABLE Event
    ADD FOREIGN KEY (A_id) REFERENCES Actor(A_id);
"""

create_table_queries = [
    table_create,
]
drop_table_queries = [
    table_drop,
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