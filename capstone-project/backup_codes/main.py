import psycopg2


def main():
    host = "redshift-cluster-1.ch9yux0jr29i.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "kD36cC9lA9k7Jii9mokn"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    # Drop table if it exists
    drop_table_query = "DROP TABLE IF EXISTS housingprice"
    cur.execute(drop_table_query)
    conn.commit()

    # Create table
    create_table_query = """
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
    cur.execute(create_table_query)
    conn.commit()

    # Copy data from S3 to the table we created above
    copy_table_query = """
    COPY housingprice FROM 's3://tands525/price_paid_records01.csv'
    ACCESS_KEY_ID 'ASIA255IGVKUMQXAHV5J'
    SECRET_ACCESS_KEY 'Knxbhfc8myuUxFWehPed/E+PXGE1ZpZcTke66U5E'
    SESSION_TOKEN 'FwoGZXIvYXdzEBsaDHia1WQKIZUX4lk7lyLOAUPpsXhutoUSEGrAafrDWnfXSBc59yXyZmCToJHEMCcuyfh5qyneL/4+ZmBQtFD5/KLXd3F6J3dJElqU/4+3aUP0s3tiXGwMmYg3j64KvsxQGVmFx3FqrmqVWIh1n7LC98JTBtTedqoqRuKBkzHPesGc2W78dw1Ikkz01eSZWbGcm6mlbKOW5FXTxU23HRMlop0oJgAWEX2sD3tmUa9FfVs8U/MwmDypocsCf3Vo7R9iWNvIiAt0P7T4B+6668VAj5qvGvRVuuKFhVVYN2YGKPjC+5wGMi3K7pPsghchdIlCqNMaPG3fc+TrnxH5FUlnKbylPbmPMz++Rw/mx+M9qi+CahM='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
    cur.execute(copy_table_query)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()