import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This query will be responsible for loading the data from S3 into staging tables for Redshift

    Parameters:
           cur: The cursor for the current db connection
           conn: The connection value returned by psycopg2 for the current db 
    Returns:
          None

   """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This query will be responsible for inserting the data from the staging tables into each of the fact/dimension tables


    Parameters:
           cur: The cursor for the current db connection
           conn: The connection value returned by psycopg2 for the current db 
    Returns:
          None

   """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()