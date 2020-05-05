import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads rawdata from s3 to staging tables
    in redshift.

    This is done by sequentially executing
    copy_table_queries.

    Args:
        cur (cursor): A cursor object to commit
            commands to the database
        conn (connection): A connection object to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transforms data from staging tables into
    the 5 tables representing a starschema.

    This is done by Sequentially executing
    insert_table_queries

    Args:
        cur (cursor): A cursor object to commit
            commands to the database
        conn (connection): A connection object to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={}\
    password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
