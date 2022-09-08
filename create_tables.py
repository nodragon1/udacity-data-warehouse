import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    -After saving cluster's informations into the cfg file, get informations by parsing cfg file
    
    -Connect to the database and create cursor by formatting the informations from cfg file
    
    -Execute the functions above and close the connection
    """
    
    # parse the cfg file and get informations
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()