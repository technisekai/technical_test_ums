from helpers.etl import *

if '__main__' == __name__:
    etl = ETL()
    # make connection database
    conn = etl.connection(
        username='your_db_username', # ex: 'root'
        password='your_db_password', # ex: 'toor'
        host='your_db_host',  # ex: 'localhost'
        port=your_port, # ex: 3306
        database='your_db_name', # ex: movielens_movie
        database_type='mysql+pymysql'
        )
    # read data
    print('Read data from database: ', end='')
    dfs = etl.read_data(conn=conn, tables_name=['movie', 'rating']) # read data from table movie and rating
    print('done!')

    # merge data based on movieId field
    print('Merge the datas: ', end='')
    df = etl.merge_data(dfs, key='movieId') # merge data based on movieId field
    print('done!')

    # clean the data
    print('Clean the data: ', end='')
    df = etl.cleaning_data(df=df) # clean the data from duplicate, NaN, and extract data from title field
    print('done!')

    # aggregate the data
    print('Agg the data: ', end='')
    df = etl.agg_data(df=df)
    print('done!')

    # load data into database
    """
        You can change conn param below if you want save data in different database. 
        Just create another connection obj:
        ```
            conn_warehouse = etl.connection(
                username='your_username', 
                password='your_password', 
                host='your_host', 
                port=your_port, 
                database='your_database', 
                database_type='your_database_type'
            )
        ```
        and then change this following line:
        ```
            etl.load_data(conn=conn, df=df, table_name='agg')
        ```
        to
        ```
            etl.load_data(conn=conn_warhouse, df=df, table_name='agg')
        ```
    """
    print('Load/save the data: ', end='')
    etl.load_data(conn=conn, df=df, table_name='agg')
    print('done!')
    
