import pandas as pd
import re
from sqlalchemy import create_engine

class ETL():
    def connection(
            self,
            username: str,
            password: str,
            host: str,
            port: int,
            database: str,
            database_type: str
            ):
        """
            Create a connection to database.
            Param:
                - username: username for login to database. Ex: 'root'
                - password: password for login to database. Ex: '' if no password or 'your_password'
                - hostname: host of database. Ex: 'localhost' or '192.168.2.x'
                - port: port of database. Ex: 3306
                - database: name of database. Ex: 'movielens_movie'
                - database_type: type of database (postgresql, mysql). Ex: 'mysql+pymysql' for mysql database or 'postgresql' for postgresql database
        """
        engine = create_engine(f"{database_type}://{username}:{password}@{host}:{port}/{database}")
        return engine

    def read_data(
            self,
            conn: create_engine, 
            tables_name: list
            ):
        """
            Read data from database.
            Param:
                - conn: sqlalchemy connection to database.
                - tables_name: list names of table. Ex: ['movie', 'rating']
            Return:
                - dfs: list of dataframes.
        """
        dfs = []
        for table in tables_name:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, conn)
            dfs.append(df)
        return dfs
    
    def merge_data(
        self, 
        dfs, 
        key
        ):
        """
            Merge dataframe into one based key in tables.
            Param:
                - dfs: list of dataframes. Ex: [df1, df2]
                - key: key for join two tables. Ex: 'movieId'
        """
        if len(dfs) > 1:
            df = dfs[0].merge(dfs[1], how='left', on=key)
            df['title'] = df['title'].astype(str)
            return df
        return df[0]
    
    def get_year(self, title):
        """
            Extract year from title field.
            Param:
                - title: title of movie. Ex: 'Toy Story (2023)'
            Return:
                - year or None
        """
        try:
            return re.search(r'\((\d{4})|-\)', str(title)).group(1)
        except Exception as e:
            print(title,': ', e)
            return None

    def cleaning_data(
        self, 
        df
        ):
        """
            Clean the dataframe. Remove duplicate, NaN value, get year movie, get title movie, and split genres
            Param:
                - df: merged dataframe.
            Return:
                - df: cleaned dataframe.
        """
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        df['year'] = df['title'].apply(self.get_year)
        df['title'] = df['title'].apply(lambda x: re.sub(r'\s*\(\d{4}\)$', '', x))
        df['genres'] = df['genres'].apply(lambda x: f"{[f'{i}' for i in x.split('|')]}")
        return df

    def agg_data(
        self, 
        df, 
        ):
        """
            Transform the dataframe with aggregate function.
            Param:
                - df: cleaned dataframe
            Return:
                - df: transformed dataframe with just certain column
        """
        agg_funcs = { 
            'title': pd.Series.mode,
            'genres': pd.Series.mode,
            'year': pd.Series.mode,
            'rating': pd.Series.mean,
        }
        df = df.groupby('movieId').agg(agg_funcs).reset_index()
        
        df.rename(columns={
            'movieId': 'movie_id',
            'rating': 'rating_avg',
            'genres': 'genre'
        }, inplace=True)
        return df[['movie_id', 'title', 'year', 'genre', 'rating_avg']]

    def load_data(
        self,
        df,
        conn,
        table_name,
        schema=None
        ):
        """
            Save/load data to database.
            Param:
                - df: transformed dataframe.
                - conn: sqlalchemy connection to database.
                - table_name: list names of table. Ex: ['movie', 'rating']
                - schema: optional for database that use schema like postgresql. Ex: 'public' 
        """
        df.to_sql(table_name, conn, if_exists='append', schema=schema, index=False)

    