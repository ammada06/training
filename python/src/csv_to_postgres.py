import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os

def load_data(connection_engine):
    df = pd.read_csv(r"C:\Users\Ammad Ashraf\Documents\training\python\src\nvidia.csv")
    df.to_sql("nvidia_stock_price", connection_engine, if_exists='replace', index=False)

def query_data(connection_engine):
    query = "select * from nvidia_stock_price where \"Date\" = '1999-03-12'"
    df = pd.read_sql(query, connection_engine)
    print(df)

if __name__ == '__main__':
    try:
        url = "postgresql+psycopg2://consultants:WelcomeItc%402022@ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        sql_engine = create_engine(url)

        load_data(sql_engine)
        query_data(sql_engine)

    except Exception as ex:
        print("Failed to connect: ", ex)