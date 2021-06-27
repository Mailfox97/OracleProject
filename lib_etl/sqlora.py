import cx_Oracle
from sqlalchemy import create_engine
import pandas as pd

def create_conn():
    host = 'localhost'
    port = 1521
    sid = 'XE'
    user = 'ngan'
    password = 'ngan'
    dsn = cx_Oracle.makedsn(host, port, sid=sid)
    engine = create_engine(f'oracle+cx_oracle://{user}:{password}@{dsn}', echo=True)
    return engine


def truncate_table(table_name):
    engine = create_conn()
    query = f"truncate table {table_name}"
    engine.connect().execute(query)

def check_exist(table_name):
    engine = create_conn()
    query = f"select count(*) from user_tables where table_name='{table_name}'"
    result = engine.connect().execute(query).fetchall()[0][0]
    if result > 0:
        return True
    else:
        return False


def sql_select(query):
    engine = create_conn()
    result = engine.connect().execute(query)
    return result


def sql_select_df(query):
    engine = create_conn()
    result = engine.connect().execute(query)
    df = pd.DataFrame(result)
    return df

def sql_ddl(query):
    engine = create_conn()
    engine.connect().execute(query)

def sql_dml(query):
    engine = create_conn()
    engine.connect().execute(query)
    engine.connect().execute("commit")

if __name__ == "__main__":
    oracle_engine = create_conn()
    country_query = "select * from country"
    # data_query = "select * from "
    df_country = pd.read_sql_query(country_query, oracle_engine)
    df_country = pd.concat([df_country['country'], df_country['country']], axis=1, keys=['label', 'value'])
    data_country = list(df_country.T.to_dict().values())
    print(data_country)