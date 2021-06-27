import json
import requests
import pandas as pd
from pandas import DataFrame
from sqlalchemy.types import FLOAT, NUMERIC, BOOLEAN, DATE, TIMESTAMP
from sqlalchemy.dialects.oracle import VARCHAR2
from lib_etl.sqlora import *
from datetime import datetime, date


def extract_data_from_api(url, query_string=None):
    headers = {
        'x-rapidapi-key': "867a22e3e9msh5cf9eecbdfcbfe2p10ad47jsn57b55f1bc70f",
        'x-rapidapi-host': "covid-193.p.rapidapi.com"
    }
    if not query_string:
        response = requests.request("GET", url, headers=headers)
    else:
        response = requests.request("GET", url, headers=headers, params=query_string)
    data = response.json()
    if len(data["response"]) == 0:
        return False
    return data


# def remove_character(serie, character):
#     serie = serie.str.


def flat_df(df, column_name):
    df_flat = pd.DataFrame(df[column_name].values.tolist())
    del df_flat['1M_pop']
    df_flat.columns = [str(x) + "_" + str(column_name) for x in df_flat.columns]
    # for col in df_flat.columns:
    #     df_flat[col] = df_flat[col].str.replace('+', '')
    df = pd.concat([df, df_flat], axis=1)
    return df


def clean_df(df: DataFrame):
    df = df.replace(['Australia/Oceania'],'Oceania')
    if 'CONTINENT' in df.columns:
        conti = df.loc[df['CONTINENT'].isin(['Asia', 'Europe', 'North-America', 'South-America', 'Africa', 'Oceania']), 'CONTINENT'].values[0]
        df.loc[~df['CONTINENT'].isin(['Asia', 'Europe', 'North-America', 'South-America', 'Africa', 'Oceania']),"CONTINENT"] = conti
    return df

def transform_datatype(df):
    print("transform_datatype>>>")
    dict_type = df.dtypes.apply(lambda x: x.name).to_dict()
    if 'DATE' in dict_type.keys():
        dict_type['DATE_INFO'] = dict_type['DATE']
        del dict_type['DATE']
        df.rename(columns={"DATE": "DATE_INFO"}, inplace=True)
    elif 'DAY' in dict_type.keys():
        dict_type['DATE_INFO'] = dict_type['DAY']
        del dict_type['DAY']
        df.rename(columns={"DAY": "DATE_INFO"}, inplace=True)
    if 'TIME' in dict_type.keys():
        dict_type['DATETIME_INFO'] = dict_type['TIME']
        del dict_type['TIME']
        df.rename(columns={"TIME": "DATETIME_INFO"}, inplace=True)
    for k, type in dict_type.items():
        if type == 'object':
            if k in ["COUNTRYCODE", "SLUG", "COUNTRY", "ISO2", "CONTINENT"]:
                df[k] = df[k].astype(str)
                dict_type[k] = VARCHAR2(256)
            elif k in ["NEW_CASES", "NEW_DEATHS"]:
                df[k] = df[k].astype(str)
                df[k] = df[k].str.replace('+', '')
                df[k] = df[k].astype(float)
                dict_type[k] = FLOAT()
            elif k in ["CRITICAL_CASES", "TOTAL_DEATHS", "TOTAL_TESTS"]:
                df[k] = df[k].astype(float)
                dict_type[k] = FLOAT()
            elif k in ["POPULATION"]:
                df[k] = df[k].astype(float)
                dict_type[k] = FLOAT()
            else:
                df[k] = pd.to_datetime(df[k])
                if k in ["DATETIME_INFO"]:
                    dict_type[k] = TIMESTAMP()
                elif k in ["DATE_INFO"]:
                    dict_type[k] = DATE()

        elif type == 'int64' or type == 'float64' or type == 'decimal':
            dict_type[k] = FLOAT()

        elif type == 'bool':
            dict_type[k] = BOOLEAN()
        elif type == 'string':
            if k in ["LAT", "LON"]:
                dict_type[k] = FLOAT()
            else:
                dict_type[k] = VARCHAR2(256)
    return dict_type

def create_fake_data(df: DataFrame):
    columns = df.columns
    df_fake = pd.DataFrame(columns=columns)
    for col in columns:
        if col in ["DATE_INFO"]:
            df_fake[col] = [datetime(2000, 1, 1)]
        elif col in ["DATETIME_INFO"]:
            df_fake[col] = [datetime(2000, 1, 1, 0, 0, 0)]
        elif col in ["CONTINENT", "COUNTRY"]:
            df_fake[col] = ["fake region"]
        else:
            df_fake[col] = [0]
    for i in range(12):
        df_fake = pd.concat([df_fake, df_fake])

    print(len(df_fake))
    print(df_fake.dtypes)
    return df_fake


def load_data_to_oracle(data, table_name, if_exist, columns=None):
    print('>>>load data to oracle')
    if columns:
        df = pd.DataFrame(data, columns=columns)
    else:
        df = pd.DataFrame(data)

    df.columns = map(str.upper, df.columns)
    print("old type>>>>", df.dtypes)
    dict_type = transform_datatype(df)
    print("dict type>>>>", dict_type)
    df = clean_df(df)
    if table_name == "covid_history":
        df_fake = create_fake_data(df)
        df = pd.concat([df, df_fake])
    engine = create_conn()
    engine.connect().execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' """)
    engine.connect().execute("""    ALTER
                                    SESSION
                                    SET
                                    NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS' """)

    df.to_sql(name=table_name, con=engine, if_exists=if_exist, index=False, chunksize=1000, dtype=dict_type)


def load_data_to_table(url, table_name, if_exist):
    country = extract_data_from_api(url)
    load_data_to_oracle(country, table_name, if_exist)


