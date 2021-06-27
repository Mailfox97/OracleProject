from lib_etl.etl import *
import pandas as pd
import numpy as np
import os



def drop_all_table():
    list_table = ["country", "covid_history", "covid_history_global"]
    for table in list_table:
        if check_exist(table):
            sql_ddl(f"drop table {table}")

def create_all_table():
    drop_all_table()
    for file in os.listdir("./sql_definition/"):
        if file.endswith(".sql"):
            with open("./sql_definition/" + file) as f:
                query = f.readlines()
                query = "".join(query)
                sql_ddl(query)







def init_country():
    url_countries = "https://covid-193.p.rapidapi.com/countries"
    data = extract_data_from_api(url_countries)
    if data:
        data = data["response"]
        data = [x for x in data if x not in ['Cura&ccedil;ao','Diamond-Princess-',
                                             'Diamond-Princess','Isle-of-Man','MS-Zaandam','MS-Zaandam-',
                                             'R&eacute;union','Saint-Kitts-and-Nevis','Saint-Helena','Saint-Kitts-and-Nevis',
                                             'Saint-Lucia','Saint-Martin','Saint-Pierre-Miquelon','Sao-Tome-and-Principe','Seychelles',
                                             'Solomon-Islands','Sint-Maarten','St-Barth','St-Vincent-Grenadines','Turks-and-Caicos',
                                             'US-Virgin-Islands','Wallis-and-Futuna','Western-Sahara','Antigua-and-Barbuda','Channel-Islands',
                                             'Micronesia','Guam','Puerto-Rico'
                                             ]]
        cols = ["country"]
        load_data_to_oracle(data, "country", "replace", cols)
        return data
    else:
        print("no data")

def init_history(country):
    url = "https://covid-193.p.rapidapi.com/history"
    querystring = {"country": country}
    table_name = "covid_history"
    data = extract_data_from_api(url,querystring)
    if data:
        data = data["response"]
        df = pd.DataFrame(data)
        for col in ["cases", "deaths", "tests"]:
            df = flat_df(df, col)
        del df["cases"], df["deaths"], df["tests"]
        df = df.fillna(value=np.nan)
        df = df.fillna(0)
        load_data_to_oracle(df, table_name, "append")
    else:
        print("no data")

def init_history_global():
    url = "https://covid-193.p.rapidapi.com/history"
    querystring = {"country": "all"}
    table_name = "covid_history_global"
    data = extract_data_from_api(url,querystring)
    if data:
        data = data["response"]
        df = pd.DataFrame(data)
        for col in ["cases", "deaths", "tests"]:
            df = flat_df(df, col)
        del df["cases"], df["deaths"], df["tests"], df["country"], df['continent'], df['population']
        df = df.fillna(value=np.nan)
        df = df.fillna(0)
        load_data_to_oracle(df, table_name, "append")
    else:
        print("no data")


if __name__ == '__main__':
    create_all_table()
    init_history_global()
    list_country = init_country()
    for country in list_country:
        init_history(country.lower())

