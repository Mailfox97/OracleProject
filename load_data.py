from lib_etl.etl import *
import numpy as np
from datetime import datetime, timedelta


def update_country():
    url_countries = "https://covid-193.p.rapidapi.com/countries"
    data = extract_data_from_api(url_countries)
    if data:
        data = data["response"]
        data = [x for x in data if x not in ['Cura&ccedil;ao', 'Diamond-Princess-',
                                             'Diamond-Princess', 'Isle-of-Man', 'MS-Zaandam', 'MS-Zaandam-',
                                             'R&eacute;union', 'Saint-Kitts-and-Nevis', 'Saint-Helena',
                                             'Saint-Kitts-and-Nevis',
                                             'Saint-Lucia', 'Saint-Martin', 'Saint-Pierre-Miquelon',
                                             'Sao-Tome-and-Principe',
                                             'Seychelles',
                                             'Solomon-Islands', 'Sint-Maarten', 'St-Barth', 'St-Vincent-Grenadines',
                                             'Turks-and-Caicos',
                                             'US-Virgin-Islands', 'Wallis-and-Futuna', 'Western-Sahara',
                                             'Antigua-and-Barbuda', 'Channel-Islands',
                                             'Micronesia', 'Guam', 'Puerto-Rico'
                                             ]]
        cols = ["country"]
        load_data_to_oracle(data, "country", "replace", cols)
        return data
    else:
        print("no data")

def update_history(country):
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    url = "https://covid-193.p.rapidapi.com/history"
    querystring = {"country": country, "day": yesterday}
    data = extract_data_from_api(url, querystring)
    if data:
        data = data["response"]
        df = pd.DataFrame(data)
        for col in ["cases", "deaths", "tests"]:
            df = flat_df(df, col)
        del df["cases"], df["deaths"], df["tests"]
        df = df.fillna(value=np.nan)
        df = df.fillna(0)
        load_data_to_oracle(df, "covid_hist", "append")
    else:
        print("no data")


def update_history_global():
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    url = "https://covid-193.p.rapidapi.com/history"
    querystring = {"country": "all", "day": yesterday}
    data = extract_data_from_api(url, querystring)
    if data:
        data = data["response"]
        df = pd.DataFrame(data)
        for col in ["cases", "deaths", "tests"]:
            df = flat_df(df, col)
        del df["cases"], df["deaths"], df["tests"], df["country"], df['continent'], df['population']
        df = df.fillna(value=np.nan)
        df = df.fillna(0)
        load_data_to_oracle(df, "covid_history_global", "append")
    else:
        print("no data")


if __name__ == '__main__':
    update_history_global()
    list_country = update_country()
    for country in list_country:
        update_history(country)
