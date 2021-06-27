from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
import pandas as pd
from lib_etl.sqlora import *
from lib_etl.utils import get_query_from_file

# Create your views here.
def home(request):
    # Connect to DB
    oracle_engine = create_conn()

    # Get list country
    country_query = "select * from country"
    df_country = pd.read_sql_query(country_query, oracle_engine)
    list_country = []
    if "Global" not in list_country:
        list_country = ["Global"] + df_country["country"].tolist()

    # Get country from selectpicker
    country = request.GET.get('select_country')
    if country in list_country:
        list_country.remove(country)

    if country == 'Global' or not country:
        # Global data
        query = get_query_from_file("\projects\OracleProject\lib_etl\select_query\global.sql")
        query_7 = get_query_from_file("\projects\OracleProject\lib_etl\select_query\global7.sql")
    else:
        query = get_query_from_file("\projects\OracleProject\lib_etl\select_query\country.sql")
        query = query.replace("{country}", country)
        query_7 = get_query_from_file("\projects\OracleProject\lib_etl\select_query\country7.sql")
        query_7 = query_7.replace("{country}", country)

    df = pd.read_sql_query(query, oracle_engine)
    df_7 = pd.read_sql_query(query_7, oracle_engine)
    list_confirmed_7 = df_7["new_cases"].tolist()
    list_death_7 = df_7["new_deaths"].tolist()
    list_day_7 = df_7["date_info"].tolist()
    list_day_7 = [d.strftime("%Y-%m-%d") for d in list_day_7]
    context = {
        "list_country": list_country,
        "confirmed": "{:,}".format(int(df["total_cases"].values[0])),
        "deaths": "{:,}".format(int(df["total_deaths"].values[0])),
        "recovered": "{:,}".format(int(df["recovered_cases"].values[0])),
        "update_time": pd.to_datetime(str(df["datetime_info"].values[0])).strftime('%Y-%m-%d'),
        "country": country,
        "list_confirmed_7": list_confirmed_7,
        "list_death_7":  list_death_7,
        "list_day_7": list_day_7

    }


    return render(request,'main/welcome.html',context)





