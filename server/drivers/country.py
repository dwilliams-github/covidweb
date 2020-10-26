import altair as alt
import pandas as pd
from datetime import date
from os import path
from flask import current_app as app
import redis, requests, time, pyarrow, hashlib


def connect():
    return redis.Redis( host=app.config['REDIS_HOST'], port=app.config['REDIS_PORT'] )

def fetchGlobal(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Returned cached value, if no more than 10 minutes old
    #
    expires = rconn.hget("country2","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("country2","dataframe"))

    #
    # Fetch
    #
    answer = pd.read_csv("https://covid.ourworldindata.org/data/ecdc/locations.csv")
    answer['code'] = answer['countriesAndTerritories']
    answer = answer.rename(columns={'location':'name'})

    #
    # Cache
    #
    rconn.hset("country2","dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("country2","expires",str(time.time()+600.0))
    return answer


#
# For ourworldindata, tables are returned for all countries,
# since this method now fetches all countries, and simply
# extracts the relative information from the appropriate tables. 
#
def fetchCases(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Returned cached value, if no more than 10 minutes old
    #
    expires = rconn.hget("country2cases","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("country2cases","dataframe"))

    answer = pd.read_csv("https://covid.ourworldindata.org/data/ecdc/new_cases.csv",parse_dates=["date"])

    #
    # Cache
    #
    rconn.hset("country2cases","dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("country2cases","expires",str(time.time()+600.0))
    return answer

def fetchDeaths(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Returned cached value, if no more than 10 minutes old
    #
    expires = rconn.hget("country2deaths","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("country2deaths","dataframe"))

    answer = pd.read_csv("https://covid.ourworldindata.org/data/ecdc/new_deaths.csv",parse_dates=["date"])

    #
    # Cache
    #
    rconn.hset("country2deaths","dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("country2deaths","expires",str(time.time()+600.0))
    return answer

    
def fetchCountry(rconn,name="United States"):
    cases = fetchCases(rconn)
    deaths = fetchDeaths(rconn)

    cases['cases'] = cases[name].fillna(0).astype(int)
    deaths['deaths'] = deaths[name].fillna(0).astype(int)

    cases = cases.filter(items=("date","cases"))
    deaths = deaths.filter(items=("date","deaths"))

    return cases.merge(deaths,on="date").sort_values(by="date")


def codes():
    r = connect()
    ckey = fetchGlobal(r)

    return {
        'abbrev': dict(zip(ckey.code,ckey.name)),
        'default': "United_States_of_America"
    }


def menu():
    r = connect()
    ckey = fetchGlobal(r)
    ckey.sort_values(by="name")
    return {
        'abbrev': dict(zip(ckey.code,ckey.name)),
        'default': "United_States_of_America"
    }


def plot(code):
    r = connect()
    ckey = fetchGlobal(r)
    dt = fetchCountry(r,ckey[ckey.code==code].name)

    dt['croll'] = dt.cases.rolling(window=7).mean()
    dt['droll'] = dt.deaths.rolling(window=7).mean()
    dt = dt[dt.date >= pd.to_datetime(date(2020,3,1))]

    #
    # This are fake, so we can make a legend
    #
    dt['src1'] = "Daily"
    dt['src2'] = "7 day"

    chart = alt.Chart(dt)

    fake_scale = alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

    case_points = chart.mark_line(point=True).encode(
        x = alt.X("date:T",title="Date"),
        y = alt.Y("cases:Q",title="Cases"),
        color = alt.Color("src1", scale=fake_scale)
    )
    case_average = chart.mark_line().encode(
        x = alt.X('date:T'),
        y = alt.Y('croll:Q'),
        color = alt.Color("src2", scale=fake_scale)
    )

    death_points = chart.mark_line(
        point={"color": "lightgrey"}, 
        color="lightgrey"
    ).encode(
        x = alt.X("date:T", title="Date"),
        y = alt.Y("deaths:Q",title="Fatalities")
    )
    death_average = chart.mark_line().encode(
        x = alt.X('date:T'),
        y = alt.Y('droll:Q')
    )

    top = (case_points + case_average).properties(
        width=500, 
        height=200,
        title=ckey[ckey.code==code].name.to_string(index=False)
    )
    bot = (death_points + death_average).properties(width=500, height=200)

    return (top & bot).configure_legend(title=None).to_dict()


def countries(names):
    r = connect()
    ckey = fetchGlobal(r)

    def make_one(name):
        dt = fetchCountry(r,name)
        ckey0 = ckey[ckey.name == name]
        dt['croll'] = dt.cases.rolling(window=7).mean()*100000/ckey0.population.mean()
        dt['droll'] = dt.deaths.rolling(window=7).mean()*100000/ckey0.population.mean()
        dt = dt[dt.date >= pd.to_datetime(date(2020,3,1))]
        dt['Country'] = name
        return dt

    dt = pd.concat([make_one(c) for c in names])

    chart = alt.Chart(dt)
    
    top = chart.mark_line().encode(
        x = alt.X('date:T', title="Date"),
        y = alt.Y('croll:Q', title="Cases per 100,000"),
        color = alt.Color("Country:N")
    ).properties(
        width=500, 
        height=200
    )

    bot = chart.mark_line().encode(
        x = alt.X('date:T', title="Date"),
        y = alt.Y('droll:Q', title="Fatalities per 100,000"),
        color = alt.Color("Country:N")
    ).properties(
        width=500, 
        height=200
    )

    return (top & bot).configure_legend(title=None).to_dict()


def scandinavia():
    return countries(['Sweden','Norway','Denmark'])

def north_america():
    return countries(['United States','Canada','Mexico'])

def europe():
    return countries(['United Kingdom','France','Spain','Germany'])