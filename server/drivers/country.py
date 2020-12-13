import altair as alt
import pandas as pd
from datetime import date
from os import path
from flask import current_app as app
import redis, requests, time, pyarrow, hashlib


def connect():
    return redis.Redis( host=app.config['REDIS_HOST'], port=app.config['REDIS_PORT'] )

#
# OurWorldInData has switched to a universal big blob of data
# See: https://github.com/owid/covid-19-data/tree/master/public/data
#
# This means less fetching, but also much higher memory requirements.
# Switch cache timeout to 30 minutes.
#
def fetchGlobal(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Returned cached value, if no more than 30 minutes old
    #
    expires = rconn.hget("country3","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("country3","dataframe"))

    #
    # Fetch
    #
    answer = pd.read_csv(
        "https://covid.ourworldindata.org/data/owid-covid-data.csv",
        parse_dates=["date"]
    ).filter(
        items=("iso_code","location","population","date","new_cases","new_deaths")
    )

    #
    # Cache
    #
    rconn.hset("country3","dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("country3","expires",str(time.time()+1800.0))
    return answer

def fetchStats(rconn):
    blob = fetchGlobal(rconn)
    answer = blob.filter(items=("iso_code","location","population")).drop_duplicates(subset="iso_code")
    return answer.rename(columns={
        'iso_code': 'code',
        'location': 'name'
    })


def fetchCountry(rconn,code="USA"):
    blob = fetchGlobal(rconn)

    answer = blob[blob.iso_code==code].filter(items=("date","new_cases","new_deaths"))
    answer = answer.rename(columns={"new_cases": "cases", "new_deaths": "deaths"})

    return answer.sort_values(by="date")

def menu():
    blob = fetchGlobal(connect())
    answer = blob.filter(items=("iso_code","location")).drop_duplicates(subset="iso_code")

    return {
        'abbrev': dict(zip(answer.iso_code,answer.location)),
        'default': "USA"
    }


def plot(code):
    r = connect()
    ckey = fetchStats(r)
    dt = fetchCountry(r,code)

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


def countries(codes):
    r = connect()
    ckey = fetchStats(r)

    def make_one(code):
        dt = fetchCountry(r,code)
        ckey0 = ckey[ckey.code == code]
        dt['croll'] = dt.cases.rolling(window=7).mean()*100000/ckey0.population.mean()
        dt['droll'] = dt.deaths.rolling(window=7).mean()*100000/ckey0.population.mean()
        dt = dt[dt.date >= pd.to_datetime(date(2020,3,1))]
        dt['Country'] = ckey0.name.iat[0]
        return dt

    dt = pd.concat([make_one(c) for c in codes])

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
    return countries(['SWE','NOR','DNK'])

def north_america():
    return countries(['USA','CAN','MEX'])

def europe():
    return countries(['GBR','FRA','ESP','DEU'])