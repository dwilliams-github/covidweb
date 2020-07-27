import altair as alt
import pandas as pd
from datetime import date
from os import path
from flask import current_app as app
import redis, requests, time, pyarrow

def fetchData(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Check date of main dataframe
    #
    expires = rconn.hget("county","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("county","dataframe"))

    #
    # Fetch new copy
    #
    dt = pd.read_csv("https://github.com/nytimes/covid-19-data/blob/master/us-counties.csv?raw=true")
    dt['dt'] = pd.to_datetime(dt.date,format="%Y-%m-%d")

    #
    # Save
    #
    rconn.hset("county","dataframe",context.serialize(dt).to_buffer().to_pybytes())
    rconn.hset("county","expires",str(time.time()+600.0))
    return dt

def fetchNames(rconn):
    context = pyarrow.default_serialization_context()

    if rconn.hexists("county","names"):
        return context.deserialize(rconn.hget("county","names"))

    dt = fetchData(rconn)
    counties = dt.filter(items=("state","county")).drop_duplicates()

    rconn.hset("county","names",context.serialize(counties).to_buffer().to_pybytes())
    return counties

def fetchCounty(rconn,state,county):
    context = pyarrow.default_serialization_context()

    #
    # Check date of main dataframe
    #
    key = state + ":" + county 
    key_expires = key + ":expires"

    expires = rconn.hget("county", key_expires)
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("county",key))

    #
    # Fetch new master data frame
    #
    dt = fetchData(rconn)

    #
    # Process and save
    #
    answer = dt[(dt.state==state) & (dt.county==county)].copy()
    answer['days'] = (answer.dt-min(answer.dt[answer.cases>0])).dt.days
    answer['days10'] = (answer.dt-min(answer.dt[answer.cases>9])).dt.days
    answer = answer.sort_values(by="days")
    answer['ddeaths'] = answer.deaths.diff()
    answer['dcases'] = answer.cases.diff()

    rconn.hset("county",key,context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("county",key_expires,str(time.time()+600.0))
    return answer


def menu():
    r = redis.Redis()
    counties = fetchNames(r)
    names = counties.apply(lambda r: '{}, {}'.format(r.county,r.state), axis=1)

    return {
        'names': names.sort_values(),
        'default': "Santa Clara, California"
    }

def simple_plot(code):
    r = redis.Redis()

    parts = code.split(", ")
    fc = fetchCounty( r, parts[1], parts[0] )

    fc['proll'] = fc.dcases.rolling(window=7).mean()
    fc['froll'] = fc.ddeaths.rolling(window=7).mean()

    fc = fc[fc.dt >= pd.to_datetime(date(2020,3,1))]

    fc['src1'] = "Daily"
    fc['src2'] = "7 day"

    chart = alt.Chart(fc)

    fake_scale = alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

    case_points = chart.mark_line(point=True).encode(
        x = alt.X("dt:T",title="Date"),
        y = alt.Y("dcases:Q",title="Cases"),
        color = alt.Color("src1", scale=fake_scale)
    )
    case_average = chart.mark_line().encode(
        x = alt.X('dt:T'),
        y = alt.Y('proll:Q'),
        color = alt.Color("src2", scale=fake_scale)
    )

    death_points = chart.mark_line(
        point={"color": "lightgrey"}, 
        color="lightgrey"
    ).encode(
        x = alt.X("dt:T", title="Date"),
        y = alt.Y("ddeaths:Q",title="Fatalities")
    )
    death_average = chart.mark_line().encode(
        x = alt.X('dt:T'),
        y = alt.Y('froll:Q')
    )

    top = (case_points + case_average).properties(
        width=500, 
        height=200,
        title=code
    )
    bot = (death_points + death_average).properties(width=500, height=200)

    return (top & bot).configure_legend(title=None).to_dict()