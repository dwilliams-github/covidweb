import altair as alt
import pandas as pd
from datetime import date
from os import path
from flask import current_app as app
import redis, requests, time, pyarrow


def fetchGlobal(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Returned cached value, if no more than 10 minutes old
    #
    expires = rconn.hget("country","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("country","dataframe"))

    #
    # Convert from json
    #
    country_key = requests.get("https://corona-api.com/countries").json()

    answer = pd.DataFrame({
        'name': [a['name'] for a in country_key['data']],
        'code': [a['code'] for a in country_key['data']],
        'population': [a['population'] for a in country_key['data']]
    })

    #
    # Cache
    #
    rconn.hset("country","dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("country","expires",str(time.time()+600.0))
    return answer


def fetchCountry(rconn,code="US"):
    context = pyarrow.default_serialization_context()

    expires = rconn.hget("country",code+"_expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("country",code))

    usdata = requests.get("http://corona-api.com/countries/"+code).json()['data']

    answer = pd.DataFrame({
        'dt': pd.to_datetime([date(int(a['date'][0:4]),int(a['date'][5:7]),int(a['date'][8:10])) for a in usdata['timeline']]),
        'cases': [a['new_confirmed'] for a in usdata['timeline']],
        'deaths': [a['new_deaths'] for a in usdata['timeline']],
        'recovered': [a['new_recovered'] for a in usdata['timeline']],
        'prog': ['is_in_progress' in a for a in usdata['timeline']]
    })
    if len(answer): answer = answer[~answer.prog].sort_values(by="dt")

    rconn.hset("country",code,context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("country",code+"_expires",str(time.time()+600.0))
    return answer


def codes():
    r = redis.Redis()
    ckey = fetchGlobal(r)

    return {
        'abbrev': dict(zip(ckey.code,ckey.name)),
        'default': "US"
    }

def menu():
    r = redis.Redis()
    ckey = fetchGlobal(r)
    ckey.sort_values(by="name")
    return {
        'abbrev': dict(zip(ckey.code,ckey.name)),
        'default': "US"
    }

def plot(code):
    r = redis.Redis()
    dt = fetchCountry(r,code)
    ckey = fetchGlobal(r)
    dt['croll'] = dt.cases.rolling(window=7).mean()
    dt['droll'] = dt.deaths.rolling(window=7).mean()
    dt = dt[dt.dt >= pd.to_datetime(date(2020,3,1))]

    #
    # This are fake, so we can make a legend
    #
    dt['src1'] = "Daily"
    dt['src2'] = "7 day"

    chart = alt.Chart(dt)

    fake_scale = scale=alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

    case_points = chart.mark_line(point=True).encode(
        x = alt.X("dt:T",title="Date"),
        y = alt.Y("cases:Q",title="Cases"),
        color = alt.Color("src1", scale=scale)
    )
    case_average = chart.mark_line().encode(
        x = alt.X('dt:T'),
        y = alt.Y('croll:Q'),
        color = alt.Color("src2", scale=scale)
    )

    death_points = chart.mark_line(
        point={"color": "lightgrey"}, 
        color="lightgrey"
    ).encode(
        x = alt.X("dt:T", title="Date"),
        y = alt.Y("deaths:Q",title="Fatalities")
    )
    death_average = chart.mark_line().encode(
        x = alt.X('dt:T'),
        y = alt.Y('droll:Q')
    )

    top = (case_points + case_average).properties(width=500, height=200)
    bot = (death_points + death_average).properties(width=500, height=200)

    return (top & bot).properties(
        title=ckey[ckey.code==code].name.to_string(index=False)
    ).configure_legend(title=None).to_dict()