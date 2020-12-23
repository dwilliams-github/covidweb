import altair as alt
import pandas as pd
import numpy as np
from datetime import date
from os import path
from flask import current_app as app
import redis, requests, time, pyarrow

def connect():
    return redis.Redis( host=app.config['REDIS_HOST'], port=app.config['REDIS_PORT'] )

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

def california_county_populations(rconn):
    context = pyarrow.default_serialization_context()

    if rconn.hexists("county","capop"):
        return context.deserialize(rconn.hget("county","capop"))

    # from https://www.california-demographics.com/counties_by_population
    raw = """
1	Los Angeles County	10,098,052
2	San Diego County	3,302,833
3	Orange County	3,164,182
4	Riverside County	2,383,286
5	San Bernardino County	2,135,413
6	Santa Clara County	1,922,200
7	Alameda County	1,643,700
8	Sacramento County	1,510,023
9	Contra Costa County	1,133,247
10	Fresno County	978,130
11	Kern County	883,053
12	San Francisco County	870,044
13	Ventura County	848,112
14	San Mateo County	765,935
15	San Joaquin County	732,212
16	Stanislaus County	539,301
17	Sonoma County	501,317
18	Tulare County	460,477
19	Santa Barbara County	443,738
20	Solano County	438,530
21	Monterey County	433,212
22	Placer County	380,077
23	San Luis Obispo County	281,455
24	Santa Cruz County	273,765
25	Merced County	269,075
26	Marin County	260,295
27	Butte County	227,075
28	Yolo County	214,977
29	El Dorado County	186,661
30	Imperial County	180,216
31	Shasta County	179,085
32	Madera County	155,013
33	Kings County	150,075
34	Napa County	140,530
35	Humboldt County	135,768
36	Nevada County	99,092
37	Sutter County	95,872
38	Mendocino County	87,422
39	Yuba County	75,493
40	Lake County	64,148
41	Tehama County	63,373
42	San Benito County	59,416
43	Tuolumne County	53,932
44	Calaveras County	45,235
45	Siskiyou County	43,540
46	Amador County	37,829
47	Lassen County	31,185
48	Glenn County	27,897
49	Del Norte County	27,424
50	Colusa County	21,464
51	Plumas County	18,699
52	Inyo County	18,085
53	Mariposa County	17,540
54	Mono County	14,174
55	Trinity County	12,862
56	Modoc County	8,938
57	Sierra County	2,930
58	Alpine County	1,146
"""

    import io
    raw = raw.replace(" County","").replace(",","")
    ca_pop = pd.read_csv(io.StringIO(raw),sep="\t",header=None,names=("rank","county","pop"))

    rconn.hset("county","capop",context.serialize(ca_pop).to_buffer().to_pybytes())
    return ca_pop


def menu():
    r = connect()
    counties = fetchNames(r)
    names = counties.apply(lambda r: '{}, {}'.format(r.county,r.state), axis=1)

    return {
        'names': names.sort_values(),
        'default': "Santa Clara, California"
    }

#
# We'll allow the axis to float a little negative, but
# not too far, otherwise it can get ugly. Fall down to the next unit 
# value, for small statistics.
#
def not_too_negative(quantity,control):
    return [
        max(quantity.min(),-np.ceil(0.05*quantity.max())), 
        min(quantity.max(),1.5*control.max())
    ]

def simple_plot(code):
    r = connect()

    parts = code.split(", ")
    fc = fetchCounty( r, parts[1], parts[0] )

    fc['proll'] = fc.dcases.rolling(window=7).mean()
    fc['froll'] = fc.ddeaths.rolling(window=7).mean()

    fc = fc[fc.dt >= pd.to_datetime(date(2020,3,1))].filter(
        items=("dt","dcases","ddeaths","proll","froll")
    )

    fc['src1'] = "Daily"
    fc['src2'] = "7 day"

    chart = alt.Chart(fc)

    fake_scale = alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

    case_points = chart.mark_line(point=True, clip=True).encode(
        x = alt.X("dt:T",title="Date"),
        y = alt.Y(
            "dcases:Q",
            title = "Cases",
            scale = alt.Scale(domain=not_too_negative(fc.dcases,fc.proll))
        ),
        color = alt.Color("src1", scale=fake_scale)
    )
    case_average = chart.mark_line(clip=True).encode(
        x = alt.X('dt:T'),
        y = alt.Y('proll:Q'),
        color = alt.Color("src2", scale=fake_scale)
    )

    death_points = chart.mark_line(
        point = {"color": "lightgrey"}, 
        color = "lightgrey",
        clip = True
    ).encode(
        x = alt.X("dt:T", title="Date"),
        y = alt.Y(
            "ddeaths:Q",
            title = "Fatalities",
            scale = alt.Scale(domain=not_too_negative(fc.ddeaths,fc.froll))
        ),
    )
    death_average = chart.mark_line().encode(
        x = alt.X('dt:T'),
        y = alt.Y('froll:Q')
    )

    top = (case_points + case_average).properties(
        width = 500, 
        height = 200,
        title = code
    )
    bot = (death_points + death_average).properties(
        width = 500, 
        height = 200
    )

    return (top & bot).configure_legend(title=None).to_dict()

def both():
    r = connect()

    dt_start = pd.to_datetime(date(2020,3,1))

    def fetchHere( r, state, county ):
        answer = fetchCounty( r, state, county )
        answer['droll'] = answer.dcases.rolling(window=7).mean()
        return answer[answer.dt >= dt_start].filter(items=("dt","dcases","droll","county"))

    sa = fetchHere( r, "Oregon", "Marion" )
    cc = fetchHere( r, "Massachusetts", "Barnstable" )
    pl = fetchHere( r, "Oregon", "Multnomah" )
    ha = fetchHere( r, "Texas", "Harris" )

    dt_top = pd.concat((sa,cc,pl))

    selection = alt.selection_multi(fields=['county'], bind='legend', empty='none')
    scale = alt.Scale(domain=[dt_start,sa.dt.max()])

    top_points = alt.Chart(dt_top).mark_line(point=True, clip=True).encode(
        x = alt.X("dt:T", title="Date", scale=scale),
        y = alt.Y(
            "dcases:Q", 
            title = "Daily cases, 7 day rolling average",
            scale = alt.Scale(domain=not_too_negative(dt_top.dcases,dt_top.droll))
        ),
        color = alt.Color("county:N"),
        opacity = alt.condition(selection, alt.value(1), alt.value(0))
    )

    top_lines = alt.Chart(dt_top).mark_line(clip=True).encode(
        x = alt.X("dt:T", title="Date", scale=scale),
        y = alt.Y("droll:Q"),
        color = alt.Color("county:N")
    )
    
    top = (top_points + top_lines).properties(width=500, height=200)

    bot_points = alt.Chart(ha).mark_line(point=True, clip=True).encode(
        x = alt.X("dt:T", title="Date", scale=scale),
        y = alt.Y(
            "dcases:Q", 
            title = "Daily cases, 7 day rolling average",
            scale = alt.Scale(domain=not_too_negative(ha.dcases,ha.droll))
        ),
        color = alt.Color("county:N"),
        opacity = alt.condition(selection, alt.value(1), alt.value(0))
    )

    bot_lines = alt.Chart(ha).mark_line(clip=True).encode(
        x = alt.X("dt:T", title="Date", scale=scale),
        y = alt.Y("droll:Q"),
        color = alt.Color("county:N")
    )
    
    bot = (bot_points + bot_lines).properties(width=500, height=200)

    return (top & bot).add_selection(selection).configure_legend(title=None).to_dict()


def silicon_valley():
    r = connect()

    dt_start = pd.to_datetime(date(2020,3,1))

    def fetchHere( r, state, county ):
        answer = fetchCounty( r, state, county )
        answer['droll'] = answer.dcases.rolling(window=7).mean()
        return answer[answer.dt >= dt_start].filter(items=("dt","dcases","droll","county"))

    dt = pd.concat((
        fetchHere( r, "California", "Santa Clara" ),
        fetchHere( r, "California", "Alameda" )
    ))
    chart = alt.Chart(dt)

    selection = alt.selection_multi(fields=['county'], bind='legend', empty='none')
    scale = alt.Scale(domain=[dt_start,dt.dt.max()])

    points = chart.mark_line(point=True, clip=True).encode(
        x = alt.X("dt:T", title="Date", scale=scale),
        y = alt.X(
            "dcases:Q", 
            title = "Daily cases, 7 day rolling average",
            scale = alt.Scale(domain=not_too_negative(dt.dcases,dt.droll))
        ),
        color = alt.Color("county:N"),
        opacity = alt.condition(selection, alt.value(1), alt.value(0))
    )

    lines = chart.mark_line(clip=True).encode(
        x = alt.X("dt:T", title="Date", scale=scale),
        y = alt.X("droll:Q"),
        color = alt.Color("county:N")
    )
    
    plot = (points+lines).properties(width=500, height=300)

    return plot.add_selection(selection).configure_legend(title=None).to_dict()

def california_bar(percapital=False):
    r = connect()
    dt = fetchData(r)
    dtca = dt[dt.state=="California"].groupby("county").apply(
        lambda a: max(a.sort_values(by="dt").cases.diff()[-7:].sum(),0)
    ).reset_index()
    dtca = dtca.rename(columns={0:"case7"})

    if percapital:
        pop = california_county_populations(r)
        dtca = dtca.merge(pop,on="county")
        dtca['cf7'] = 100000*dtca.case7/dtca['pop']
        answer = alt.Chart(dtca).mark_bar().encode(
            x = alt.X('cf7:Q', title="New cases per 100,000, last 7 days"),
            y = alt.Y('county:N', title="County")
        )
    else:
        answer = alt.Chart(dtca).mark_bar().encode(
            x = alt.X('case7:Q', title="New cases, last 7 days"),
            y = alt.Y('county:N', title="County")
        )

    return answer.properties(width=300, height=600).to_dict()
