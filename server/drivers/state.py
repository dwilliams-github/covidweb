import altair as alt
import pandas as pd
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
    expires = rconn.hget("state","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("state","dataframe"))

    #
    # Fetch new copy
    #
    dt = pd.read_csv("https://covidtracking.com/api/v1/states/daily.csv")
    dt['dt'] = pd.to_datetime(dt.date.apply(lambda x: date(x//10000,(x%10000)//100,x%100)))

    #
    # Save
    #
    rconn.hset("state","dataframe",context.serialize(dt).to_buffer().to_pybytes())
    rconn.hset("state","expires",str(time.time()+600.0))
    return dt


def fetchPopulation(rconn):
    context = pyarrow.default_serialization_context()
    if rconn.hexists("state","population"):
        return context.deserialize(rconn.hget("state","population"))

    #
    # Create for first time
    #
    popfile = path.join(app.config['DATA_DIR'],"pop-est2019.csv")
    namefile = path.join(app.config['DATA_DIR'],"state-abbre.csv")

    pop = pd.read_csv(popfile).merge(pd.read_csv(namefile).rename(columns={'State':'NAME'}),on="NAME")
    pop = pop.filter(items=("NAME","Code","POPESTIMATE2010")).rename(columns={"Code":"state"})

    rconn.hset("state","population",context.serialize(pop).to_buffer().to_pybytes())
    return pop


def menu():
    r = connect()
    pop = fetchPopulation(r)

    return {
        'abbrev': dict(zip(pop.state,pop.NAME)),
        'default': "CA"
    }


def fetchState(rconn,key):
    dt = fetchData(rconn)
    return dt[dt.state==key].sort_values(by="dt")


def plot(code,mode):
    r = connect()
    dt = fetchState(r,code)
    pop = fetchPopulation(r)

    dt = dt[dt.dt >= pd.to_datetime(date(2020,3,1))]

    #
    # This are fake, so we can make a legend
    #
    dt['src1'] = "Daily"
    dt['src2'] = "7 day"

    title = pop[pop.state==code].NAME.to_string(index=False)

    case_items = ("dt","positiveIncrease","croll","src1","src2")

    def case_plot(chart):
        fake_scale = alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

        case_points = chart.mark_line(point=True).encode(
            x = alt.X("dt:T",title="Date"),
            y = alt.Y("positiveIncrease:Q",title="Cases"),
            color = alt.Color("src1", scale=fake_scale)
        )
        case_average = chart.mark_line().encode(
            x = alt.X('dt:T'),
            y = alt.Y('croll:Q'),
            color = alt.Color("src2", scale=fake_scale)
        )
        return (case_points + case_average).properties(width=500, height=200, title=title)

    if mode == 'T':
        dt['troll'] = dt.totalTestResultsIncrease.rolling(window=7).mean()
        dt['fpos'] = dt.positiveIncrease/dt.totalTestResultsIncrease
        #
        # Notice that our 7 day average for positive rate is calculated using
        # the denominator and nominator individually averaged. Otherwise we'll
        # see odd fluctuations due to daily rates.
        #
        dt['froll'] = dt.positiveIncrease.rolling(window=7).mean()/dt.totalTestResultsIncrease.rolling(window=7).mean()
        chart = alt.Chart(dt.filter(
            items = case_items + ("totalTestResultsIncrease","troll","fpos","froll")
        ))

        fake_scale = alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

        case_points = chart.mark_line(point=True,clip=True).encode(
            x = alt.X("dt:T",title="Date"),
            y = alt.Y(
                "totalTestResultsIncrease:Q",
                title = "Tests",
                scale = alt.Scale(domain=[0,dt.totalTestResultsIncrease.max()])
            ),
            color = alt.Color("src1", scale=fake_scale)
        )
        case_average = chart.mark_line(clip=True).encode(
            x = alt.X('dt:T'),
            y = alt.Y('troll:Q'),
            color = alt.Color("src2", scale=fake_scale)
        )
        top = (case_points + case_average).properties(width=500, height=200, title=title)

        #
        # Extend scale if 7 day max is above 0.4 within last two weeks
        #
        max_positive = max(dt.froll.tail(14).max(),0.4)

        posit_points = chart.mark_line(
            point = {"color": "lightgrey"}, 
            color = "lightgrey",
            clip = True
        ).encode(
            x = alt.X("dt:T", title="Date"),
            y = alt.Y("fpos:Q",title="Fraction positive",scale=alt.Scale(domain=[0,max_positive]))
        )
        posit_average = chart.mark_line(clip=True).encode(
            x = alt.X('dt:T'),
            y = alt.Y('froll:Q')
        )

        bot = (posit_points + posit_average).properties(width=500, height=200)

    elif mode == 'H':
        dt['croll'] = dt.positiveIncrease.rolling(window=7).mean()
        dt['hroll'] = dt.hospitalizedCurrently.rolling(window=7).mean()
        chart = alt.Chart(dt.filter(
            items = case_items + ("hospitalizedCurrently","hroll")
        ))
        top = case_plot(chart)

        hospital_points = chart.mark_line(
            point = {"color": "lightgrey"}, 
            color = "lightgrey",
            clip = True
        ).encode(
            x = alt.X("dt:T", title="Date"),
            y = alt.Y(
                "hospitalizedCurrently:Q",
                title = "Hospitalizations",
                scale = alt.Scale(domain=[0,dt.hospitalizedCurrently.max()])
            )
        )
        hospital_average = chart.mark_line(clip=True).encode(
            x = alt.X('dt:T'),
            y = alt.Y('hroll:Q')
        )

        bot = (hospital_points + hospital_average).properties(width=500, height=200)

    else:
        dt['croll'] = dt.positiveIncrease.rolling(window=7).mean()
        dt['droll'] = dt.deathIncrease.rolling(window=7).mean()
        chart = alt.Chart(dt.filter(
            items = case_items + ("deathIncrease","droll")
        ))
        top = case_plot(chart)

        death_points = chart.mark_line(
            point = {"color": "lightgrey"}, 
            color = "lightgrey",
            clip = True
        ).encode(
            x = alt.X("dt:T", title="Date"),
            y = alt.Y(
                "deathIncrease:Q",
                title = "Fatalities",
                scale = alt.Scale(domain=[0,dt.deathIncrease.max()])
            )
        )
        death_average = chart.mark_line(clip=True).encode(
            x = alt.X('dt:T'),
            y = alt.Y('droll:Q')
        )

        bot = (death_points + death_average).properties(width=500, height=200)

    return (top & bot).configure_legend(title=None).to_dict()


def top_four_cases():
    r = connect()
    dt = fetchData(r)
    
    #
    # Reduce dataframe to four worst states, by today's positiveIncrease
    #
    worst = dt.loc[:,("state","positiveIncrease")].groupby("state").first().sort_values(("positiveIncrease"),ascending=False)
    dtds = dt[dt.state.isin(worst.index[0:4])]

    #
    # Use groupby to produce a rolling average per state
    #
    dtds = dtds[dtds.dt>=pd.to_datetime(date(2002,3,1))]
    dtds = dtds.filter(items=("state","dt","positiveIncrease")).sort_values(by="dt")

    roll = dtds.groupby("state").apply(lambda r: r.positiveIncrease.rolling(window=7).mean())
    dtds['roll'] = roll.droplevel(0)

    chart = alt.Chart(dtds)

    selection = alt.selection_multi(fields=['state'], bind='legend')

    top = chart.mark_line(point=True, clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'positiveIncrease:Q',
            title = "Cases",
            scale = alt.Scale(domain=[0,dtds.positiveIncrease.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(width=500, height=200, title="Top states in new cases")

    bottom = chart.mark_line(clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Cases, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.roll.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(width=500, height=200)

    return (top & bottom).add_selection(selection).to_dict()


def top_four_cases_capita():
    r = connect()
    dt = fetchData(r)
    pop = fetchPopulation(r)

    dt = dt.merge(pop,on="state")
    dt['cases'] = 100000*dt.positiveIncrease/dt.POPESTIMATE2010
    
    #
    # Reduce dataframe to four worst states, by today's positiveIncrease
    #
    worst = dt.loc[:,("state","cases")].groupby("state").first().sort_values(("cases"),ascending=False)
    dtds = dt[dt.state.isin(worst.index[0:4])]

    #
    # Use groupby to produce a rolling average per state
    #
    dtds = dtds[dtds.dt>=pd.to_datetime(date(2002,3,1))]
    dtds = dtds.filter(items=("state","dt","cases")).sort_values(by="dt")

    roll = dtds.groupby("state").apply(lambda r: r.cases.rolling(window=7).mean())
    dtds['roll'] = roll.droplevel(0)

    chart = alt.Chart(dtds)

    selection = alt.selection_multi(fields=['state'], bind='legend')

    top = chart.mark_line(point=True, clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'cases:Q',
            title = "Cases per 100,000",
            scale = alt.Scale(domain=[0,dtds.cases.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(width=500, height=200, title="Top states in new cases per capita")

    bottom = chart.mark_line(clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Cases per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.roll.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(width=500, height=200)

    return (top & bottom).add_selection(selection).to_dict()


def top_five_fatalities():
    r = connect()
    dt = fetchData(r)
    
    #
    # Rank by rolling average. This requires a little pandas trickery.
    #
    ranking = dt.loc[:,("dt","state","deathIncrease")].groupby("state").apply(
        lambda s: s.sort_values("dt").deathIncrease.rolling(window=7).mean().tail(1)
    )
    worst = ranking.sort_values(ascending=False).reset_index().state[0:5]
    dtds = dt[dt.state.isin(worst)]

    #
    # Use groupby to produce a rolling average per state
    #
    dtds = dtds[dtds.dt>=pd.to_datetime(date(2002,3,1))]
    dtds = dtds.filter(items=("state","dt","deathIncrease")).sort_values(by="dt")

    roll = dtds.groupby("state").apply(lambda r: r.deathIncrease.rolling(window=7).mean())
    dtds['roll'] = roll.droplevel(0)

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Daily fatalities, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.roll.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300,
        title="Top states in 7 day fatalities"
    ).add_selection(selection).to_dict()


def top_five_fatalities_capita():
    r = connect()
    dt = fetchData(r)
    pop = fetchPopulation(r)

    dt = dt.merge(pop,on="state")
    dt['ndeath'] = 100000*dt.deathIncrease/dt.POPESTIMATE2010
    
    #
    # Rank by rolling average. This requires a little pandas trickery.
    #
    ranking = dt.loc[:,("dt","state","ndeath")].groupby("state").apply(
        lambda s: s.sort_values("dt").ndeath.rolling(window=7).mean().tail(1)
    )
    worst = ranking.sort_values(ascending=False).reset_index().state[0:5]
    dtds = dt[dt.state.isin(worst)]

    #
    # Use groupby to produce a rolling average per state
    #
    dtds = dtds[dtds.dt>=pd.to_datetime(date(2002,3,1))]
    dtds = dtds.filter(items=("state","dt","ndeath")).sort_values(by="dt")

    roll = dtds.groupby("state").apply(lambda r: r.ndeath.rolling(window=7).mean())
    dtds['roll'] = roll.droplevel(0)

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Fatalities per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.roll.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300,
        title="Top states in 7 day fatalities per capita"
    ).add_selection(selection).to_dict()


def big_four_cases_capita():
    r = connect()
    dt = fetchData(r)
    pop = fetchPopulation(r)

    dt = dt.merge(pop,on="state")
    dt['cases'] = 100000*dt.positiveIncrease/dt.POPESTIMATE2010

    dtds = dt[dt.state.isin(["TX","CA","NY","FL"])].filter(items=("dt","state","cases"))
    dtds = dtds[dtds.dt >= pd.to_datetime(date(2002,3,1))]
    roll = dtds.groupby("state").apply(lambda r: r.cases.rolling(window=7).mean())
    dtds['roll'] = roll.droplevel(0)

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T', title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Cases per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.roll.max()])
        ),
        color = alt.Color('state:N'),
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300
    ).add_selection(selection).to_dict()


def big_four_cases():
    r = connect()
    dt = fetchData(r)

    dtds = dt[dt.state.isin(["TX","CA","NY","FL"])].filter(items=("dt","state","positiveIncrease"))
    dtds = dtds[dtds.dt >= pd.to_datetime(date(2002,3,1))]
    roll = dtds.groupby("state").apply(lambda r: r.positiveIncrease.rolling(window=7).mean())
    dtds['roll'] = roll.droplevel(0)

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T', title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Cases, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.roll.max()])
        ),
        color = alt.Color('state:N'),
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300
    ).add_selection(selection).to_dict()




def big_four_fatalities():
    r = connect()
    dt = fetchData(r)
    pop = fetchPopulation(r)

    dtds = dt[dt.state.isin(["TX","CA","NY","FL"])]
    dtds = dtds[dtds.dt >= pd.to_datetime(date(2002,3,1))]

    dtds = dtds.merge(pop,on="state")
    roll = dtds.groupby("state").apply(lambda r: r.deathIncrease.rolling(window=7).mean())
    dtds['ndeath'] = 100000*roll.droplevel(0)/dtds.POPESTIMATE2010
    dtds = dtds.filter(items=("dt","state","ndeath"))

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T', title="Date"),
        y = alt.Y(
            'ndeath:Q',
            title = "Fatalities per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.ndeath.max()])
        ),
        color = alt.Color('state:N'),
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300
    ).add_selection(selection).to_dict()


def death_bar():
    r = connect()
    dt = fetchData(r)
    pop = fetchPopulation(r)

    latest = dt.groupby("state").apply(lambda r: r.sort_values(by="dt").deathIncrease.rolling(7).mean().tail(1))
    latest = latest.reset_index()
    latest = latest.merge(pop,on="state")

    latest['dper'] = 100000*latest.deathIncrease/latest.POPESTIMATE2010

    chart = alt.Chart(latest)

    #
    # We won't force the axis to start at zero, since a bar of zero
    # wouldn't be distinguishable from an (accidental) negative value
    #
    top = chart.mark_bar().encode(
        x = alt.X("state:N",title="State"),
        y = alt.Y("deathIncrease:Q",title="Fatalities")
    ).properties(
        width = 600,
        height = 200,
        title = "7 day average {}".format(date.today())
    )

    bottom = chart.mark_bar().encode(
        x = alt.X("state:N",title="State"),
        y = alt.Y("dper:Q",title="Fatalities / 100,000 population")
    ).properties(
        width = 600,
        height = 200
    )

    return (top & bottom).to_dict()
