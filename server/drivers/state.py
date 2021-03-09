import altair as alt
import pandas as pd
from datetime import date, timedelta
from os import path
from io import StringIO
from flask import current_app as app
import redis, requests, time, pyarrow

def connect():
    return redis.Redis( host=app.config['REDIS_HOST'], port=app.config['REDIS_PORT'] )


def fetchState(rconn,key):
    context = pyarrow.default_serialization_context()

    #
    # Check date of main dataframe
    #
    expires = rconn.hget("state"+key,"expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("state"+key,"dataframe"))

    #
    # Fetch
    # Make sure we include a user agent. We are limited to 50,000 records per query,
    # but that should be plenty for this table (which has rows per day)
    #
    req = requests.get(
        "https://data.cdc.gov/resource/9mfq-cb36.csv",
        params={
            'state': key,
            '$limit': 5000, 
            '$select': "submission_date,state,new_case,new_death",
            "$$app_token": app.config['SOCRATA_TOKEN']
        },
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    )

    answer = pd.read_csv(StringIO(req.text), parse_dates=["submission_date"]).rename(columns={
        'submission_date': 'dt'
    })

    answer = answer.sort_values('dt')

    #
    # Save
    #
    rconn.hset("state"+key,"dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("state"+key,"expires",str(time.time()+600.0))

    return answer


def fetchRecent(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Check date of main dataframe
    #
    expires = rconn.hget("staterecent","expires")
    #if expires and time.time() < float(expires):
    #    return context.deserialize(rconn.hget("staterecent","dataframe"))

    #
    # Fetch
    # Make sure we include a user agent. We are limited to 50,000 records per query,
    # but that should be plenty for this table (which has rows per day)
    #
    # Fetch starting from 10 days ago, to ensure we get at least seven
    #
    start = date.today() - timedelta(days=11)

    req = requests.get(
        "https://data.cdc.gov/resource/9mfq-cb36.csv",
        params={
            '$where': "submission_date > '{:4d}-{:02d}-{:02d}'".format(start.year,start.month,start.day),
            '$limit': 5000, 
            '$select': "submission_date,state,new_case,new_death",
            "$$app_token": app.config['SOCRATA_TOKEN']
        },
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    )

    answer = pd.read_csv(StringIO(req.text), parse_dates=["submission_date"]).rename(columns={
        'submission_date': 'dt'
    })

    #
    # We actually get some odd "states". Let's remove them.
    #
    namefile = path.join(app.config['DATA_DIR'],"state-abbre.csv")
    valid = pd.read_csv(namefile)
    answer = answer[answer.state.isin(valid.Code)]

    #
    # Sort
    #
    answer = answer.sort_values('dt')

    #
    # Save
    #
    rconn.hset("staterecent","dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("staterecent","expires",str(time.time()+600.0))

    return answer


def fetchVaccine(rconn):
    context = pyarrow.default_serialization_context()

    #
    # Check date of main dataframe
    #
    expires = rconn.hget("statevac","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("statevac","dataframe"))

    #
    # Fetch new copy
    #
    url = "https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/us_data/time_series/vaccine_data_us_timeline.csv"
    dt = pd.read_csv(url, parse_dates=["Date"])
    dt = dt[dt.Vaccine_Type == 'All']
    dt = dt.filter(items=("Province_State","Date","Doses_alloc","Doses_shipped","Doses_admin","Stage_Two_Doses"))
    dt = dt.sort_values(by="Date")

    #
    # Save
    #
    rconn.hset("statevac","dataframe",context.serialize(dt).to_buffer().to_pybytes())
    rconn.hset("statevac","expires",str(time.time()+600.0))
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


def plot(code):
    r = connect()
    dt = fetchState(r,code)
    pop = fetchPopulation(r)

    dt = dt[dt.dt >= pd.to_datetime(date(2020,3,1))]

    #
    # This are fake, so we can make a legend
    #
    dt['src1'] = "Daily"
    dt['src2'] = "7 day"

    title = pop[pop.state==code].NAME.to_string(index=False).strip()

    case_items = ("dt","new_case","croll","src1","src2")

    #
    # Here we use a scale from 0 to min(daily.max(),rolling.max()*1.5),
    # in order to protect from wild daily corrections
    #

    def case_plot(chart):
        fake_scale = alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

        case_points = chart.mark_line(point=True,clip=True).encode(
            x = alt.X("dt:T",title="Date"),
            y = alt.Y(
                "new_case:Q",
                title="Cases",
                scale = alt.Scale(domain=[0,min(dt.new_case.max(),dt.croll.max()*1.5)])
            ),
            color = alt.Color("src1", scale=fake_scale)
        )
        case_average = chart.mark_line(clip=True).encode(
            x = alt.X('dt:T'),
            y = alt.Y('croll:Q'),
            color = alt.Color("src2", scale=fake_scale)
        )
        return (case_points + case_average).properties(width=500, height=200, title=title)

    dt['croll'] = dt.new_case.rolling(window=7).mean()
    dt['droll'] = dt.new_death.rolling(window=7).mean()
    chart = alt.Chart(dt.filter(
        items = case_items + ("new_death","droll")
    ))
    top = case_plot(chart)

    death_points = chart.mark_line(
        point = {"color": "lightgrey"}, 
        color = "lightgrey",
        clip = True
    ).encode(
        x = alt.X("dt:T", title="Date"),
        y = alt.Y(
            "new_death:Q",
            title = "Fatalities",
            scale = alt.Scale(domain=[0,min(dt.new_death.max(),dt.droll.max()*1.5)])
        )
    )
    death_average = chart.mark_line(clip=True).encode(
        x = alt.X('dt:T'),
        y = alt.Y('droll:Q')
    )

    bot = (death_points + death_average).properties(width=500, height=200)

    return (top & bot).configure_legend(title=None).to_dict()


def vaccines(code):
    r = connect()
    pop = fetchPopulation(r)

    title = pop[pop.state==code].NAME.to_string(index=False).strip()
    capita = pop[pop.state==code].POPESTIMATE2010.max()

    dt = fetchVaccine(r)
    dt = dt[dt.Province_State==title]
    dt = dt[dt.Date >= pd.to_datetime(date(2020,12,1))]

    dt['Doses_alloc_cap'] = dt.Doses_alloc/capita
    dt['Doses_shipped_cap'] = dt.Doses_shipped/capita
    dt['Doses_admin_cap'] = dt.Doses_admin/capita

    chart = alt.Chart(dt)

    #
    # We want dual y axes, but with just a scaling factor
    # (so a single plot line).
    #
    # Altair doesn't support this, so instead we can
    # pretend we are plotting independent values, but explicitly 
    # set y axis limits consistently
    #
    # Note: it is important to set nice=False to override padding
    #
    # Be careful not to include a NaN in the scale domain, as
    # this produces bad json objects.
    #
    y_max = dt.Doses_shipped.append(pd.Series([0])).max()*1.05

    scale1 = chart.mark_line().encode(
        x = alt.X("Date:T", title="Date"),
        y = alt.Y("Doses_shipped:Q", 
            title = "Total shipped",
            scale = alt.Scale(domain=[0,y_max], nice=False)
        )
    )
    scale2 = chart.mark_line(opacity=0).encode(
        x = alt.X("Date:T", title="Date"),
        y = alt.Y("Doses_shipped_cap:Q", 
            title = "Total shipped, per capita", 
            axis = alt.Axis(format='%'),
            scale = alt.Scale(domain=[0,y_max/capita], nice=False)
        )
    )

    top = alt.layer(scale1,scale2).resolve_scale(
        y = 'independent'
    ).properties(
        width = 500, 
        height = 200,
        title = title
    )

    y_max = dt.Doses_admin.append(pd.Series([0])).max()*1.05

    scale1 = chart.mark_line().encode(
        x = alt.X("Date:T", title="Date"),
        y = alt.Y("Doses_admin:Q", 
            title = "Total dosed",
            scale = alt.Scale(domain=[0,y_max], nice=False)
        )
    )
    scale2 = chart.mark_line(opacity=0).encode(
        x = alt.X("Date:T", title="Date"),
        y = alt.Y("Doses_admin_cap:Q",
            title="Total dosed, per capita",
            axis=alt.Axis(format='%'),
            scale = alt.Scale(domain=[0,y_max/capita], nice=False)
        )
    )

    bot = alt.layer(scale1,scale2).resolve_scale(
        y = 'independent'
    ).properties(
        width = 500,
        height = 200
    )

    return (top & bot).to_dict()


def top_four_cases():
    r = connect()
    dt = fetchRecent(r)
    
    #
    # Reduce dataframe to four worst states, by most recent 7 day rolling
    #
    ranking = dt.loc[:,("dt","state","new_case")].groupby("state").apply(
        lambda s: s.sort_values("dt").new_case.rolling(window=7).mean().tail(1)
    )
    worst = ranking.sort_values(ascending=False).reset_index().state[0:5]

    #
    # Fetch those states
    #
    def fetchWithRoll(code):
        answer = fetchState(r,code)
        answer['roll'] = answer.new_case.rolling(window=7).mean()
        return answer

    dtds = pd.concat([fetchWithRoll(code) for code in worst])

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Cases, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.roll.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500,
        height=300,
        title="Top states in new cases"
    ).add_selection(selection).to_dict()


def top_four_cases_capita():
    r = connect()
    dt = fetchRecent(r)
    pop = fetchPopulation(r)

    dt = dt.merge(pop,on="state")
    dt['cases'] = 100000*dt.new_case/dt.POPESTIMATE2010
    
    #
    # Reduce dataframe to four worst states, by most recent 7 day rolling
    #
    ranking = dt.loc[:,("dt","state","cases")].groupby("state").apply(
        lambda s: s.sort_values("dt").cases.rolling(window=7).mean().tail(1)
    )
    worst = ranking.sort_values(ascending=False).reset_index().state[0:5]

    #
    # Fetch those states
    #
    def fetchWithRoll(code):
        answer = fetchState(r,code)
        answer['roll'] = answer.new_case.rolling(window=7).mean()
        return answer

    dtds = pd.concat([fetchWithRoll(code) for code in worst])
    dtds = dtds.merge(pop,on="state")
    dtds['rollpc'] = 100000*dtds.roll/dtds.POPESTIMATE2010

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'rollpc:Q',
            title = "Cases per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.rollpc.max()])
        ),
        color = 'state:N',
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300, 
        title="Top states in new cases per capita"
    ).add_selection(selection).to_dict()


def top_five_fatalities():
    r = connect()
    dt = fetchRecent(r)
    
    #
    # Reduce dataframe to four worst states, by most recent 7 day rolling
    #
    ranking = dt.loc[:,("dt","state","new_death")].groupby("state").apply(
        lambda s: s.sort_values("dt").new_death.rolling(window=7).mean().tail(1)
    )
    worst = ranking.sort_values(ascending=False).reset_index().state[0:5]

    #
    # Fetch those states
    #
    def fetchWithRoll(code):
        answer = fetchState(r,code)
        answer['roll'] = answer.new_death.rolling(window=7).mean()
        return answer

    dtds = pd.concat([fetchWithRoll(code) for code in worst])

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
    dt = fetchRecent(r)
    pop = fetchPopulation(r)

    dt = dt.merge(pop,on="state")
    dt['deaths'] = 100000*dt.new_death/dt.POPESTIMATE2010
    
    #
    # Reduce dataframe to four worst states, by most recent 7 day rolling
    #
    ranking = dt.loc[:,("dt","state","deaths")].groupby("state").apply(
        lambda s: s.sort_values("dt").deaths.rolling(window=7).mean().tail(1)
    )
    worst = ranking.sort_values(ascending=False).reset_index().state[0:5]

    #
    # Fetch those states
    #
    def fetchWithRoll(code):
        answer = fetchState(r,code)
        answer['roll'] = answer.new_death.rolling(window=7).mean()
        return answer

    dtds = pd.concat([fetchWithRoll(code) for code in worst])
    dtds = dtds.merge(pop,on="state")
    dtds['rollpc'] = 100000*dtds.roll/dtds.POPESTIMATE2010

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dtds).mark_line(clip=True).encode(
        x = alt.X('dt:T',title="Date"),
        y = alt.Y(
            'rollpc:Q',
            title = "Fatalities per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dtds.rollpc.max()])
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
    pop = fetchPopulation(r)

    def fetchWithRoll(code):
        answer = fetchState(r,code)
        answer['roll'] = answer.new_case.rolling(window=7).mean()
        return answer

    dt = pd.concat([fetchWithRoll(code) for code in ["TX","CA","NY","FL"]])

    dt = dt.merge(pop,on="state")
    dt['rollpc'] = 100000*dt.roll/dt.POPESTIMATE2010

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dt).mark_line(clip=True).encode(
        x = alt.X('dt:T', title="Date"),
        y = alt.Y(
            'rollpc:Q',
            title = "Cases per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dt.rollpc.max()])
        ),
        color = alt.Color('state:N'),
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300
    ).add_selection(selection).to_dict()


def big_four_cases():
    r = connect()

    def fetchWithRoll(code):
        answer = fetchState(r,code)
        answer['roll'] = answer.new_case.rolling(window=7).mean()
        return answer

    dt = pd.concat([fetchWithRoll(code) for code in ["TX","CA","NY","FL"]])

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dt).mark_line(clip=True).encode(
        x = alt.X('dt:T', title="Date"),
        y = alt.Y(
            'roll:Q',
            title = "Cases, 7 day rolling average",
            scale = alt.Scale(domain=[0,dt.roll.max()])
        ),
        color = alt.Color('state:N'),
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300
    ).add_selection(selection).to_dict()


def big_four_fatalities():
    r = connect()
    pop = fetchPopulation(r)

    def fetchWithRoll(code):
        answer = fetchState(r,code)
        answer['roll'] = answer.new_death.rolling(window=7).mean()
        return answer

    dt = pd.concat([fetchWithRoll(code) for code in ["TX","CA","NY","FL"]])

    dt = dt.merge(pop,on="state")
    dt['rollpc'] = 100000*dt.roll/dt.POPESTIMATE2010

    selection = alt.selection_multi(fields=['state'], bind='legend')

    return alt.Chart(dt).mark_line(clip=True).encode(
        x = alt.X('dt:T', title="Date"),
        y = alt.Y(
            'rollpc:Q',
            title = "Fatalities per 100,000, 7 day rolling average",
            scale = alt.Scale(domain=[0,dt.rollpc.max()])
        ),
        color = alt.Color('state:N'),
        opacity = alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=500, 
        height=300
    ).add_selection(selection).to_dict()


def death_bar():
    r = connect()
    dt = fetchRecent(r)
    pop = fetchPopulation(r)

    dt = dt.merge(pop,on="state")
    dt['deaths'] = 100000*dt.new_death/dt.POPESTIMATE2010

    latest = dt.groupby("state").apply(lambda r: r.sort_values(by="dt").new_death.rolling(7).mean().tail(1))
    latest = latest.reset_index()
    latest = latest.merge(pop,on="state")

    latest['dper'] = 100000*latest.new_death/latest.POPESTIMATE2010

    chart = alt.Chart(latest)

    #
    # We won't force the axis to start at zero, since a bar of zero
    # wouldn't be distinguishable from an (accidental) negative value
    #
    top = chart.mark_bar().encode(
        x = alt.X("state:N",title="State"),
        y = alt.Y("new_death:Q",title="Fatalities")
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
