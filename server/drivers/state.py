import altair as alt
import pandas as pd
import numpy as np
from datetime import date, timedelta
from os import path
from io import StringIO
from flask import current_app as app
import redis, requests, time, pyarrow

def connect():
    return redis.Redis( host=app.config['REDIS_HOST'], port=app.config['REDIS_PORT'] )

#
# A simple label placement algorithm, to avoid overlaps
#
# A conventional approach might use scipy.optimize, but I want
# to avoid dependence on something so big as scipy just for this one
# purpose.
#
class label_placement:
    def __init__(self, xcol, ycol):
        xy = np.column_stack( [xcol.to_numpy(), ycol.to_numpy()] )
        dxy = (xy.max(axis=0) - xy.min(axis=0))/40
        
        r0 = xy/dxy

        #
        # Default starts above mark
        #
        phi = np.array([np.pi/2]*len(r0))

        #
        # Push away from closest neighbor
        #
        i0, i1 = np.triu_indices(r0.shape[0], k=1)
        r = r0 + [0,1]    # = [cos(phi),sin(phi)]

        for attempt in range(0,100):
            #
            # Find worst collision
            #
            id2 = np.argmin(((r[i0,:]-r[i1,:])**2).sum(axis=1))
            t0 = i0[id2]
            t1 = i1[id2]

            #
            # We're finished if the collision isn't too bad
            #
            dr = r[t0,:] - r[t1,:]
            if (dr**2).sum() > 2: break

            #
            # Move each by delta in phi
            #
            if np.cos(phi[t0]) * dr[1] - np.sin(phi[t0]) * dr[0] > 0:
                phi[t0] += np.pi/6
            else:
                phi[t0] -= np.pi/6

            r[t0,:] = r0[t0,:] + [np.cos(phi[t0]), np.sin(phi[t0])]

            if np.cos(phi[t1]) * dr[1] - np.sin(phi[t1]) * dr[0] < 0:
                phi[t1] += np.pi/6
            else:
                phi[t1] -= np.pi/6

            r[t1,:] = r0[t1,:] + [np.cos(phi[t1]), np.sin(phi[t1])]

        self.xy = r * dxy

    def X(self):
        return self.xy[:,0]

    def Y(self):
        return self.xy[:,1]




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

    if req.status_code != 200:
        raise Exception("Requestion failure: {}".format(req.status_code))

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
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("staterecent","dataframe"))

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

    if req.status_code != 200:
        raise Exception("Requestion failure: {}".format(req.status_code))

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


def fetchHospital(rconn,key):
    context = pyarrow.default_serialization_context()

    #
    # See: https://dev.socrata.com/foundry/healthdata.gov/g62h-syeh
    #

    #
    # Check date of main dataframe
    #
    expires = rconn.hget("statehos"+key,"expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("statehos"+key,"dataframe"))

    #
    # Fetch
    # Make sure we include a user agent. We are limited to 50,000 records per query,
    # but that should be plenty for this table (which has rows per day)
    #
    # The HHS sure loves long column names...
    #
    columns = [
        'date',
        'state',
        'inpatient_beds',
        'inpatient_beds_used',
        'inpatient_beds_used_covid',
        'staffed_icu_adult_patients_confirmed_and_suspected_covid',
        'total_staffed_adult_icu_beds',
    ]

    req = requests.get(
        "https://healthdata.gov/resource/g62h-syeh.csv",
        params={
            'state': key,
            '$limit': 5000, 
            '$select': ",".join(columns),
            "$$app_token": app.config['SOCRATA_TOKEN']
        },
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    )

    if req.status_code != 200:
        raise Exception("Requestion failure: {}".format(req.status_code))

    answer = pd.read_csv(StringIO(req.text), parse_dates=["date"]).rename(columns={
        'date': 'dt'
    })

    answer = answer.sort_values('dt')

    #
    # Save
    #
    rconn.hset("statehos"+key,"dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("statehos"+key,"expires",str(time.time()+600.0))

    return answer


def fetchVaccine(rconn,key):
    context = pyarrow.default_serialization_context()

    #
    # See: https://dev.socrata.com/foundry/data.cdc.gov/unsk-b7fc
    #

    #
    # Check date of main dataframe
    #
    expires = rconn.hget("statevac"+key,"expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("statevac"+key,"dataframe"))

    #
    # Fetch
    # Make sure we include a user agent. We are limited to 50,000 records per query,
    # but that should be plenty for this table (which has rows per day)
    #
    columns = [
        'Date',
        'administered_dose1_recip',
        'series_complete_yes'
    ]

    req = requests.get(
        " https://data.cdc.gov/resource/unsk-b7fc.csv",
        params={
            'Location': key,
            '$limit': 5000, 
            '$select': ",".join(columns),
            "$$app_token": app.config['SOCRATA_TOKEN']
        },
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    )

    if req.status_code != 200:
        raise Exception("Requestion failure: {}".format(req.status_code))

    answer = pd.read_csv(StringIO(req.text), parse_dates=["Date"]).rename(columns={
        'Date': 'date',
        'administered_dose1_recip': 'onedose',
        'series_complete_yes': 'complete'
    }).sort_values('date')

    #
    # Get corresponding FIPS code
    #
    fipfile = path.join(app.config['DATA_DIR'],"fips-code.csv")
    fips = pd.read_csv(fipfile,sep="\t")
    fip_code = fips[fips['key']==key].code.values[0]

    #
    # Use this to fetch population by age
    #
    popfile = path.join(app.config['DATA_DIR'],"sc-est2019-agesex-civ.csv")
    pop = pd.read_csv(popfile).filter(items=[
        'STATE', 'SEX', 'AGE', 'POPEST2019_CIV'
    ])
    pop = pop[(pop.STATE == fip_code) & (pop.SEX == 0)]

    #
    # Convert to total population
    #
    pop12 = pop[(pop.AGE >= 12) & (pop.AGE < 900)]['POPEST2019_CIV'].sum()
    pop16 = pop[(pop.AGE >= 16) & (pop.AGE < 900)]['POPEST2019_CIV'].sum()

    #
    # Merge
    #
    answer['eligible'] = np.where(answer['date'] > pd.to_datetime(date(2021,5,10)), pop12, pop16)

    #
    # Save
    #
    rconn.hset("statevac"+key,"dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("statevac"+key,"expires",str(time.time()+600.0))

    return answer




def fetchRecentVaccine(rconn):
    context = pyarrow.default_serialization_context()

    #
    # See: https://dev.socrata.com/foundry/data.cdc.gov/unsk-b7fc
    #

    #
    # Check date of main dataframe
    #
    expires = rconn.hget("staterecvac","expires")
    if expires and time.time() < float(expires):
        return context.deserialize(rconn.hget("staterecvac","dataframe"))

    #
    # Fetch, sorted by date, to get most recent results, and fetch enough
    # to cover all the states
    #
    columns = [
        'Date',
        'Location',
        'administered_dose1_recip',
        'series_complete_yes'
    ]

    req = requests.get(
        "https://data.cdc.gov/resource/unsk-b7fc.csv",
        params={
            '$order': "Date DESC",
            '$limit': 200, 
            '$select': ",".join(columns),
            "$$app_token": app.config['SOCRATA_TOKEN']
        },
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    )

    if req.status_code != 200:
        raise Exception("Requestion failure: {}".format(req.status_code))

    answer = pd.read_csv(StringIO(req.text), parse_dates=["Date"]).rename(columns={
        'Date': 'date',
        'Location': 'key',
        'administered_dose1_recip': 'onedose',
        'series_complete_yes': 'complete'
    })

    #
    # Only keep most recent results
    #
    answer = answer.sort_values(['key','date'], ascending=False).drop_duplicates(subset=["key"])

    #
    # Merge with FIPS
    #
    fipfile = path.join(app.config['DATA_DIR'],"fips-code.csv")
    fips = pd.read_csv(fipfile,sep="\t")
    answer = answer.merge(fips,on="key")

    #
    # Get population > 12 years old
    #
    popfile = path.join(app.config['DATA_DIR'],"sc-est2019-agesex-civ.csv")
    pop = pd.read_csv(popfile).filter(items=[
        'STATE', 'SEX', 'AGE', 'POPEST2019_CIV'
    ])
    pop = pop[(pop.AGE >= 12) & (pop.AGE < 900) & (pop.SEX == 0)].filter(
        items=("STATE","POPEST2019_CIV")
    ).rename(columns={
        'STATE': 'code',
        'POPEST2019_CIV': 'eligible'
    })

    pop = pop.groupby(by="code").sum().reset_index()

    #
    # Merge into answer
    #
    answer = answer.merge(pop,on="code")

    #
    # Save
    #
    rconn.hset("staterecvac","dataframe",context.serialize(answer).to_buffer().to_pybytes())
    rconn.hset("staterecvac","expires",str(time.time()+600.0))

    return answer


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


def fetchPolitics(rconn):
    context = pyarrow.default_serialization_context()
    if rconn.hexists("state","politics"):
        return context.deserialize(rconn.hget("state","politics"))
        
    politicsfile = path.join(app.config['DATA_DIR'],"state-party-affiliation.csv")
    namefile = path.join(app.config['DATA_DIR'],"state-abbre.csv")
    pol = pd.read_csv(politicsfile, sep="\t")
    pol = pol.merge(pd.read_csv(namefile).rename(columns={'State':'state'}),on="state")

    rconn.hset("state","politics",context.serialize(pol).to_buffer().to_pybytes())
    return pol


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


def hospitals(code):
    r = connect()
    dt = fetchState(r,code)
    dth = fetchHospital(r,code)
    pop = fetchPopulation(r)

    dt = dt[dt.dt >= pd.to_datetime(date(2020,3,1))]
    dth = dth[dth.dt >= pd.to_datetime(date(2020,3,1))]

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
    chart = alt.Chart(dt.filter(items = case_items))
    top = case_plot(chart)

    #
    # We'll fold the data for the hospital plot
    #
    dt_1 = dth.filter(items=("dt","staffed_icu_adult_patients_confirmed_and_suspected_covid")).rename(
        columns={"staffed_icu_adult_patients_confirmed_and_suspected_covid":"icu"}
    )
    dt_2 = dth.filter(items=("dt","total_staffed_adult_icu_beds")).rename(
        columns={"total_staffed_adult_icu_beds":"icu"}
    )
    dt_1['label'] = 'COVID-19'
    dt_2['label'] = 'Total'

    chart2 = alt.Chart(pd.concat([dt_1,dt_2])).mark_line().encode(
        x = alt.X("dt:T", title="Date"),
        y = alt.Y(
            "icu:Q",
            title = "Staffed ICU beds"
        ),
        color = alt.Color("label",scale=alt.Scale(
            domain=('Total','COVID-19'), 
            range=('darkblue','darkorange')
        ))
    )

    bot = chart2.properties(width=500, height=200)

    return (top & bot).resolve_scale(color='independent').configure_legend(title=None).to_dict()



def vaccines(code):
    r = connect()
    dt = fetchVaccine(r,code)

    dt['onedose_pop'] = dt['onedose'] / dt['eligible']
    dt['complete_pop'] = dt['complete'] / dt['eligible']

    #
    # Serialize, and also eliminate data rows with zeros, which appear to be
    # placeholders for "no data".
    #
    dt1 = dt[dt['onedose'] > 0].filter(items=['date','onedose_pop']).copy().rename(columns={
        'onedose_pop': 'frac'
    })
    dt1['status'] = 'One dose'

    dt2 = dt[dt['complete'] > 0].filter(items=['date','complete_pop']).copy().rename(columns={
        'complete_pop': 'frac'
    })
    dt2['status'] = 'Fully vaccinated'

    #
    # Title
    #
    pop = fetchPopulation(r)
    title = pop[pop.state==code].NAME.to_string(index=False).strip()

    #
    # Build chart
    #
    chart = alt.Chart(pd.concat([dt1,dt2])).mark_line().encode(
        x = alt.X('date:T', title="Date"),
        y = alt.Y('frac:Q', title="Percent eligible", axis=alt.Axis(format='%')),
        color = alt.Color('status:N')
    )

    return chart.properties(width=500, height=300, title=title).configure_legend(title=None).to_dict()



def vaccines_bar():
    r = connect()
    dt = fetchRecentVaccine(r)

    dt['onedose'] = dt['onedose'] / dt['eligible']
    dt['complete'] = dt['complete'] / dt['eligible']
    reduced = dt.filter(items=[
        'key', 'onedose', 'complete'
    ]).sort_values(by="key")

    datestamp = pd.to_datetime(dt['date'].values[0]).strftime('%D')

    chart = alt.Chart(reduced)

    top = chart.mark_bar().encode(
        x = alt.X("key:N",title="State"),
        y = alt.Y("onedose:Q", title="Percent eligible", axis=alt.Axis(format='%'), scale=alt.Scale(domain=[0,1]))
    ).properties(
        width = 600,
        height = 200,
        title = "At least one dose {}".format(datestamp)
    )

    bottom = chart.mark_bar().encode(
        x = alt.X("key:N",title="State"),
        y = alt.Y("complete:Q", title="Percent eligible", axis=alt.Axis(format='%'), scale=alt.Scale(domain=[0,1]))
    ).properties(
        width = 600,
        height = 200,
        title = "Fully vaccinated {}".format(datestamp)
    )

    return (top & bottom).to_dict()


def vaccines_by_party():
    r = connect()
    dt = fetchRecentVaccine(r)
    pol = fetchPolitics(r).filter(items=("Code","democrat")).rename(columns={"Code":"key"})
    dt = dt.merge(pol,on="key")

    dt['onedose'] = dt['onedose'] / dt['eligible']
    dt['complete'] = dt['complete'] / dt['eligible']
    dt['democrat'] = dt['democrat'] / 100.0
    reduced = dt.filter(items=[
        'key', 'onedose', 'complete', "democrat"
    ]).sort_values(by="key")

    datestamp = pd.to_datetime(dt['date'].values[0]).strftime('%D')

    #
    # Label overlap avoidance
    #
    # This is a feature that is coming to Vega, but hasn't been incorporated
    # yet in altair. Do something simple here.
    #
    labels = label_placement(reduced['democrat'], reduced['onedose'])
    reduced['xlab'] = labels.X()
    reduced['ylab'] = labels.Y()

    #
    # Back to making the chart
    #
    chart = alt.Chart(reduced)

    points = chart.mark_point().encode(
        x = alt.X(
            "democrat:Q",
            title="Percent Democrat/Lean Democrat (2018)", 
            axis=alt.Axis(format='%'),
            scale=alt.Scale(zero=False)
        ),
        y = alt.Y("onedose:Q",
            title="Percent eligible vaccinated",
            axis=alt.Axis(format='%'), 
            scale=alt.Scale(zero=False)
        )
    ).properties(
        width = 500,
        height = 400,
        title = "At least one dose {}".format(datestamp)
    )

    marks = chart.mark_text(
        align = "center",
        baseline = "middle",
        fontSize = 9
    ).encode(
        x = alt.X("xlab:Q"),
        y = alt.Y("ylab:Q"),
        text="key"
    )

    return (points + marks).to_dict()


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
