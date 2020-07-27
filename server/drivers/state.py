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
    r = redis.Redis()
    pop = fetchPopulation(r)

    return {
        'abbrev': dict(zip(pop.state,pop.NAME)),
        'default': "CA"
    }


def fetchCountry(rconn,key):
    dt = fetchData(rconn)
    return dt[dt.state==key].sort_values(by="dt")


def state(ab="TX",title="Texas",dmin=date(2020,3,11)):
    dtds = dt[dt.state==ab].sort_values(by="dt")
    fig,axs = plt.subplots(figsize=(10,9),nrows=2,sharex=True)
    fig.subplots_adjust(hspace=0.05)
    ax = axs[0]
    ax.plot(dtds.dt,dtds.positiveIncrease.rolling(window=7).mean(),"-",label='7 day rolling average')
    ax.plot(dtds.dt,dtds.positiveIncrease,"-o",label='Daily',linewidth=0.5)
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    ax.set_ylabel('Confirmed cases')
    xlo,xhi = ax.get_xlim()
    ax.set_xlim(dmin,xhi)
    ax.set_title("{} {}".format(title,date.today()))
    ax.grid()
    ax.legend()

    ax = axs[1]
    ax.plot(dtds.dt,dtds.deathIncrease.rolling(window=7).mean(),"-",label='7 day rolling average')
    ax.plot(dtds.dt,dtds.deathIncrease,"-o",label='Daily',linewidth=0.5)
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    ax.set_xlabel('Date')
    ax.set_ylabel('Fatalities')
    xlo,xhi = ax.get_xlim()
    ax.set_xlim(dmin,xhi)
    ax.tick_params(axis='x', labelrotation=45)
    ax.grid()




def plot(code,mode):
    r = redis.Redis()
    dt = fetchCountry(r,code)
    pop = fetchPopulation(r)

    dt = dt[dt.dt >= pd.to_datetime(date(2020,3,1))]

    #
    # This are fake, so we can make a legend
    #
    dt['src1'] = "Daily"
    dt['src2'] = "7 day"

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
        return (case_points + case_average).properties(width=500, height=200)

    if mode == 'T':
        dt['troll'] = dt.totalTestResultsIncrease.rolling(window=7).mean()
        dt['fpos'] = dt.positiveIncrease/dt.totalTestResultsIncrease
        #
        # Notice that our 7 day average for positive rate is calculated using
        # the denominator and nominator individually averaged. Otherwise we'll
        # see odd fluctuations due to daily rates.
        #
        dt['froll'] = dt.positiveIncrease.rolling(window=7).mean()/dt.totalTestResultsIncrease.rolling(window=7).mean()
        chart = alt.Chart(dt)

        fake_scale = alt.Scale(domain=('Daily','7 day'), range=('lightgrey','blue'))

        case_points = chart.mark_line(point=True).encode(
            x = alt.X("dt:T",title="Date"),
            y = alt.Y("totalTestResultsIncrease:Q",title="Tests"),
            color = alt.Color("src1", scale=fake_scale)
        )
        case_average = chart.mark_line().encode(
            x = alt.X('dt:T'),
            y = alt.Y('troll:Q'),
            color = alt.Color("src2", scale=fake_scale)
        )
        top = (case_points + case_average).properties(width=500, height=200)

        posit_points = chart.mark_line(
            point={"color": "lightgrey"}, 
            color="lightgrey",
            clip=True
        ).encode(
            x = alt.X("dt:T", title="Date"),
            y = alt.Y("fpos:Q",title="Fraction positive",scale=alt.Scale(domain=[0,0.4]))
        )
        posit_average = chart.mark_line(clip=True).encode(
            x = alt.X('dt:T'),
            y = alt.Y('froll:Q')
        )

        bot = (posit_points + posit_average).properties(width=500, height=200)

    elif mode == 'H':
        dt['croll'] = dt.positiveIncrease.rolling(window=7).mean()
        dt['hroll'] = dt.hospitalizedCurrently.rolling(window=7).mean()
        chart = alt.Chart(dt)
        top = case_plot(chart)

        hospital_points = chart.mark_line(
            point={"color": "lightgrey"}, 
            color="lightgrey"
        ).encode(
            x = alt.X("dt:T", title="Date"),
            y = alt.Y("hospitalizedCurrently:Q",title="Hospitalizations")
        )
        hospital_average = chart.mark_line().encode(
            x = alt.X('dt:T'),
            y = alt.Y('hroll:Q')
        )

        bot = (hospital_points + hospital_average).properties(width=500, height=200)

    else:
        dt['croll'] = dt.positiveIncrease.rolling(window=7).mean()
        dt['droll'] = dt.deathIncrease.rolling(window=7).mean()
        chart = alt.Chart(dt)
        top = case_plot(chart)

        death_points = chart.mark_line(
            point={"color": "lightgrey"}, 
            color="lightgrey"
        ).encode(
            x = alt.X("dt:T", title="Date"),
            y = alt.Y("deathIncrease:Q",title="Fatalities")
        )
        death_average = chart.mark_line().encode(
            x = alt.X('dt:T'),
            y = alt.Y('droll:Q')
        )

        bot = (death_points + death_average).properties(width=500, height=200)

    return (top & bot).properties(
        title=pop[pop.state==code].NAME.to_string(index=False)
    ).configure_legend(title=None).to_dict()