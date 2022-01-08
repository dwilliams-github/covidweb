from flask import Blueprint, jsonify, request
from .drivers import country, state, county

api = Blueprint('api', __name__)
    
@api.route("/api/country/graph")
def plot_country():
    return jsonify(country.plot(
        request.args.get('code','US'),
        int(request.args.get('time',0))
    ))


@api.route("/api/country/composite")
def plot_country_composite():
    mode = request.args.get('mode','TC')
    time = int(request.args.get('time',0))
    if mode == 'NA':
        return jsonify(country.north_america(time))
    if mode == 'SC':
        return jsonify(country.scandinavia(time))
    if mode == 'EU':
        return jsonify(country.europe(time))
    return jsonify({'status':'failure'}), 400

    
@api.route("/api/state/graph")
def plot_state():
    code = request.args.get('code','US')
    mode = request.args.get('mode','D')
    time = int(request.args.get('time',0))
    if mode == 'V':
        return jsonify(state.vaccines(code,time))
    elif mode == 'H':
        return jsonify(state.hospitals(code,time))
    else:
        return jsonify(state.plot(code,time))

    
@api.route("/api/state/composite")
def plot_state_composite():
    mode = request.args.get('mode','TC')
    if mode == 'VB':
        return jsonify(state.vaccines_bar())
    if mode == 'VP':
        return jsonify(state.vaccines_by_party())
    if mode == 'DB':
        return jsonify(state.death_bar())

    time = int(request.args.get('time',0))
    if mode == 'TC':
        return jsonify(state.top_four_cases(time))
    if mode == 'TCC':
        return jsonify(state.top_four_cases_capita(time))
    if mode == 'TF':
        return jsonify(state.top_five_fatalities(time))
    if mode == 'TFC':
        return jsonify(state.top_five_fatalities_capita(time))
    if mode == 'B4':
        return jsonify(state.big_four_cases(time))
    if mode == 'B4C':
        return jsonify(state.big_four_cases_capita(time))
    if mode == 'B4F':
        return jsonify(state.big_four_fatalities(time))
    return jsonify({'status':'failure'}), 400


@api.route("/api/county/simple")
def plot_county_simple():
    return jsonify(county.simple_plot(
        request.args.get('code','Santa Clara, California'),
        int(request.args.get('time',0))
    ))

    
@api.route("/api/county/composite")
def plot_county_composite():
    mode = request.args.get('mode','B')
    if mode == "CC":
        return jsonify(county.california_bar(False))
    if mode == "CCC":
        return jsonify(county.california_bar(True))

    time = int(request.args.get('time',0))
    if mode == 'B':
        return jsonify(county.both(time))
    if mode == 'SV':
        return jsonify(county.silicon_valley(time))
    return jsonify({'status':'failure'}), 400
