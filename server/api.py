from flask import Blueprint, jsonify, request
from .drivers import country, state, county

api = Blueprint('api', __name__)
    
@api.route("/api/country/graph")
def plot_country():
    return jsonify(country.plot(request.args.get('code','US')))


@api.route("/api/country/composite")
def plot_country_composite():
    mode = request.args.get('mode','TC')
    if mode == 'NA':
        return jsonify(country.north_america())
    if mode == 'SC':
        return jsonify(country.scandinavia())
    if mode == 'EU':
        return jsonify(country.europe())
    return jsonify({'status':'failure'}), 400

    
@api.route("/api/state/graph")
def plot_state():
    code = request.args.get('code','US')
    mode = request.args.get('mode','D')
    if mode == 'V':
        return jsonify(state.vaccines(code))
    else:
        return jsonify(state.plot(code))

    
@api.route("/api/state/composite")
def plot_state_composite():
    mode = request.args.get('mode','TC')
    if mode == 'TC':
        return jsonify(state.top_four_cases())
    if mode == 'TCC':
        return jsonify(state.top_four_cases_capita())
    if mode == 'TF':
        return jsonify(state.top_five_fatalities())
    if mode == 'TFC':
        return jsonify(state.top_five_fatalities_capita())
    if mode == 'DB':
        return jsonify(state.death_bar())
    if mode == 'B4':
        return jsonify(state.big_four_cases())
    if mode == 'B4C':
        return jsonify(state.big_four_cases_capita())
    if mode == 'B4F':
        return jsonify(state.big_four_fatalities())
    return jsonify({'status':'failure'}), 400


@api.route("/api/county/simple")
def plot_county_simple():
    return jsonify(county.simple_plot(request.args.get('code','Santa Clara, California')))

    
@api.route("/api/county/composite")
def plot_county_composite():
    mode = request.args.get('mode','B')
    if mode == 'B':
        return jsonify(county.both())
    if mode == 'SV':
        return jsonify(county.silicon_valley())
    if mode == "CC":
        return jsonify(county.california_bar(False))
    if mode == "CCC":
        return jsonify(county.california_bar(True))
    return jsonify({'status':'failure'}), 400
