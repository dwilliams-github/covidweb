from flask import Blueprint, jsonify, request
from .drivers import country, state, county

api = Blueprint('api', __name__)
    
@api.route("/api/country/graph")
def plot_country():
    return jsonify(country.plot(request.args.get('code','US')))

    
@api.route("/api/state/graph")
def plot_state():
    return jsonify(state.plot(request.args.get('code','US'), request.args.get('mode','D')))

    
@api.route("/api/state/composite")
def plot_state_composite():
    mode = request.args.get('mode','TC')
    if mode == 'TC':
        return jsonify(state.top_four_cases())
    if mode == 'TF':
        return jsonify(state.top_five_fatalities())
    if mode == 'TFC':
        return jsonify(state.top_five_fatalities_capita())
    if mode == 'DB':
        return jsonify(state.death_bar())
    if mode == 'B4':
        return jsonify(state.big_four())

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
