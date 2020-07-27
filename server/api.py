from flask import Blueprint, jsonify, request
from .drivers import country, state

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
