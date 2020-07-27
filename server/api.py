from flask import Blueprint, jsonify, request
from .drivers import country, state

api = Blueprint('api', __name__)
    
@api.route("/api/country/graph")
def plot_country():
    return jsonify(country.plot(request.args.get('code','US')))

    
@api.route("/api/state/graph")
def plot_state():
    return jsonify(state.plot(request.args.get('code','US'), request.args.get('mode','D')))
