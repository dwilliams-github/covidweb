from flask import Flask, render_template, send_from_directory
from .page import main_page
from .api import api

app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(main_page)
app.register_blueprint(api)

#
# Static routes, provided here for development.
# Consider routing these directly from your web server.
#
@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('../static/css', path)

@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('../static/js', path)
    
@app.route('/assets/<path:path>')
def send_assets(path):
    return send_from_directory('../static/assets', path)