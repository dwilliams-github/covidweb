from flask import Flask, render_template, send_from_directory
from .page import main_page

app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(main_page)

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