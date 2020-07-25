from flask import Flask, render_template
from .page import main_page

app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(main_page)
