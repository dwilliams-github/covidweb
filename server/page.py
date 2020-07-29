from flask import Blueprint, render_template
from .drivers.country import menu as country_menu
from .drivers.state import menu as state_menu
from .drivers.county import menu as county_menu

main_page = Blueprint('main_page', __name__)
@main_page.route("/")
def index():
    return render_template(
        "main.html",
        country_menu = country_menu(),
        state_menu = state_menu(),
        county_menu = county_menu()
    )

@main_page.route("/about")
def about():
    return render_template("about.html")