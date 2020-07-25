from flask import Blueprint, render_template

main_page = Blueprint('page', __name__)
@main_page.route("/")
def index():
    return render_template(
        "main.html",
        message="Hello"
    )