from jinja2 import StrictUndefined
from flask import Flask, render_template, redirect, request, flash, session, url_for, jsonify
from flask_bootstrap import Bootstrap


app = Flask(__name__)
Bootstrap(app)
app.secret_key = "AcBbCa"
app.jinja_env.undefined = StrictUndefined


@app.route('/')
def homepage():
    """This will bring us to the homepage"""


    return render_template('dashboard.html')



# @app.route('/')
# def index():
#     """Show homepage"""
#
#     return """
#     <html>
#     <body>
#       <h1>I am the landing page</h1>
#     </body>
#     </html>
#     """



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
