import flask
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from access_db import *
import plotly.express as px
from dash.dependencies import Input, Output


server = flask.Flask(__name__)
app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/',
    external_stylesheets=[dbc.themes.BOOTSTRAP]
    )


navbar_children = [
    dbc.NavItem(dbc.NavLink("Page 1", href="/page-1")),
    dbc.NavItem(dbc.NavLink("Page 2", href="/page-2")),
]

navbar = dbc.Navbar(navbar_children, sticky="top")

query = """select coat, count(*) from animal_info group by coat;"""
df = run_query(query)

app.layout = html.Div(children=[
    html.H1(children='Dash Tutorials'),
    dcc.Graph(
        id='example1',
        figure={
            'data': [
                {'x': [1, 2, 3, 4, 5], 'y': [9, 6, 2, 1, 5], 'type': 'line', 'name': 'Boats'},
                {'x': [1, 2, 3, 4, 5], 'y': [8, 7, 2, 7, 3], 'type': 'bar', 'name': 'Cars'},
            ],
            'layout': {
                'title': 'Basic Dash Example'
            }
        }

    ),
dcc.Input(id='input', value='Enter Table name', type ='text'),
    html.Div(id='output')
])

# @app.callback(
#     Output(component_id='output', component_property='children'),
#     [Input(component_id='input', component_property='value')])
# def update_value(input_data):
#     return "Input: {}".format(input_data)


# fig = go.Figure(data=go.Heatmap(
#                    z=[[1, None, 30, 50, 1], [20, 1, 60, 80, 30], [30, 60, 1, -10, 20]],
#                    x=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
#                    y=['Morning', 'Afternoon', 'Evening'],
#                    hoverongaps = False))

fig = px.bar(df, x="coat", y="count", barmode="group")
fig.show()

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
