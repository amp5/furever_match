import flask
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from access_db import *
import plotly.express as px
from dash.dependencies import Input, Output
import plotly.graph_objs as go


server = flask.Flask(__name__)ß
app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/',
    external_stylesheets=[dbc.themes.BOOTSTRAP]
    )


navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Analysts", href="#coats_color")),
        dbc.NavItem(dbc.NavLink("Engineers", href="#missing_data")),
    ],
    brand="Furever Match",
    brand_href="#",
    sticky="top",
)

query1 = """select coat, colors_primary, count(*) from animal_info where coat is not NULL and colors_primary is not NULL group by coat, colors_primary;"""
analyst_df = run_query(query1)



query2 = """select raw.*, round(raw.num_null / raw.total_rows , 2) as pct_null  from (select *, (select count(*) as total_rows from animal_temperment) from
((select 'dogs' as column_name,  count(*) as num_null from animal_temperment where dogs is null ) union
    (select 'id' as column_name,  count(*) as num_null from animal_temperment where id is null ) union
    (select 'children' as column_name,  count(*) as num_null from animal_temperment where children is null ) union
    (select 'cats' as column_name,  count(*) as num_null from animal_temperment where cats is null )) nulls) raw;"""
engineer_df = run_query(query2)


hovertemplate = "<b>  %{y} and %{x} : %{z} Cats "

app.layout = html.Div(children=[
    navbar,
    dcc.Graph(
            id='coats_color',
            figure={
                'data': [
                    go.Heatmap(
                        z=analyst_df['count'],
                        x=analyst_df['colors_primary'],
                        y=analyst_df['coat'],
                        colorscale='Viridis',
                        hoverongaps=False,
                    hovertemplate=hovertemplate)
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Coat Color Variations'},
                    yaxis={'title': 'Coat Length Variations'},
                    hovermode='closest'
                )
            }
    ),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in engineer_df.columns],
        data=engineer_df.to_dict('records'),
    ),

],
style={'padding': '10px 40px 40px 40px'})


d = [{"name": i, "id": i} for i in dft.columns]

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
