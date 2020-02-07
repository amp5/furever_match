import flask
import dash
import dash_table
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objs as go
from access_db import *

server = flask.Flask(__name__)
app = dash.Dash(
    __name__,
    server=server,
    # routes_pathname_prefix='/',
    external_stylesheets=[dbc.themes.BOOTSTRAP]
    )


navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dcc.Link('Analysts   ', href='/page-1')),
        html.Br(),
        dbc.NavItem(dcc.Link('   Engineers', href='/page-2')),
    ],
    brand="Furever Match",
    brand_href="#",
    sticky="top",
)


app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    dcc.Link('Analysts', href='/page-1'),
    html.Br(),
    dcc.Link('Engineers', href='/page-2'),
])

test_query = "select * from animal_temperment;"
trq = run_query(test_query)
print("what am I?")
print(trq)

query1 = """select animal_info.coat, animal_info.color_primary, count(*) as num from animal_info where animal_info.coat is not NULL and animal_info.color_primary is not NULL group by animal_info.coat, animal_info.color_primary;"""
analyst_df = run_query(query1)
hovertemplate1 = "<b>  %{y} and %{x} : %{z} Cats "
print("this is my query result")
print(analyst_df)

query2 = """select raw.*, round(raw.num_null / raw.total_rows , 2) as pct_null  from (select *, (select count(*) as total_rows from animal_temperment) from
((select 'dogs' as column_name,  count(*) as num_null from animal_temperment where dogs is null ) union
    (select 'id' as column_name,  count(*) as num_null from animal_temperment where id is null ) union
    (select 'children' as column_name,  count(*) as num_null from animal_temperment where children is null ) union
    (select 'cats' as column_name,  count(*) as num_null from animal_temperment where cats is null )) nulls) raw;"""
engineer_df = run_query(query2)


page_1_layout = html.Div(children=[
        navbar,
        html.H1('Analysis Dashboard'),
        dcc.Graph(
                id='coats_color',
                figure={
                    'data': [
                        go.Heatmap(
                            z=analyst_df['num'],
                            x=analyst_df['colors_primary'],
                            y=analyst_df['coat'],
                            colorscale='Viridis',
                            hoverongaps=False,
                        hovertemplate=hovertemplate1)
                    ],
                    'layout': go.Layout(
                        xaxis={'title': 'Coat Color Variations'},
                        yaxis={'title': 'Coat Length Variations'},
                        hovermode='closest'
                    )
                }
        ),
        dcc.Link('Go back to home', href='/'),
    ],
    style={'padding': '10px 40px 40px 40px'}
)

@app.callback(dash.dependencies.Output('page-1-content', 'children'),
              [dash.dependencies.Input('page-1-dropdown', 'value')])
def page_1_dropdown(value):
    return 'You have selected "{}"'.format(value)


page_2_layout = html.Div(children=[
        navbar,
        html.H1('Engineering Dashboard'),
        dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in engineer_df.columns],
            data=engineer_df.to_dict('records'),
            sort_action="native",
            style_cell={'textAlign': 'left'},
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
        ),

        dcc.Link('Go back to home', href='/')
    ],
    style={'padding': '10px 40px 40px 40px'}
)

@app.callback(dash.dependencies.Output('page-2-content', 'children'),
              [dash.dependencies.Input('page-2-radios', 'value')])
def page_2_radios(value):
    return 'You have selected "{}"'.format(value)


# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/page-1':
        return page_1_layout
    elif pathname == '/page-2':
        return page_2_layout
    else:
        return index_page




if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
