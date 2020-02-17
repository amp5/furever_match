
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
from queries import *

server = flask.Flask(__name__)
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=[dbc.themes.BOOTSTRAP]
)


app.layout = html.Div([
    dcc.Tabs(id="tabs", value='tab-1', children=[
        dcc.Tab(label='Analysis Dashboard', value='tab-1'),
        dcc.Tab(label='Engineering Dashboard', value='tab-2'),
    ]),
    html.Div(id='tabs-content')
])

analyst_df = run_query(get_data("coat_variations"))
sorted_analyst = analyst_df.sort_values(by='colors_primary')
hovertemplate1 = "<b>  %{y} and %{x} : %{z} Cats "


shelter_df = run_query(get_data("shelter_med"))
shelter_df['total'] = shelter_df['num_declawed'] + \
                      shelter_df['num_not_house_trained'] + \
                      shelter_df['num_shots_not_current'] + \
                      shelter_df['num_special_needs'] + \
                      shelter_df['num_not_fixed']

shelter_df = shelter_df.replace('NaN', 0)
final_shelter = shelter_df.sort_values(by=['total'], ascending=False).head(10)
org_names = final_shelter['organization_name']

trace1 = go.Bar(
    x=org_names, y=final_shelter['num_declawed'],
    name='Declawed'
)
trace2 = go.Bar(
    x=org_names, y=final_shelter['num_not_house_trained'],
    name='Not House Trained'
)
trace3 = go.Bar(
    x=org_names, y=final_shelter['num_shots_not_current'],
    name='Shots Not Current'
)
trace4 = go.Bar(
    x=org_names, y=final_shelter['num_special_needs'],
    name='Special Needs'
)
trace5 = go.Bar(
    x=org_names, y=final_shelter['num_not_fixed'],
    name='Needs Spay / Neuter'
)

stacked_d = [trace1, trace2, trace3, trace4, trace5]
stacked_layout = go.Layout(
    barmode='stack',
)

stacked_fig = go.Figure(data=stacked_d, layout=stacked_layout)



nulls = get_data('null')
engineer_df = run_query(nulls)
engineer_df['Percent Null'] = (engineer_df['num_null'] / engineer_df['total_rows'] * 100).round(2)
clean_eng = engineer_df[['table_name', 'column_name', 'num_null', 'Percent Null']].copy()
clean_eng.columns = ["Table Name", "Column Name", "Number of Null Values", 'Percent']
sorted_clean = clean_eng.sort_values('Percent', ascending=False)

page_1_layout = html.Div(children=[
    html.Div([
        html.H4('Shelter Level',
                style={
                    'textAlign': 'center',
                }
                ),
    ]),
    html.Div(
        dcc.Graph(figure=stacked_fig)
    ),
    html.Div([
        html.H4('Animal Level',
                style={
                    'textAlign': 'center',
                }
                ),
    ]),
    html.Div([
        dcc.Graph(
            id='coats_color',
            figure={
                'data': [
                    go.Heatmap(
                        z=sorted_analyst['num'],
                        x=sorted_analyst['coat'],
                        y=sorted_analyst['colors_primary'],
                        colorscale='Viridis',
                        hoverongaps=False,
                        hovertemplate=hovertemplate1)
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Coat Color Variations'},
                    yaxis={'title': 'Coat Length Variations'},
                    hovermode='closest',
                    width=1000,
                    height=500,
                    autosize=True,
                    font=dict(
                        size=12,
                        color="#7f7f7f"
                    ),
                )
            }
        ),
    ],
        style={'width': '80%', 'display': 'inline-block', 'vertical-align': 'right', 'padding': '10%'}
    ),
]
)

page_2_layout = html.Div(children=[
    html.H3('Missing values'),
    dash_table.DataTable(
        id='table1',
        columns=[{"name": i, "id": i} for i in sorted_clean.columns],
        data=sorted_clean.to_dict('records'),
        sort_action="native",
        style_cell={'textAlign': 'center'},
        style_header={
            'backgroundColor': 'white',
            'fontWeight': 'bold'
        },
        style_cell_conditional=[
            {
                'if': {'column_id': 'Column Name'},
                'textAlign': 'left'
            },
            {
                'if': {'column_id': 'Table Name'},
                'textAlign': 'left'
            },
            # {
            #     'if': {
            #         'column_id': 'Percent',
            #         'filter_query': '{Percent} > 50'
            #     },
            #     'backgroundColor': '#99493d',
            #     'color': 'white',
            # },
        ],

    ),
],
 style={'padding': '10%'}
)


@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-1':
        return html.Div(page_1_layout)
    elif tab == 'tab-2':
        return html.Div([
            page_2_layout
        ])


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)










