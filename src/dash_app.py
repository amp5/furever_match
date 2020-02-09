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


query1 = """select animal_info.coat, animal_info.colors_primary, count(*) as num from animal_info where animal_info.coat is not NULL and animal_info.colors_primary is not NULL group by animal_info.coat, animal_info.colors_primary;"""
analyst_df = run_query(query1)
sorted_analyst = analyst_df.sort_values(by='colors_primary')
hovertemplate1 = "<b>  %{y} and %{x} : %{z} Cats "


#### Shelter query
shelter_q = """
                select
                    declaw.organization_id,
                    num_declawed,
                    num_not_house_trained,
                    num_shots_not_current,
                    num_special_needs,
                    num_not_fixed
                from
                    (select
                        organization_id,
                        count(declawed) as num_declawed
                    from animal_medical_info
                    left join animal_info
                        on animal_info.id = animal_medical_info.id
                    where declawed is True
                    group by organization_id) declaw
                
                left join
                    (select
                        organization_id,
                        count(house_trained) as num_not_house_trained
                    from animal_medical_info
                    left join animal_info
                        on animal_info.id = animal_medical_info.id
                    where house_trained is False
                    group by organization_id) house
                on declaw.organization_id = house.organization_id
                left join
                    (select
                        organization_id,
                        count(shots_current) as num_shots_not_current
                    from animal_medical_info
                    left join animal_info
                        on animal_info.id = animal_medical_info.id
                    where shots_current is False
                    group by organization_id) shots
                on declaw.organization_id = shots.organization_id
                left join
                    (select
                        organization_id,
                        count(special_needs) as num_special_needs
                    from animal_medical_info
                    left join animal_info
                        on animal_info.id = animal_medical_info.id
                    where special_needs is True
                    group by organization_id) special
                on declaw.organization_id = special.organization_id
                left join
                    (select
                        organization_id,
                        count(spayed_neutered) as num_not_fixed
                    from animal_medical_info
                    left join animal_info
                        on animal_info.id = animal_medical_info.id
                    where spayed_neutered is False
                    group by organization_id) fixed
                on declaw.organization_id = fixed.organization_id;
            """
shelter_df = run_query(shelter_q)
shelter_df['total'] = shelter_df['num_declawed'] + \
                      shelter_df['num_not_house_trained'] + \
                      shelter_df['num_shots_not_current'] + \
                      shelter_df['num_special_needs'] + \
                      shelter_df['num_not_fixed']

shelter_df = shelter_df.replace('NaN', 0)
final_shelter = shelter_df.sort_values(by=['total'], ascending=False).head(10)
org_names = final_shelter['organization_id']

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

def table_selection(table):
    if table  == 'temp':
        query_selection = """
                select 
                    *, 
                    (
                        select 
                            count(*) as total_rows 
                        from animal_temperment
                    ) 
                from
                    (
                        (select 'dogs' as column_name,  count(*) as num_null from animal_temperment where dogs is null) 
                        union
                        (select 'id' as column_name,  count(*) as num_null from animal_temperment where id is null ) 
                        union
                        (select 'children' as column_name,  count(*) as num_null from animal_temperment where children is null ) 
                        union
                        (select 'cats' as column_name,  count(*) as num_null from animal_temperment where cats is null )
                    ) nulls;"""
    engineer_df = run_query(query_selection)
    engineer_df['Percent Null'] = (engineer_df['num_null'] / engineer_df['total_rows'] * 100).round(2)

    clean_eng = engineer_df[['column_name', 'num_null', 'Percent Null']].copy()
    clean_eng.columns = ["Column Name", "Number of Null Values", 'Percent Null']
    return clean_eng

table_df = table_selection('temp')

page_1_layout = html.Div(children=[
    navbar,
    html.H1('Analysis Dashboard',
            style={
                'textAlign': 'center',
            }
    ),
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
    style={'width': '80%', 'display': 'inline-block', 'vertical-align': 'right'}
    ),
    ]
)





page_2_layout = html.Div(children=[
        navbar,
        html.H1('Engineering Dashboard',
                style = {
                'textAlign': 'center',
            }
        ),
        html.Br(),
        html.H4('Select Table'),
        dcc.Dropdown(id= 'table_dropdown',
                     options=[
                         {'label': 'Medical Information', 'value': 'med'},
                         {'label': 'Animal Information', 'value': 'ani'},
                         {'label': 'Temperment Information', 'value': 'temp'},
                         {'label': 'Status Information', 'value': 'stat'},
                         {'label': 'Animal Description', 'value': 'descr'}
                     ],
                     value= 'temp'
        ),

        html.Br(),
        dash_table.DataTable(
            id='table1',
            columns=[{"name": i, "id": i} for i in table_df.columns],
            data=table_df.to_dict('records'),
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
                    }
                ]
        ),

        dcc.Link('Go back to home', href='/')
    ],
    style={'padding': '10px 40px 40px 40px'}
)


@app.callback(dash.dependencies.Output('table1', 'figure'),
            [dash.dependencies.Input('table_dropdown', 'value')])
def update_output(table_opt):

    return table_opt


@app.callback(dash.dependencies.Output('page-1-content', 'children'),
              [dash.dependencies.Input('page-1-dropdown', 'value')])
def page_1_dropdown(value):
    return 'You have selected "{}"'.format(value)

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








