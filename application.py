import dash
from datetime import date
import pandas as pd
import psycopg2 as psycopg2
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import plotly.express as px
import json
import time

# Setting dictionary (fonts, colors, etc.)
settings = {
    'background': '#aad3df',
    'text': '#0f3057',
    'accent': '#008891',
    'background-2': '#e7e7de',
    'company': '#2dcf11',
    'map_zoom': 3.25,
    'font-family': 'Open Sans, sans-serif'
}

app = dash.Dash(
    name=__name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width"}],
    title='AirQuality',
    assets_folder="static",  # Elastic Beanstalk recognizes "static folder"
    assets_url_path="static"
)
application = app.server

# State dictionary (for later)
states = {'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO',
          'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
          'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
          'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
          'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
          'New_Hampshire': 'NH', 'New_Jersey': 'NJ', 'New_Mexico': 'NM', 'New_York': 'NY', 'North_Carolina': 'NC',
          'North_Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
          'Rhode_Island': 'RI', 'South_Carolina': 'SC', 'South_Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX',
          'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West_Virginia': 'WV',
          'Wisconsin': 'WI', 'Wyoming': 'WY'}
states_query = tuple(states.keys())


# --- Functions to build graphics ---

def query_database(query):
    """
    :param query: (str) query to send to airquality database
    :return rows: (list of lists) all rows returned from sql query
    """
    rds_host = "airquality.ce6w097amgsa.us-west-1.rds.amazonaws.com"
    name = "" # these values are hidden for public repo for
    password = "" # more information on connecting to the
    db_name = "" # database, contact me directly!

    conn = psycopg2.connect(host=rds_host,
                            database=db_name,
                            user=name,
                            password=password)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows


def build_map(data, particulate_val):
    """
    :param data: (dataframe) Data to be plotted
    :param particulate_val: (str) Value for particulate matter (pm25 or pm10)
    :return fig: (figure) plotly mapbox figure
    """
    with open(r'static/gz_2010_us_040_00_5m.json') as fp:
        geojson = json.load(fp)
    particulate_val_formatted = 'PM2.5' if particulate_val == 'pm25' else 'PM10'
    fig = px.choropleth_mapbox(data, geojson=geojson,
                               featureidkey="properties.NAME", locations='state', color=particulate_val,
                               color_continuous_scale="RdYlGn_r",
                               mapbox_style="open-street-map",
                               zoom=3, center={"lat": 37.0902, "lon": -95.7129},
                               opacity=0.75,
                               labels={'state': 'State', particulate_val: particulate_val_formatted},
                               template='seaborn'
                               )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},
                      paper_bgcolor='#aad3df')
    return fig


def build_line(data, state, particulate):
    """
    :param data: (dataframe) Data to be plotted
    :param state: (tuple) states to be plotted
    :param particulate: (str) Value for particulate matter (pm25 or pm10)
    :return fig: (figure) plotly mapbox figure
    """
    if len(state) >= 50:
        data = data.groupby('timestamp', as_index=False).mean()
        fig = px.line(data, x='timestamp', y=particulate, template='seaborn')
    else:
        data = data.sort_values(by='timestamp')
        fig = px.line(data, x='timestamp', y=particulate, color='state', template='seaborn')

    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},
                      paper_bgcolor=settings['background-2'],
                      plot_bgcolor=settings['background-2'],
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.90
                      ),
                      )
    return fig


def build_hist(data, state, particulate):
    """
    :param data: (dataframe) Data to be plotted
    :param state: (tuple) states to be plotted
    :param particulate: (str) Value for particulate matter (pm25 or pm10)
    :return fig: (figure) plotly mapbox figure
    """
    if len(state) >= 50:
        fig = px.histogram(data, x=particulate, template='seaborn')
    else:
        fig = px.histogram(data, x=particulate, color='state', template='seaborn', opacity=0.80)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},
                      paper_bgcolor=settings['background-2'],
                      plot_bgcolor=settings['background-2'],
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.9
                      ),
                      )
    return fig


# --- Configure Layout ---
app.layout = \
    html.Div(
        id='whole-container',
        style={
            'background-color': settings['background'],
        },
        children=[
            dcc.Store(id='data-store'),
            # Div for the entire container (background div)
            html.Div(
                style={
                    'background-color': settings['background'],
                    'font-family': settings['font-family'],
                    'margin': '0px 10px 0px 10px'
                },
                children=[
                    # Div for sidebar and figures
                    html.Div(
                        className='row',
                        children=[

                            # Div for sidebar settings
                            html.Div(
                                id='left-sidebar',
                                className='four columns div-user-controls',
                                children=[
                                    html.H4('''Air Quality Analysis Dashboard''',
                                            style={
                                                'color': settings['text'],
                                                'margin-top': '5px',
                                                'margin-bottom': '0px',
                                                'text-align': 'center',
                                                'margin-right': '10px'
                                            }),
                                    html.P('Peter Van Katwyk, Berkeley Earth',
                                           style={
                                               'color': settings['text'],
                                               'text-align': 'center',
                                               'margin-right': '20px'
                                           }),

                                    # Div for Date Dropdown
                                    html.Div(
                                        id='dates',
                                        className='div-for-dropdown',
                                        children=[
                                            html.H6('Instructions',
                                                    style={
                                                        'border-top': '3px solid ' + settings['text'],
                                                        'color': settings['text'],
                                                        'margin-bottom': '0px',
                                                        'margin-top': '10px',
                                                        'padding-top': '10px',
                                                        'text-decoration': 'underline'
                                                    }),
                                            html.P("""Change the fields below and click \"Apply Filters\" to 
                                            update the map and figure(s) to the right. The purpose of this dashboard
                                            is to help visualize the quality of air throughout the US. Users can 
                                            choose to view air quality (PM 2.5 & PM 10) at different locations during
                                            2021. Air quality data is updated twice per day. 
                                            """,
                                                   style={
                                                       'color': settings['text'],
                                                       'margin-bottom': '0px',
                                                       'font-size': '13px'
                                                   }
                                                   ),
                                            html.P("""For more information on the air quality measurement, PM 2.5 and 
                                            PM 10, see the following link:
                                            """,
                                                   style={
                                                       'color': settings['text'],
                                                       'margin-bottom': '0px',
                                                       'margin-top': '10px',
                                                       'font-size': '13px'
                                                   }
                                                   ),
                                            html.A('Air Quality Index',
                                                   href="https://en.wikipedia.org/wiki/Air_quality_index",
                                                   style={
                                                       'color': settings['text'],
                                                       'margin-bottom': '0px',
                                                       'font-size': '13px'
                                                   }
                                                   ),
                                            html.P('Date Range',
                                                   style={
                                                       'color': settings['text'],
                                                       'margin-bottom': '0px',
                                                       'margin-top': '10px',
                                                       'border-top': '3px solid ' + settings['text']
                                                   }),
                                            dcc.DatePickerRange(
                                                id='date-picker',
                                                start_date=date(2021, 1, 1),
                                                min_date_allowed=date(2021, 1, 1),
                                                max_date_allowed=date.today(),
                                                initial_visible_month=date.today(),
                                                end_date=date.today(),
                                                className='DateRangePickerInput',
                                            ),
                                        ],
                                    ),

                                    # Div for PM Type
                                    html.Div(
                                        id='pm-type',
                                        className='div-for-pm',
                                        style={'margin-right': '10px'},
                                        children=[
                                            html.P('State',
                                                   style={
                                                       'color': settings['text'],
                                                       'margin-top': '10px',
                                                       'margin-bottom': '0px'
                                                   }),
                                            dcc.Dropdown(
                                                id='state-dropdown',
                                                options=[{'label': x.replace('_', ' '), 'value': x} for x in
                                                         set(states.keys())],
                                                value=None,
                                                multi=True
                                            ),
                                            html.P('Particulate Matter Level',
                                                   style={
                                                       'color': settings['text'],
                                                       'margin-top': '10px',
                                                       'margin-bottom': '0px'
                                                   }),
                                            dcc.Dropdown(
                                                id='particulate-dropdown',
                                                options=[
                                                    {'label': 'PM 2.5', 'value': 'pm25'},
                                                    {'label': 'PM 10', 'value': 'pm10'},
                                                ],
                                                value='pm25'
                                            ),

                                            html.P('Aggregate Function',
                                                   style={
                                                       'color': settings['text'],
                                                       'margin-top': '10px',
                                                       'margin-bottom': '0px'
                                                   }),
                                            dcc.Dropdown(
                                                id='aggregate-function',
                                                options=[
                                                    {'label': 'Average', 'value': 'mean'},
                                                    {'label': 'Median', 'value': 'median'},
                                                    {'label': 'Max', 'value': 'max'},
                                                    {'label': 'Min', 'value': 'min'},
                                                ],
                                                value='median',
                                            )
                                        ],
                                    ),
                                    # Div for Apply Filter button
                                    html.Div(
                                        id='apply-filters-button-div',
                                        style={
                                            'margin-right': '10px'
                                        },
                                        children=[
                                            html.Button('Apply Filters',
                                                        id='apply-filters-button',
                                                        style={
                                                            'margin-top': '20px',
                                                            'width': '100%',
                                                            'border': '1px solid ' + settings['text']
                                                        }),
                                            dcc.Download(id='apply')
                                        ],
                                    ),

                                    # Div for bottom of sidebar
                                    html.Div(
                                        id='bottom-sidebar-div',
                                        className='div-for-dropdown',
                                        children=[

                                            # Div for text at the bottom of the sidebar
                                            html.Div(
                                                id='bottom-text',
                                                className='text-padding',
                                                children=[
                                                    html.P("""Note: Be patient when loading the website and applying 
                                                    filters. Because this is a school project, I am using the free tier 
                                                    AWS Database (RDS) so the instance is not very fast. Expect delays
                                                    of up to 1 minute to load the website and 30 seconds for filters 
                                                    applied. Thanks!""",
                                                           style={
                                                               'margin-bottom': '0px',
                                                               'margin-top': '35px',
                                                               'color': settings['text'],
                                                               'font-size': '12px'
                                                           }
                                                           ),
                                                    html.Br(),
                                                    html.P('v1.0.2 - Air Quality Dashboard',
                                                           style={
                                                               'margin-bottom': '0px',
                                                               'color': settings['text']
                                                           }),
                                                    html.P('Website Developer: Peter Van Katwyk',
                                                           style={
                                                               'font-size': '12px',
                                                               'margin-bottom': '0px',
                                                               'color': settings['text']
                                                           }),
                                                    html.P('Data retrieved from: berkeleyearth.lbl.gov',
                                                           style={
                                                               'font-size': '12px',
                                                               'margin-bottom': '0px',
                                                               'color': settings['text']
                                                           }),

                                                ],
                                            ),
                                        ]
                                    ),
                                ],
                            ),

                            # Div for right figure container
                            html.Div(
                                id='right-graphs',
                                className='eight columns div-for-charts bg-grey',
                                style={
                                    'border-left': '3px solid ' + settings['text'],
                                    'margin-left': '0px'

                                },
                                children=[
                                    # Div for map
                                    html.Div(
                                        id='map-div',
                                        className='row graph-top',
                                        style={
                                            'margin-top': '5px',
                                            'margin-left': '5px'
                                        },
                                        children=[
                                            dcc.Graph(
                                                id='map',
                                                className='chart-graph',
                                            ),
                                        ],
                                    ),
                                    # Div for graphs below
                                    html.Div(
                                        id='graphs-div',
                                        style={
                                            'margin-left': '5px',
                                            'margin-top': '20px'
                                        },
                                        children=[
                                            dcc.Tabs(id='tabs',
                                                     children=[
                                                         dcc.Tab(label='Air Quality vs Time',
                                                                 style={
                                                                     'background-color': settings['background'],
                                                                     'border': '3px ' + settings[
                                                                         'background-2'] + ' solid',
                                                                 },
                                                                 selected_style={
                                                                     'background-color': settings['background-2']
                                                                 },
                                                                 className='custom-tab',
                                                                 selected_className='custom-tab--selected',
                                                                 children=[
                                                                     dcc.Graph(
                                                                         id='quality-line',
                                                                         style={
                                                                             'height': '300px'
                                                                         }
                                                                     )
                                                                 ],
                                                                 ),
                                                         dcc.Tab(label='Air Quality Histogram',
                                                                 style={
                                                                     'background-color': settings['background'],
                                                                     'border': '3px ' + settings[
                                                                         'background-2'] + ' solid',
                                                                 },
                                                                 selected_style={
                                                                     'background-color': settings['background-2']
                                                                 },
                                                                 className='custom-tab',
                                                                 selected_className='custom-tab--selected',
                                                                 children=[
                                                                     dcc.Graph(
                                                                         id='quality-hist',
                                                                         style={
                                                                             'height': '300px'
                                                                         }
                                                                     ),
                                                                 ])
                                                     ]
                                                     ),
                                        ],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )


# Callback for updating the map and histogram
@app.callback(
    [Output('data-store', 'data'),
     Output('map', 'figure'),
     Output('quality-line', 'figure'),
     Output('quality-hist', 'figure')],  # output
    [Input('apply-filters-button', 'n_clicks')],  # input
    [State('date-picker', 'start_date'),
     State('date-picker', 'end_date'),
     State('particulate-dropdown', 'value'),
     State('state-dropdown', 'value'),
     State('aggregate-function', 'value'), ]  # state
)
def apply_filter(n_clicks, start_date, end_date, particulate, state, aggregate_fxn):
    # if there hasn't been any specification for states, show data from all states
    if state in (None, '') or len(state) == 0:
        state = (
            'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida',
            'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
            'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska',
            'Nevada', 'New_Hampshire', 'New_Jersey', 'New_Mexico', 'New_York', 'North_Carolina', 'North_Dakota', 'Ohio',
            'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode_Island', 'South_Carolina', 'South_Dakota', 'Tennessee',
            'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West_Virginia', 'Wisconsin', 'Wyoming')

    else:
        if len(state) > 1:
            state = tuple(state)
        else:
            # if tuple of size 1, format for query
            state = "(\'" + str(state[0]) + "\')"

    # set default values
    if particulate in (None, ''):
        particulate = 'pm25'

    if aggregate_fxn in (None, ''):
        aggregate_fxn = 'median'

    # set query according to filters
    query = f"""SELECT state, make_timestamp(CAST(year AS int), CAST(month AS int), CAST(day AS int), CAST(utc_hour AS int), 0, CAST(0.0 AS double precision)) AS timestamp, {particulate} FROM airquality
    WHERE make_timestamp(CAST(year AS int), CAST(month AS int), CAST(day AS int), CAST(utc_hour AS int), 0, CAST(0.0 AS double precision))
    BETWEEN CAST(\'{start_date}\' AS DATE) and CAST(\'{end_date}\' AS DATE)
    AND state in {state};
    """

    # time query (personal/developer use)
    start = time.time()
    data = query_database(query)
    finish = time.time()
    print("Query Time:", finish - start)

    # Convert data based on aggregate function specification
    df = pd.DataFrame(data)
    df.columns = ['state', 'timestamp', str(particulate)]
    if aggregate_fxn in 'mean':
        df_map = df.groupby('state', as_index=False).mean()
    elif aggregate_fxn in 'median':
        df_map = df.groupby('state', as_index=False).median()
    elif aggregate_fxn in 'min':
        df_map = df.groupby('state', as_index=False).min()
    elif aggregate_fxn in 'max':
        df_map = df.groupby('state', as_index=False).max()
    else:
        df_map = df.groupby('state', as_index=False).median()

    map = build_map(df_map, particulate)
    line = build_line(df, state, particulate)
    hist = build_hist(df, state, particulate)

    return [data, map, line, hist]


if __name__ == '__main__':
    app.run_server(debug=True)
