import psycopg2
import pandas as pd
from pandas_profiling import ProfileReport


def query_database(query):
    rds_host = "airquality.ce6w097amgsa.us-west-1.rds.amazonaws.com"
    name = "postgres"
    password = "L2PnKR!&M2eGL5"
    db_name = "postgres"

    conn = psycopg2.connect(host=rds_host,
                            database=db_name,
                            user=name,
                            password=password)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(rows)


query = f"""SELECT state, make_timestamp(CAST(year AS int), CAST(month AS int), CAST(day AS int), CAST(utc_hour AS int), 0, CAST(0.0 AS double precision)), pm25, pm10 FROM airquality
    WHERE year = 2021"""
data = query_database(query)
data.columns = ['state', 'date', 'pm2.5', 'pm10']
profile = ProfileReport(data)
profile.to_file(output_file=r'C:/Users/Peter/Documents/airqualityprofile.html')


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

# import plotly.express as px
# fig = px.line(rows, x="timestamp", y="pm25", color='state')
# fig.show()
# px.histogram(rows, x='pm25')
