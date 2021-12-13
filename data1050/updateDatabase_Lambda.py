import psycopg2 # uploaded with .zip file from: https://github.com/jkehler/awslambda-psycopg2
import numpy as np # add base SciPY/NumPY AWS layer
import requests # add layer by arn: arn:aws:lambda:us-west-1:770693421928:layer:Klayers-python38-requests:26


def lambda_handler(event, context):
    # LAMBDA FOR UPDATING DATABASE
    rds_host = "airquality.ce6w097amgsa.us-west-1.rds.amazonaws.com"
    name = ""
    password = ""
    db_name = ""

    conn = psycopg2.connect(host=rds_host,
                            database=db_name,
                            user=name,
                            password=password)
    cur = conn.cursor()
    # Get the time of the last update
    query = """SELECT max(make_timestamp(CAST(year AS int), CAST(month AS int), CAST(day AS int), CAST(utc_hour AS int), 0, CAST(0.0 AS double precision))) FROM airquality"""
    cur.execute(query)
    rows = cur.fetchall()
    latest_datetime = rows[0][0]

    states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
              'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
              'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
              'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
              'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New_Hampshire',
              'New_Jersey', 'New_Mexico', 'New_York', 'North_Carolina', 'North_Dakota',
              'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode_Island',
              'South_Carolina', 'South_Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
              'Virginia', 'Washington', 'West_Virginia', 'Wisconsin', 'Wyoming']

    # scrape the data and subset for all data since the last date
    for state in states:
        data = scrape_data(state)
        datetime_array = make_datetime_array(data)

        new_data = data[datetime_array > latest_datetime]

        if len(new_data) > 0:
            add_to_database(conn, cur, new_data, state)

    cur.close()
    conn.close()

    return 'Done'


def scrape_data(state):
    target_url = f'http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/{state}/{state}.txt'
    res = requests.get(target_url)
    text_file = open("/tmp/response.txt", "w")
    n = text_file.write(res.text.replace(' ', ''))
    text_file.close()
    data = np.loadtxt("/tmp/response.txt", comments='%')
    return data


def format_date(array):
    year = str(int(array[0]))
    month = str(int(array[1])) if len(str(int(array[1]))) > 1 else '0' + str(int(array[1]))
    day = str(int(array[2])) if len(str(int(array[2]))) > 1 else '0' + str(int(array[2]))
    hour = str(int(array[3])) if len(str(int(array[3]))) > 1 else '0' + str(int(array[3]))
    return np.datetime64(f'{year}-{month}-{day}T{hour}')


def make_datetime_array(data):
    datetime_array = np.zeros(len(data), dtype='datetime64[h]')
    for i, val in enumerate(data):
        datetime_array[i] = format_date(val)
    return datetime_array


def add_to_database(conn, cur, new_data, state):
    for row in new_data:
        row = list(row)
        del row[-1] # delete last "retrospective" value
        year, month, day, utc_hour, pm25, pm10 = row
        query = f"""INSERT INTO public.airquality (year, month, day, utc_hour, pm25, pm10, state)
        VALUES ({year}, {month}, {day}, {utc_hour}, {pm25}, {pm10}, \'{state}\')"""
        cur.execute(query)
        conn.commit()
