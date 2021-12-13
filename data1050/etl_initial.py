# Initial ETL Script
# Script for scraping the data to populate the database. The script with loop through
# the 50 states and scrape the data from berkeley earth and compile the data into
# on dataframe. Then, I used pgAdmin4 (for PostgreSQL) to upload a CSV into a database
# which is hosted on AWS RDS.
import pandas as pd


def get_state_data(state):
    url = f'http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States/{state}/{state}.txt'
    state_data = pd.read_csv(url, skiprows=8, delimiter='\t', header=None)
    state_data.columns = ['year', 'month', 'day', 'utc_hour', 'pm25', 'pm10', 'retrospective']
    state_data['state'] = state
    state_data = state_data.drop(columns=['retrospective'])
    return state_data


states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
          'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
          'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
          'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
          'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New_Hampshire',
          'New_Jersey', 'New_Mexico', 'New_York', 'North_Carolina', 'North_Dakota',
          'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode_Island',
          'South_Carolina', 'South_Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
          'Virginia', 'Washington', 'West_Virginia', 'Wisconsin', 'Wyoming']

all_data = pd.DataFrame()
for state in states:
    print(state)
    state_data = get_state_data(state)
    state_data = state_data.loc[state_data['year'] >= 2021] # change to any year -- only 2021 for small DB instance
    all_data = pd.concat([all_data, state_data])

out_fp = r'C:\Users\Peter\Documents\air_quality_data_2021.csv'
all_data.to_csv(out_fp, index=False)
