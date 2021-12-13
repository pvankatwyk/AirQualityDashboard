# AirQualityDashboard
This repository includes a Plotly Dash project for displaying air quality over time. The data was retrieved from Berkeley Earth (berkeleyearth.lbl.gov) and uploaded into a  database instance hosted on Amazon Web Services (AWS). Then, at a fixed interval, a Lambda function is called to periodically update the data from the hourly updating data hosted by Berekely Earth.

A live version of this dashboard hosted on AWS Elastic Beanstalk can be found here:
http://airqualitydashboard-env.eba-c7tdh9sd.us-west-1.elasticbeanstalk.com/

For more information on the project or the stack, feel free to reach out to me personally at pvankatwyk@gmail.com.
