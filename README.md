# Furever Match

#### Insight Data Engineering Seattle, WA


![](https://github.com/amp5/furever_match/blob/master/reports/pictures/giphy.gif)



## Project Idea
My goal for this project was to create a tool that would enable animal shelters access to the insights of the their data. Whether that be a volunteer at the shelter, an analyst of data scientist at a larger non-profit like ASPCA or the engineers who help keep everything running. My focus of this project was on cat adoption data using Petfinder's national adoption data API and building the architecture for a tool that would support not only large amounts of data but also various kinds of data. The ultimate goal being to help kitties everywhere find their furever homes 😻.


## Tech Stack
Tools: AWS S3, AWS EC2, Apache Spark SQL, PostgreSQL, Flask, Dash

This project was built using the above tools. First I called Petfinder's API to extract data on current cat adoptions and store raw JSON files into AWS S3. From there I used Spark to extract, transform and load data from AWS S3 to Postgres using Spark's main features as well as Spark SQL to query and analyze the data. Throughout this process JSON data was transformed into dataframe, normalized and condenced into multiple tables in Postgres. This pipeline was also deployed on AWS EC2 and the resulting information found in Postgres was visualized in a Flask web application using Dash for visualizations. 


## What I would like to work on next with Furever Match
- Incorporate Airflow into this to call Petfinder API daily or 2x a day. 
- Add additional data sources, perhaps hyper local datasets from shelters.
- Build out historical snapshots of data to track progression of cat profiles over time.
- Collaborate with data scientists / analysts to build ML models in the decription data to highlight key features for cats to retroactively add to the data set. For example overall temperment of the cat or health issues mentioned in the description but not added into the original key value pairs of JSON. I could also build this myself :)
