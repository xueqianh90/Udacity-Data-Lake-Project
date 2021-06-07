
# Data Lake

Create a database schema and ETL pipeline for a startup called Sparkify to extract data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

#### Database Schema Design

> **Fact Table**

>> ***songplays*** - playing records in log data associated with song title, artist name, and song duration time that match from the staging_events and staging_songs tables. 

> **Dimension Tables**

>> ***users*** - users info extracted from staging_events table
>> ***songs*** - songs info extracted from staging_songs table
>> ***artists*** - artists info extracted from staging_songs table
>> ***time*** - timestamps of records in songplays broken down into specific units

#### ETL Pipeline (files used: sql_queries.py, etl.py)

Get all the needed song and log datasets from from S3. Transform the data and then load data to each tables. 


## Data And Files Overview

**etl.py** - Loads and processes song and log datasets from from S3, and copys and inserts them into tables. 
**dl.cfg** - Contains IAM's credentials

## How to Run The Python Scripts

To run Python scripts, open a command-line and type in the word "python" followed by the path to the script.


