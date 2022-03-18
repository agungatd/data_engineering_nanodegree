### Introduction
A startup named Sparkify, a music streaming app, has been collected songs and user activity logs and wants to create data analysis from the data. This project is to build data warehouse and build an ETL pipeline using Python and AWS Redshift.

### Database Schema design and ETL process
With events and song dataset, we can create a star schema for optimal data analysis. a star schema contains a fact table with several dimension tables.
for the fact table we will create songplays table and for the dimension tables we will create users, artist, time, and song tables.

## How to run the Python scripts
first, You need to create EC2, S3, Redshift, and IAM resources.
then, make sure that you are in `/home/workspace` working directory.
afterthat, run `python create_tables.py` to reset the database and run create queries using the `sql_queries.py` from the terminal.
finally, run `python etl.py` from the terminal to execute the ETL process.

## An explanation of the files in the repository
- `sql_queries.py` (scripts to run SQL queries for dropping, creating, and inserting tables)
- `create_tables.py` (scripts to reset and create the tables using the sql_queries.py file)
- `etl.py` (scripts to run the ETL process)
- `dwh.cfg` (configuration file for the AWS EC@2, S3, Redshift, and IAM resources)