## Summary
A startup named Sparkify, a music streaming app, has been collected songs and user activity logs and wants to create data analysis from the data. This project is to build data modeling with Postgres and build an ETL pipeline using Python.

## How to run the Python scripts
first, make sure that you are in `/home/workspace` working directory.
then, run `create_tables.py` to reset the database and run the `sql_queries.py` from the terminal using command `python create_tables.py`.
afterthat, run `etl.py` to insert the data into the database from the terminal using command `python etl.py`.

## An explanation of the files in the repository
- `sql_queries.py` (scripts to run SQL queries for dropping, creating, and inserting tables)
- `create_tables.py` (scripts to reset the database using the sql_queries.py file)
- `etl.py` (scripts to run the ETL process)
- `etl.ipynb` (jupyter notebook to debug and create the ETL scripts)
- `test.ipynb` (jupyter notebook to conduct sanity test of the database)
- data folder contains all the json formated songs and user activity data