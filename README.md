## PURPOSE OF THE DATABASE

The startup, Sparkify want to move their processes and data onto the cloud(AWS). They need to build ETL pipeline to extract data stored in S3 and stage them in Redshift, transform data into a star schema.

## EXPLANATION OF THE FILES

#### create_tables.py
Connect to the database using AWS cluster's informations and Create tables.
It also drops the tables if it exists, so I can use this function whenever I want to re-create the tables.

#### dwh.cfg
It contains cluster's informations, ARN and paths of datasets stored is S3.
These are used for connecting to the cluster, granting roles and loading data to staging tables, respectively.

#### etl.py
Copy the data into the staging tables and insert data into the fact-dimension tables.

#### sql_queries.py
All the SQL queries used in create_tables.py and etl.py are organized in this file.

## STEPS FOR COMPLETION

- Complete create statements in sql_queries.py(you can also complete  drop statements here)
- Create a cluster in AWS
- Check the informations of cluster and save it into dwh.cfg(CLUSTER)
- Run create_tables.py(if you don't complete drop statements in the first step, complete it)
- Complete stage and insert statements
- Run etl.py
- Check the tables running SELECT statement 

## DATABASE SCHEMA DESIGN

<center>
  <img
    src="erd.png"
    width="700"
    height="1400"
  />
</center>