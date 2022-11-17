# Description of Data Warehouse project for Udacity's Data engineering Nanodegree
## Purpose of the project
The purpose of this project is to help a fictional streaming startup called Sparkify to create and hence move their data onto the cloud. The company has decided to commit to data warehouse on AWS using Redshift.

The task is perfored by ETL pipeline that extracts data from s3, stages them in Redshift, and transforms them into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Database and ETL design
A) Database
The database schema used was 'Star schema'. The schema consists of Fact and Dimension tables. Fact tables generally consist of numeric values and foreign keys to dimensional data. Dimension tables usually hold smaller number of records in comparison to Fact tables, but each record may have a very large number of attributes to describe the fact data. Dimension tables cover wide range of characteristics, but we can distinguish the following Dimension Table's types: time dimension tables, geography dimension tables, employee dimension tables among others.

Star schema offers the following benefits:
- simpler queries
- possibility of using simple joins to access data
- query performance gains for read_only / reporting applications
- fast aggregations 
- possibility of use in OLAP/ROLAP systems

(source: https://en.wikipedia.org/wiki/Star_schema)

B) ETL - Extract, transform, load
The ETL process is a three part activity where data is extracted from variety of sources, then it is cleaned and finally loaded into its destination container. In this project data is extracted from s3 bucket where it is stored in form of json files and delivered to staging tables hosted on Amazon Redshift Data Warehouse. Then the data is transformed and extracted. The final stage of the process - the loading - is being done by inserting data into the destination tables in Redshift.

(source: https://en.wikipedia.org/wiki/Extract,_transform,_load)

## Project's files description
1. create_tables.py - allows creation and deletion of databases.
2. dwh.cfg - contains constants for connecting and setting up resources on AWS.
3. dw_create.ipynb - allows setting up and deletion of data warehouse.
4. etl.py - loads data into staging tables and destination tables.
5. sql_queries.py - contains queries for creating and dropping tables as well as copying data from s3 and inserting data from staging tables to destination tables.
