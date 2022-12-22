# Description of Data Lake project for Udacity's Data engineering Nanodegree
## Purpose of the project
The purpose of this project is to help a fictional streaming startup called Sparkify to extract, transform and load their data from and to Data Lake/AWS s3 using engine for large-scale data analytics - Spark.

## Schema design and ELT pipeline.
The database schema used was 'Star schema'. The schema consists of Fact and Dimension tables. Fact tables generally consist of numeric values and foreign keys to dimensional data. Dimension tables usually hold smaller number of records in comparison to Fact tables, but each record may have a very large number of attributes to describe the fact data. Dimension tables cover wide range of characteristics, but we can distinguish the following Dimension Table's types: time dimension tables, geography dimension tables, employee dimension tables among others.

(source: https://en.wikipedia.org/wiki/Star_schema)

The process of extracion, transformation and load of data used in the project is known as 'ETL'. The ETL process is a three part activity where data is extracted from variety of sources, then it is cleaned and finally loaded into its destination container. In this project data is extracted from s3 bucket where it is stored in form of json files, then the date is transformed in order to create tables as a star schema. Subsequently, data is delivered to s3.

(source: https://en.wikipedia.org/wiki/Extract,_transform,_load)

## Project's files description
- etl.py - script that extracts data from s3, creates tables for the project and loads them into data lake hosted on s3.
- dl.cfg - contains secret information for connecting and setting up resources on AWS.