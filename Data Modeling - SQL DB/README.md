Data Modeling – SQL DB

Purpose of the project

The purpose of the project is to help a fictional startup Sparkify to create a database containing the data.

How to run scripts?

Please run the scripts directly from the command line or in jupyter notebooks provided

Content

The project contains: 

-	etl.py and etl.ipynb – both files run the extract, transform and load process of data for the use of sql_queries.py and sql_queries.ipynb
-	sql_queries.py and sql_queries.ipynb – both files create database, tables and load data into the tables
-	test.ipynb -runs the test to check if the requirements for the project have been met 
-	data.zip – zipped folder with data
-	.gitignore – ignores files/folders while adding to git/github

Schema:

For this project the star schema was used. A star schema is a database organizational structure optimized for use in a data warehouse. It uses one fact table and connected dimensional tables. The star schema combines simple design with fast read and queries, easy data aggregation and is easy integrated with OLAP systems.
Resource used: https://www.techtarget.com/searchdatamanagement/definition/star-schema).

ETL Pipeline

ETL pipeline is performed in etl.py and etl.ipynb. It extracts data from data.zip folder, transforms it into several tables, then data is stored in the form of list of tuples. Finally, data is loaded to appropriate tables using sql_queries.py and sql_queries.ipynb.
