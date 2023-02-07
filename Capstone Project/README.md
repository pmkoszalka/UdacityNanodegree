# Udacity Data Egineering Capstone Project
### Rationale for the project
The purpose of this project is to investigate the relationship between immigration, airports and demographics for US cities.

### Scope
Projects is a pipeline that involves: data extraction, data cleaning and transformation, loading data into AWS S3 (data lake) and transferring data to AWS Redshift's Data Warehouse.

### Datasets
1) [I94 Immigration Data](https://www.trade.gov/national-travel-and-tourism-office)
2) [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
3) [Airport Code Table](https://datahub.io/core/airport-codes#data)

### Technologies used
- Pandas for exploratory data analysis and ETL process
- PySpark for reading the parquet file
- S3 for storing the data in the cloud
- Redshift for creation of Data Warehouse
- Postgres/SQL for CRUD operations

### Data model
Database model contain six tables: states, cities, airports, demographics, flights and passengers. Each table has a single primary key and there are no repeating groups. The relationships between tables are defined through the use of foreign keys, which provide referential integrity between tables. This ensures that there are no duplicate data and minimizes data redundancy.

For description of the columns in each table, please refer to: data_dictionary.md.
For diagram of tables in the database, please refer to: database_diagram.png.

### Example of questions that data model can answer
What is the percentage of immigrants in population of Stevenson, Washington based on the data available in datasets?

```with immigrants as (SELECT Count(*) as count_immigrants FROM passengers p
INNER JOIN flights f ON p.flight_id = f.id
INNER JOIN cities c ON f.city_id = c.id
WHERE city = 'Stevenson' and state_id = 47), 

total_population as (SELECT total_population FROM demographics d
INNER JOIN cities c ON d.city_id = c.id
WHERE city='Stevenson' and state_id = 47)

SELECT cast(count_immigrants as float) / total_population * 100 AS percentage
FROM immigrants, total_population;
```
The query should yield: 3.906089041224176

### How often data should be updated?
The anlysis should focus on the most up-to-date immigration data. Since source immigration data is produced once a month, the data should be updated monthly.
### How to run the project?
1. Fill the fields in config.cfg.
2. Create AWS Redshift cluster (you can use dw_create.ipynb).
3. Run etl.py.

### Files description:
airports.py - performs airports on demographic DataFrame.
all_df.py - perfroms transformations on all DataFrames.
bucket.py - performs actions with AWS S3 Bucket.
config.cfg - configuration file.
data_dictionary.md - description of tables.
database_diagram.png - displays relations between tables.
dbconnection.py - allows connection to database.
demographics.py - performs transformations on demographic DataFrame.
dw_create.ipynb - template for creation of Redshift cluster.
etl.py - pipline for the whole workflow of the project.
immigration.py - performs immigration on demographic DataFrame.
logging_config.py - config for event logging system.
redshift.py - performs actions with database on Redshift.
sql_queries.py - produces queries for creation and deletion of database and copy the data.
validate.py - validates data in AWS Redshift.
data/ - folder containg data used for the project.
    airport-codes.csv - airports data.
    I94_SAS_Labels_Descriptions.SAS - description of immigration data.
    immigration_data_sample.csv - immigration data.
    us-cities-demographics.csv - demographics of US cities data.
    sas_data/ - folder containing full immigration data.

### Scenarios to consider
1. The data was increased by 100x.
If the data was increased 100 fold then we could no longer rely on Pandas as a tool for transformation/processing of data. At that point, a tool for parallel data processing on a cluster should be considered. I would take advantage of Apache Spark which is much faster than Pandas on large data sets due to its in-memory computing and parallel processing capabilities. I would set Spark on Amazon EMR that would allow me to scale my cluster up or down depending on the workload. Moreover, EMR provides performance optimizations for Spark, such as caching data in memory, reducing disk I/O, and fine-tuning configurations for the specific workloads.
2. The data populates a dashboard that must be updated on a daily basis by 7am every day.
I would consider using Apache Airflow (pipeline orchestration tool). I would schedule a workflow to run before a certain hour by specifying the desired execution time in the "schedule_interval" (for example: schedule_interval="0 6 * * *" - this means that DAG would be run everyday at 6 am) then I would create an operator to check the current hour and determine if the workflow managed to succeed before 7:00 AM. If the workflow didn't end before 7AM, automatic email would be sent to engineering team to fix the issue.
3. The database needed to be accessed by 100+ people.
In this project, the data is loaded to AWS Redshift Data Warehouse. This service is more than capable of handling over 100 users. To facilitate this action, I would create a role for read only access (or other depending on the business need) to the database and assign the users to that role.