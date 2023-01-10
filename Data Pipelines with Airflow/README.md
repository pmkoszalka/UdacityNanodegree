# Data Pipelines with Apache Airflow 
## Projects overview:
The objective of the project is to help a fictional music streaming company, Sparkify, to introduce automation and monitoring to their data warehouse ETL pipelines using Apache Airflow.

The company requires creation of high grade data pipelines that are dynamic and build from reusable tasks, can be monitored and allow easy backfills. Additionally, data monitoring measures are required after the ETL to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed to AWS Redshift using Apache Airflow.

## Concepts:
### Airflow
Airflow is a software created by Airbnb in 2014 to programmatically manage and schedule company's complex workflows. It contains build-in graphical user interfaces
that provides an overview of all the processes taking place within a workflow/pipeline. In 2016 the software became open source as an Apache Incubator and in 2019 it became 
a fully fledged Apache Software Foundation project.

(Source: https://en.wikipedia.org/wiki/Apache_Airflow)

### Directed acyclic graph
Directed acyclic graph (DAG) is a series of vertexes connected by edges where:
- edges of the graph only go one way (directed);
- it is not possible to traverse the entire graph starting at one edge (acyclic).

Please see the project workflow section to see an example. 

(Source: https://www.techopedia.com/definition/5739/directed-acyclic-graph-dag)

### Tasks
Basic units of execution in Airflow. They are arranged in DAGs. 

### Operators
Templates for a predefined task that can be defined inside DAG.

### Hooks
High level interfaces to an external platform that allows to connect to their API. Building Blocks of an Operator. 

(Source: https://airflow.apache.org/docs/apache-airflow/stable/index.html)

## Projects composition:
- dags - folder with dags.
    - create_tables.sql - contains queries to create sql tables.  
    - sparkify_dag.py - contains main logic of the workflow, runs dag and calls operators that perform ETL tasks.  
- plugins - folders with operators and helpers files.  
    - __init__.py - marks directories on disk as Python package directories.  
    - helpers - folder with helping files.  
        - __init__.py - marks directories on disk as Python package directories.  
        - sql_queries.py - contains queries for loading data into the tables on AWS Redshift.  
    - operators - folder containing custom made operators.  
        - __init__.py - marks directories on disk as Python package directories.  
        - data_quality.py - contains Airflow operator for validation of data.  
        - load_dimension.py - contains Airflow operator for loading data into dimension table.  
        - load_fact.py - contains Airflow operator that loads data into a fact table.  
        - stage_redshift.py - contains Airflow operator that copies data from s3 to tables in Redshift.  
- images - folder with images for README  
    - project_workflow.png - image of the project's workflow.  
- docker-compose.yaml - docker instruction for setting up container with airflow.

Note: after running docker-compose.yaml, new files/folders will be created including: config files and folder for logs.

## Project workflow:
![Project workflow](https://github.com/pmkoszalka/UdacityNanodegree/blob/main/Data%20Pipelines%20with%20Airflow/images/project_workflow.png)

## How to run the project?
1. Download the project
2. Run a docker container with command: docker-compose up
3. Enter in your web browser: http://localhost:8080/
4. Login: airflow Password: airflow
5. Create Redshift cluster
6. Create custom connections to AWS (named: 'aws_credentials') and to Redshift Database (named: 'redshift_postgres') in Airflow
7. Run the dag
8. Terminate the resources

Note: you might need to set the necessary permissions to dags, logs and plugins folders (example: sudo chmod 777 dags, logs, plugins)
