from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator


default_args = {
    "owner": "sparkify",
    "depends_on_past": False,
    "retries": 3,
    "catchup": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 4),
}

# directed acyclic graph for the project
with DAG(
    "sparkify_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@monthly",
) as dag:

    # constitutes a starting point for the projects workflow
    start_operator = DummyOperator(task_id="Begin_execution")

    # creates a tables on AWS Redshift
    create_tables_redshift = RedshiftSQLOperator(
        redshift_conn_id="redshift_postgres", task_id="Create_tables", sql="create_tables.sql"
    )

    # creates stage tables
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        dag=dag,
        redshift_conn_id="redshift_postgres",
        aws_credentials_id="aws_credentials",
        table="public.staging_events",
        s3_path="s3://udacity-dend/log_data/2018/11",
        schema_path="s3://udacity-dend/log_json_path.json",
        arn="arn:aws:iam::010456511385:role/dwhRole",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift_postgres",
        aws_credentials_id="aws_credentials",
        table="public.staging_songs",
        s3_path="s3://udacity-dend/song_data/A/B/C",
        schema_path="auto",
        arn="arn:aws:iam::010456511385:role/dwhRole",
    )

    # loads data into the fact table
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        table="public.songplays",
        sql_select=SqlQueries.songplay_table_insert,
        redshift_conn_id="redshift_postgres",
    )

    # loads data into the dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        table="public.users",
        sql_select=SqlQueries.user_table_insert,
        redshift_conn_id="redshift_postgres",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        table="public.songs",
        sql_select=SqlQueries.song_table_insert,
        redshift_conn_id="redshift_postgres",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        table="public.artists",
        sql_select=SqlQueries.artist_table_insert,
        redshift_conn_id="redshift_postgres",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        table="public.time",
        sql_select=SqlQueries.time_table_insert,
        redshift_conn_id="redshift_postgres",
    )

    # checks the quality of the data
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        tables=["public.time","public.artists"],
        redshift_conn_id="redshift_postgres",
    )

    # constitutes an ending point for the projects workflow
    end_operator = DummyOperator(task_id="Stop_execution")

    # workflow of the project:
    start_operator >> create_tables_redshift
    create_tables_redshift >> [stage_songs_to_redshift, stage_events_to_redshift, load_songplays_table]
    stage_events_to_redshift >> [load_user_dimension_table, load_songplays_table] >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    (
        stage_songs_to_redshift
        >> [load_artist_dimension_table, load_song_dimension_table]
        >> run_quality_checks
    )
    run_quality_checks >> end_operator
