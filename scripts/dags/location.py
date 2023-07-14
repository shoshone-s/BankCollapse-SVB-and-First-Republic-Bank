from __future__ import annotations

from airflow.decorators import dag, task
import pendulum
from pathlib import Path
import sys
scripts_path = Path(__file__).resolve().parents[1]
sys.path += [str(scripts_path / "utilities"), str(scripts_path), str(scripts_path / "extract"), str(scripts_path / "transform"), str(scripts_path / "load")]

import util
import fdic
import location
from redshift_load_data import load_to_redshift


TABLE_NAME = "location"
QUEUE_URL = f"https://sqs.us-east-2.amazonaws.com/{util.ACCOUNT_ID}/{util.QUEUE_NAME}"

@dag(
    schedule=None,
    depends_on_past=False,
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),
    catchup=False,
    tags=["our_dag"],
)
def location_importer():
    """
    ### ETL for locations
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
        """
        #### Extract task
        Extract the data from all data sources and load into S3 bucket / raw_data
        """
        fdic.extract(table_name=TABLE_NAME)

    @task()
    def transform():
        """
        #### Transform task
        Clean data and load int S3 bucket / transformed_data
        """
        location.transform(table_name=TABLE_NAME)

    @task()
    def load():
        """
        #### Load task
        Copy clean data from S3 to Redshift
        """
        load_to_redshift(sql_table_name=TABLE_NAME)
        
    @task()
    def wait_raw_data():
        s3_sensor_raw_data_fdic = SqsSensor(
            sqs_queue=QUEUE_URL,
            message_filtering='literal',
            message_filtering_match_values=f"data/raw_data/fdic_{TABLE_NAME}.csv"
        )

    @task()
    def wait_clean_data():
        s3_sensor_transformed_data = SqsSensor(
            sqs_queue=QUEUE_URL,
            message_filtering='literal',
            message_filtering_match_values=f"data/transformed_data/{TABLE_NAME}.csv"
        )

    # [END load]

    extract()
    wait_raw_data()
    transform()
    wait_clean_data()
    load()

location_importer()

