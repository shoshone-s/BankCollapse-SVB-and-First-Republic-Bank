from __future__ import annotations

import json
import os

import extract
import load
import pendulum
import transform
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=["our_dag"],
)
def financials_importer():
    """
    ### TaskFlow API Tutorial Documentation
    This data pipeline is based on the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract_all():
        """
        #### Extract task
        Extract the data from all data sources and load into s3 bucket
        """
        
        extract.market_watch.load_raw_financials()
        extract.fdic.load_raw_financials()
        extract.secEDGAR_api.load_raw_financials()
        extract.y_finance.load_raw_financials()
        extract.macrotrends.load_raw_financials()
        extract.alpha_vantage.load_raw_financials()

    @task(multiple_outputs=True)
    def transform():
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        transform.financials.load_clean_data()

    @task()
    def load_all():
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        table_name = 'financials'
        load.load_to_redshift(table_name) 

    # [END load]

    extract()
    transform()
    load()

financials_importer()

