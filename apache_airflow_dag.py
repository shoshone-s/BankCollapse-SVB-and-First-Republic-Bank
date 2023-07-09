#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

# [START tutorial]
# [START import_module]
import json

# import pendulum

# from transform.transform import transform_all

import extract
import transform
import load

# from airflow.decorators import dag, task

# [END import_module]


# [START instantiate_dag]
# @dag(
#     schedule=None,
#     start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=["our_dag"],
# )
# def tutorial_taskflow_api():
#     """
#     ### TaskFlow API Tutorial Documentation
#     This data pipeline is based on the Airflow TaskFlow API tutorial is
#     located
#     [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
#     """
#     # [END instantiate_dag]

#     # [START extract]
#     @task()
#     def extract_all():
#         """
#         #### Extract task
#         Extract the data from all data sources and load into s3 bucket
#         """
#         extract.market_watch.extract()
#         extract.fdic.extract()
#         extract.secEDGAR_api.extract()
#         extract.y_finance.extract()
#         extract.macrotrends.extract()
#         extract.alpha_vantage.extract()

#     # [END extract]

#     # [START transform]
#     @task(multiple_outputs=True)
#     def transform_all():
#         """
#         #### Transform task
#         A simple Transform task which takes in the collection of order data and
#         computes the total order value.
#         """
#         transform.market_watch.transform()
#         transform.fdic.transform()
#         transform.secEDGAR_api.transform()
#         transform.y_finance.transform()
#         transform.macrotrends.transform()
#         transform.alpha_vantage.transform()

#     # # [END transform]

#     # [START load]
#     @task()
#     def load_all():
#         """
#         #### Load task
#         A simple Load task which takes in the result of the Transform task and
#         instead of saving it to end user review, just prints it out.
#         """

#         load.load_to_redshift() 

#     # [END load]

#     # # # [START main_flow]
#     # # order_data = extract()
#     # # order_summary = transform(order_data)
#     # # load(order_summary["total_order_value"])
#     # # # [END main_flow]

#     extract_all()
#     transform_all()
#     load_all()





# [START dag_invocation]
# tutorial_taskflow_api()
# [END dag_invocation]

# [END tutorial]


extract.market_watch.extract()
extract.fdic.extract()
extract.secEDGAR_api.extract()
extract.y_finance.extract()
extract.macrotrends.extract()
extract.alpha_vantage.extract()

transform.market_watch.transform()
transform.fdic.transform()
transform.secEDGAR_api.transform()
transform.y_finance.transform()
transform.macrotrends.transform()
transform.alpha_vantage.transform()

load.load_to_redshift() 
