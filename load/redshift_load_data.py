"""
Loads transformed data from S3 to Redshift
"""

import aws_read_write
import configparser
import os

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")

AWS_ACCESS_KEY_ID = cfg_data["AWS"]["access_key_id"]
AWS_SECRET_ACCESS_KEY = cfg_data["AWS"]["secret_access_key"]

S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

REDSHIFT_DB_NAME = cfg_data["Redshift"]["database_name"]
REDSHIFT_WORKGROUP_NAME = cfg_data["Redshift"]["workgroup_name"]
IAM_REDSHIFT = cfg_data["Redshift"]["iam_role"]
REDSHIFT_REGION_NAME = cfg_data["Redshift"]["region_name"]

# Create a for loop that iterates through the list of files in the sql_scripts folder and executes each file in the Redshift database
def load_to_redshift(sql_table_name):
    sql_file = ''
    sql_script = sql_file.read()
    s3_obj_path = f"s3://ds4ateam20/clean_data/{sql_table_name}.csv"

    # TODO: 

    """
        check if it exists... drop if the columns don't match...
    """
    aws_read_write.drop_table(
        region_name=REDSHIFT_REGION_NAME,
        database_name=REDSHIFT_DB_NAME,
        workgroup_name=REDSHIFT_WORKGROUP_NAME,
        table_name=sql_table_name)

    aws_read_write.execute_sql(
        region_name=REDSHIFT_REGION_NAME,
        database_name=REDSHIFT_DB_NAME,
        workgroup_name=REDSHIFT_WORKGROUP_NAME,
        sql_statement=sql_script)

    aws_read_write.copy_from_s3(
        database_name=REDSHIFT_DB_NAME,
        workgroup_name=REDSHIFT_WORKGROUP_NAME,
        table_name=sql_table_name,
        object_path=s3_obj_path,
        iam_role=IAM_REDSHIFT,
        region_name=REDSHIFT_REGION_NAME)

    print(f"Successfully loaded {sql_table_name} to Redshift")

# load_to_redshift() 