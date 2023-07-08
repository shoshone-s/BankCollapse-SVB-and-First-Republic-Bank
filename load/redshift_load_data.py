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


# locations = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='transformed_data/locations.csv')

sql_create_location = """
"""

aws_read_write.drop_table(
    region_name=REDSHIFT_REGION_NAME,
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    table_name="location")

aws_read_write.execute_sql(
    region_name=REDSHIFT_REGION_NAME,
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    sql_statement=sql_create_location)

aws_read_write.copy_from_s3(
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    table_name="location",
    object_path="s3://ds4ateam20/transformed_data/locations.csv",
    iam_role=IAM_REDSHIFT,
    region_name=REDSHIFT_REGION_NAME)

print(
aws_read_write.execute_sql(
    region_name=REDSHIFT_REGION_NAME,
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    sql_statement="SELECT COUNT(*) FROM location")
)


# price_history = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='transformed_data/price_history.csv')


aws_read_write.drop_table(
    region_name=REDSHIFT_REGION_NAME,
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    table_name="price_history")

# aws_read_write.execute_sql(
#     region_name=REDSHIFT_REGION_NAME,
#     database_name=REDSHIFT_DB_NAME,
#     workgroup_name=REDSHIFT_WORKGROUP_NAME,
#     sql_statement=sql_create_price_history)

aws_read_write.copy_from_s3(
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    table_name="price_history",
    object_path="s3://ds4ateam20/transformed_data/price_history.csv",
    iam_role=IAM_REDSHIFT,
    region_name=REDSHIFT_REGION_NAME)

print(
aws_read_write.execute_sql(
    region_name=REDSHIFT_REGION_NAME,
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    sql_statement="SELECT COUNT(*) FROM price_history")
)

# Create a for loop that iterates through the list of files in the sql_scripts folder and executes each file in the Redshift database
def load_to_redshift():
    
    for file in os.listdir("sql_scripts"):
        if file.endswith(".sql"):
            with open(os.path.join("sql_scripts", file), "r") as sql_file:
                sql_table_name = str(file.split(".")[0])
                sql_script = sql_file.read()
                s3_obj_path = f"s3://ds4ateam20/transformed_data/{sql_table_name}.csv"

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

load_to_redshift() 