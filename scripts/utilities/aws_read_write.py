import io
from botocore.exceptions import ClientError
import boto3
import pandas as pd
import configparser
from pathlib import Path

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read(Path(__file__).resolve().parents[2] / "config" / "keys_config.cfg")
#print(cfg_data.sections())

AWS_ACCESS_KEY_ID = cfg_data["AWS"]["access_key_id"]
AWS_SECRET_ACCESS_KEY = cfg_data["AWS"]["secret_access_key"]


def upload_file(file_name, bucket_name, object_name):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
        print("Successful S3 upload_file response.")
    except ClientError as e:
        print("Unsuccessful S3 upload_file response.")
        logging.error(e)


def get_file(bucket_name, object_name):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_name
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")

    return response


def get_csv(bucket_name, object_name):
    response = get_file(bucket_name, object_name)

    df = pd.read_csv(response.get("Body"))
    return df


def get_parquet(bucket_name, object_name):
    response = get_file(bucket_name, object_name)

    buffer = io.BytesIO()
    s3 = boto3.resource("s3")
    s3_object = s3.Object(bucket_name, object_name)
    s3_object.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
    return df


# execute sql statement
def execute_sql(region_name, database_name, workgroup_name, sql_statement):
    print(sql_statement)

    # create Redshift data client
    redshift_data_client = boto3.client(
        "redshift-data",
        region_name=region_name
    )

    response1 = redshift_data_client.execute_statement(
        Database=database_name,
        WorkgroupName=workgroup_name,
        Sql=sql_statement
    )
    # print("response 1\n", response1, "\n")

    response2 = redshift_data_client.describe_statement(Id=response1["Id"])

    # wait for query to finish
    while response2["Status"] in ["PICKED", "STARTED", "SUBMITTED"]:
        response2 = redshift_data_client.describe_statement(Id=response1["Id"])

    # if query aborted or failed
    status = response2["Status"]
    if status != "FINISHED":
        print(f"Unsuccessful Redshift Data execute_statement response. Status - {status}")
        print(response2)

    try:
        # get query result, status = finished
        response3 = redshift_data_client.get_statement_result(Id=response1["Id"])

        # return dataframe of result
        if response3["TotalNumRows"] > 0:

            columns = []
            for i in range(len(response3["ColumnMetadata"])):
                columns.append(response3["ColumnMetadata"][i]["name"])

            # list of list of dicts to list of lists (list of row values)
            result_records = response3["Records"]
            return pd.DataFrame([[list(d.values())[0] for d in r] for r in result_records], columns=columns)

        else:
            print("No rows from query")
    except ClientError as error:
        print("Query does not have result")


# drop table from database
def drop_table(region_name, database_name, workgroup_name, table_name):
    sql_statement = f"DROP TABLE IF EXISTS {table_name};"
    execute_sql(region_name, database_name, workgroup_name, sql_statement)


# populate table with data from S3
def copy_from_s3(region_name, database_name, workgroup_name, table_name, object_path, iam_role):
    
    data_format = "CSV" if object_path.endswith('csv') else "PARQUET" if object_path.endswith('parquet') else ""
    
    addl_params = """
        DELIMITER ',' 
        BLANKSASNULL 
        EMPTYASNULL 
        FILLRECORD 
        IGNOREBLANKLINES 
        IGNOREHEADER 1 
        ROUNDEC 
        TRIMBLANKS 
    """ if object_path.endswith('csv') else ""
    
    sql_statement = f"""
        COPY {table_name} 
        FROM '{object_path}' 
        IAM_ROLE '{iam_role}' 
        REGION '{region_name}' 
        FORMAT AS {data_format} 
        {addl_params}
        ;
    """
    execute_sql(region_name, database_name, workgroup_name, sql_statement)