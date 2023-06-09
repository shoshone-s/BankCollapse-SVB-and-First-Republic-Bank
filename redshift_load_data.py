import aws_read_write
import configparser

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
CREATE TABLE IF NOT EXISTS location (
    cert               INTEGER NOT NULL,
    company_name       VARCHAR(255),
    main_office        BOOL,
    branch_name        VARCHAR(255),
    established_date   DATE,
    service_type       VARCHAR(255),
    address            VARCHAR(255),
    county             VARCHAR(255),
    city               VARCHAR(255),
    state              VARCHAR(255),
    zip                VARCHAR(5),
    latitude           NUMERIC,
    longitude          NUMERIC
);
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

sql_create_price_history = """
CREATE TABLE IF NOT EXISTS price_history (
    symbol          VARCHAR(6) NOT NULL,
    date            DATE,
    "open"          NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    adjusted_close  NUMERIC,
    volume          INTEGER
);
"""

aws_read_write.drop_table(
    region_name=REDSHIFT_REGION_NAME,
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    table_name="price_history")

aws_read_write.execute_sql(
    region_name=REDSHIFT_REGION_NAME,
    database_name=REDSHIFT_DB_NAME,
    workgroup_name=REDSHIFT_WORKGROUP_NAME,
    sql_statement=sql_create_price_history)

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