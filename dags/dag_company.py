import datetime as dt
from airflow import DAG
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.date(2023,7,1),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG(
    'dag_price_history',
    default_args=default_args, 
    description="stock and index price history"
    dagrun_timeout=timedelta(hours=2),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # pull data and save to S3 raw_data
    extract_yf = BashOperator(
        task_id='extract_yf', 
        bash_command='python3 ${AIRFLOW_HOME}/dags/yahoo_finance.py',
    )
    extract_av = BashOperator(
        task_id='extract_av', 
        bash_command='python3 ${AIRFLOW_HOME}/dags/alpha_vantage.py',
    )
    extract_dj = BashOperator(
        task_id='extract_dj', 
        bash_command='python3 ${AIRFLOW_HOME}/dags/djusb.py',
    )
    
    # trigger for new file in S3 bucket - message sent to SQS
    # sensor to check for new messages
    
    s3_sensor_raw_data_yf = SqsSensor(  
        task_id='s3_sensor_raw_data_yf',  
        sqs_queue='https://sqs.us-east-2.amazonaws.com/912879684479/team20_queue',
        message_filtering='literal',
        message_filtering_config='',
        message_filtering_match_values='data/raw_data/stock_price_daily.csv'
        dag=dag
    )
    s3_sensor_raw_data_av = SqsSensor(  
        task_id='s3_sensor_raw_data_yf',  
        sqs_queue='https://sqs.us-east-2.amazonaws.com/912879684479/team20_queue',
        message_filtering='literal',
        message_filtering_config='',
        message_filtering_match_values='data/raw_data/price_history.csv'
        dag=dag
    )
    s3_sensor_raw_data_dj = SqsSensor(  
        task_id='s3_sensor_raw_data_yf',  
        sqs_queue='https://sqs.us-east-2.amazonaws.com/912879684479/team20_queue',
        message_filtering='literal',
        message_filtering_config='',
        message_filtering_match_values='data/raw_data/dow_jones_us_banks_index.csv'
        dag=dag
    )
    
    # TODO: add all transforms for each data sources 

    # transform data and save to S3 transformed_data
    transform_raw_data = BashOperator(
        task_id='transform_raw_data', 
        bash_command='python3 ${AIRFLOW_HOME}/dags/transform.py',
    )
    
    # wait for files to be available in S3
    s3_sensor_transformed_data = SqsSensor(  
        task_id='s3_sensor_transformed_data',  
        sqs_queue='https://sqs.us-east-2.amazonaws.com/912879684479/team20_queue',
        message_filtering='jsonpath',
        message_filtering_config='',
        message_filtering_match_values='data/transformed_data/price_history.csv'
        dag=dag
    )
    
    # copy data to Redshift
    load_to_redshift = BashOperator(
        task_id='load_to_redshift', 
        bash_command='python3 ${AIRFLOW_HOME}/dags/redshift_load_data.py',
    )
    
    # task dependencies
    [extract_yf,extract_av,extract_dj] >> \
    [s3_sensor_raw_data_yf,s3_sensor_raw_data_av,s3_sensor_raw_data_dj] >> \
    transform_raw_data >> \
    s3_sensor_transformed_data >> \
    load_to_redshift