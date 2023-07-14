# import util
# import aws_read_write

# cleaning up the sec data
# - ie. this should drop data outside of the 2017-2022 dates
def clean_sec_data():
    dest_table_name = 'sec_data'
    source = 'sec_data'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    # sec_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)



    