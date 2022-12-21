# import awswrangler as wr
# import pandas as pd
# import urllib.parse
# import os

# # Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
# os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']    
# # inside environment var we will store some info such as output location for our processed file
# os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']      
# # catalog name create catalog for that
# os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
# # table name we will create for that 
# os_input_write_data_operation = os.environ['write_data_operation']       
# # either we want to append data or overwrite the data


# def lambda_handler(event, context):
#     # Get the object from the event and show its content type
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     try:

#         # Creating DF from content
#         df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

#         # Extract required columns:
#         df_step_1 = pd.json_normalize(df_raw['items'])

#         # Write to S3
#         wr_response = wr.s3.to_parquet(
#             df=df_step_1,
#             path=os_input_s3_cleansed_layer, # writing this data to s3 bucket 
#             dataset=True,
#             database=os_input_glue_catalog_db_name,  # also creating glue catalog 
#             table=os_input_glue_catalog_table_name,  # and table on to the glue
#             mode=os_input_write_data_operation
#         )

#         return wr_response
#     except Exception as e:
#         print(e)
#         print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
#         raise e

## upar wala bhi shi hai 
### and in comparasin to darshan code i used de1_youtube_cleaned instead of db_youtube_cleaned database

import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        # Creating DF from content
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Extract required columns:
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e