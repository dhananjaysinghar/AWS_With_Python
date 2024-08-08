import pandas as pd
import boto3
from io import BytesIO
import math
from datetime import datetime, timedelta

def getDate(dateformat):
    return (datetime.now() - timedelta(1)).strftime(dateformat)

def delete_existing_s3_directory(s3_client, bucket_name, s3_key):
    # List all objects with the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_key)
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        
        # Delete the listed objects
        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        print(f"Deleted existing directory: s3://{bucket_name}/{s3_key}, count:{response['KeyCount']}")


def upload_dataframe_in_chunks(dataframe, bucket_name, s3_key,  base_file_name, aws_access_key_id, aws_secret_access_key, chunk_size=3):
    # Create a boto3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    delete_existing_s3_directory(s3_client, bucket_name, s3_key)
    # Calculate the number of chunks needed
    number_of_chunks = math.ceil(len(dataframe) / chunk_size)
    for i in range(number_of_chunks):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size
        chunk_df = dataframe.iloc[start_index:end_index]
        print("chunk_df size :", len(chunk_df))

        # Convert the DataFrame chunk to a Parquet file in memory
        parquet_buffer = BytesIO()
        chunk_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Define a unique key for each chunk
        s3_file_key = f"{s3_key}{base_file_name}_part_{i}.parquet"

        # Upload the chunk to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=parquet_buffer)
        print(f"Chunk {i} uploaded to s3://{bucket_name}/{s3_file_key}")

if __name__ == "__main__":
    # Sample DataFrame
    data = {
        'column1': range(11),  # Large dataset
        'column2': ['A']*11
    }
    df = pd.DataFrame(data)

    # AWS S3 configuration
    bucket_name = '<bucket_name>'
    aws_access_key_id = '<aws_access_key_id>'
    aws_secret_access_key = '<aws_secret_access_key>'

    bucket_dir = f'period={getDate('%Y-%m-%d')}/'
    s3_key = f'data/samples/{bucket_dir}'
    base_file_name = f'test-{getDate('%H%M%S')}'

    # Upload DataFrame to S3 as Parquet
    upload_dataframe_in_chunks(df, bucket_name, s3_key, base_file_name, aws_access_key_id, aws_secret_access_key, 2)
