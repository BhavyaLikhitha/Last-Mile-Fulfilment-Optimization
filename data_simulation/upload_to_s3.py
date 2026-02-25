# data_simulation/upload_to_s3.py

# how to run: python -m data_simulation.upload_to_s3

import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

bucket = os.getenv('S3_BUCKET_NAME')

# Walk through output/raw/ and upload everything
for root, dirs, files in os.walk('output/raw'):
    for file in files:
        local_path = os.path.join(root, file)
        # Convert: output/raw/fact_orders/date=2022-02-01/data.csv
        # To:      raw/fact_orders/date=2022-02-01/data.csv
        s3_key = local_path.replace('output/', '').replace('\\', '/')
        
        print(f'Uploading: {s3_key}')
        s3.upload_file(local_path, bucket, s3_key)

print('Upload complete!')