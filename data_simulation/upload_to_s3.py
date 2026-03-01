# # data_simulation/upload_to_s3.py

# # how to run: python -m data_simulation.upload_to_s3

# import boto3
# import os
# from dotenv import load_dotenv

# load_dotenv()

# s3 = boto3.client('s3',
#     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
#     region_name=os.getenv('AWS_REGION')
# )

# bucket = os.getenv('S3_BUCKET_NAME')

# # Walk through output/raw/ and upload everything
# for root, dirs, files in os.walk('output/raw'):
#     for file in files:
#         local_path = os.path.join(root, file)
#         # Convert: output/raw/fact_orders/date=2022-02-01/data.csv
#         # To:      raw/fact_orders/date=2022-02-01/data.csv
#         s3_key = local_path.replace('output/', '').replace('\\', '/')
        
#         print(f'Uploading: {s3_key}')
#         s3.upload_file(local_path, bucket, s3_key)

# print('Upload complete!')

# data_simulation/upload_to_s3.py

# Usage:
#   python -m data_simulation.upload_to_s3                         # original backfill
#   python -m data_simulation.upload_to_s3 --dir output_extension  # extension backfill

# Performance improvements over original:
#   - Parallel uploads using ThreadPoolExecutor (16 workers)
#   - boto3 TransferConfig with multipart for large files
#   - Progress reporting every 100 files instead of every file
#   - Collects all files first, then uploads in parallel

import boto3
import os
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument(
    '--dir',
    default='output',
    help='Output directory to upload (default: output). Use output_extension for extension backfill.'
)
parser.add_argument(
    '--workers',
    type=int,
    default=16,
    help='Number of parallel upload threads (default: 16)'
)
args = parser.parse_args()

# TransferConfig: multipart upload for files > 8MB, 4 parallel parts per file
transfer_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,   # 8 MB
    multipart_chunksize=8 * 1024 * 1024,   # 8 MB chunks
    max_concurrency=4,
    use_threads=True
)

bucket    = os.getenv('S3_BUCKET_NAME')
source_dir = os.path.join(args.dir, 'raw')

print(f'Uploading from : {source_dir}')
print(f'Destination    : s3://{bucket}/raw/')
print(f'Parallel workers: {args.workers}')
print()

# ── Collect all files first ───────────────────────────────────
all_files = []
for root, dirs, files in os.walk(source_dir):
    for file in files:
        local_path = os.path.join(root, file)
        s3_key = local_path.replace(args.dir + os.sep, '').replace('\\', '/')
        all_files.append((local_path, s3_key))

total = len(all_files)
print(f'Found {total:,} files to upload')
print()

# ── Parallel upload ───────────────────────────────────────────
start_time  = time.time()
uploaded    = 0
failed      = []

def upload_file(args_tuple):
    local_path, s3_key = args_tuple
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id    =os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name          =os.getenv('AWS_REGION')
        )
        s3_client.upload_file(
            local_path, bucket, s3_key,
            Config=transfer_config
        )
        return s3_key, None
    except Exception as e:
        return s3_key, str(e)

with ThreadPoolExecutor(max_workers=args.workers) as executor:
    futures = {executor.submit(upload_file, f): f for f in all_files}
    for future in as_completed(futures):
        s3_key, error = future.result()
        if error:
            failed.append((s3_key, error))
        else:
            uploaded += 1

        # Progress every 100 files
        if uploaded % 100 == 0 or uploaded == total:
            elapsed = time.time() - start_time
            rate    = uploaded / elapsed if elapsed > 0 else 0
            eta     = (total - uploaded) / rate if rate > 0 else 0
            print(f'  {uploaded:>5}/{total} files | '
                  f'{rate:.0f} files/s | '
                  f'ETA: {eta:.0f}s')

elapsed = time.time() - start_time
print(f'\nUpload complete in {elapsed:.0f}s ({elapsed/60:.1f} min)')
print(f'  Uploaded : {uploaded:,} files')
if failed:
    print(f'  Failed   : {len(failed)} files')
    for s3_key, err in failed[:10]:
        print(f'    {s3_key}: {err}')
else:
    print(f'  Failed   : 0')