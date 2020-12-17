import subprocess
import re
import argparse
import sys
import boto3
import os
from botocore.exceptions import ClientError


parser = argparse.ArgumentParser(description='Sync archive files to aws and remove')
parser.add_argument('--days', dest='days', default=7, help='The total of number of days to keep local')

args = parser.parse_args()

days_to_keep = args.days

out = subprocess.Popen([
        'find',
        '/logs/archive/logs',
        '-type',
        'f',
        '-mtime',
        f'+{days_to_keep}',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
)

stdout,stderr = out.communicate()

if stdout == b'':
    print("No files to process")
    sys.exit(0)

s3 = boto3.client('s3')

for filename in stdout.rstrip().decode('utf8').split("\n"):

    local_size = os.path.getsize(filename)
    aws_full_path = re.sub(r"/logs/archive/logs",r"s3://sl-eng-logs/logs",filename)
    aws_key_path = re.sub(r"/logs/archive/",r"",filename)
    #print(f"{filename} => {aws_full_path}")
    #print(f"Looking at Bucket(sl-eng-logs) and Key({aws_key_path})")
    key = s3.head_object(Bucket='sl-eng-logs', Key=aws_key_path)
    remote_size = int(key['ContentLength'])

    if key is None:
        print(f"Missing from AWS, uploading...")
    else:
        if(local_size == remote_size):
            print(f"OK: File already in AWS, can delete {aws_full_path} ({local_size} == {remote_size})")
            os.remove(filename)
            pass
        else:
            print(f"File in AWS, but wrong size")
