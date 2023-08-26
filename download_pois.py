#Import Libraries
import boto3
import json
import os
import argparse

# Load arguments
parser = argparse.ArgumentParser(description="Main module that runs segmentation workflow on folder")

# Add positional arguments
parser.add_argument('access', help='AWS Access Key', type=str)
parser.add_argument('secret', help='AWS Secret Key', type=str)
parser.add_argument('bucket', help='AWS Bucket', type=str) 
parser.add_argument('key', help='Filtering Key', type=str)   
parser.add_argument('output', help='Filepath to output folder', type=str) 
# parser.add_argument('secret', default=1.0, help='Image segmentation threshold value', type=float)

args = parser.parse_args()

def main():
    s3 = boto3.client('s3', aws_access_key_id=args.access, aws_secret_access_key=args.secret)
    response = s3.list_objects(Bucket=args.bucket)

    if not os.path.exists(args.output):
        os.makedirs(args.output)

    for key in response['Contents']:
        if args.key in key['Key']:
            s3.download_file(args.bucket, key['Key'], f"{args.output}/{key['Key'].split('-')[-1]}")
            print(f"Downloaded: {key['Key'].split('-')[-1]}")


if __name__ == "__main__":
    main()
