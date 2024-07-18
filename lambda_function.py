import json
import boto3
from datetime import datetime
import requests as r

def lambda_handler(event, context): 
  bucket_name = 'niftyde-raw' 
  file_name = 'file_name.txt' 
  file_content = 'This is the content of the file.' 

  # s3 = boto3.client('s3') 
  # s3.put_object(Body=file_content, Bucket=bucket_name, Key=file_name)
  now = datetime.now()
  
  
  current_time = now.strftime("%H:%M:%S")
  print("Current Time =", current_time, "and layer works perfectly!!!=====")
  return { 
      'statusCode': 200, 
      'body': 'File uploaded successfully.' 
  }
