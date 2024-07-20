import boto3
import os
import pandas as pd

print('Loading function...')

s3 = boto3.client('s3')
prefix_list = {
        'secarch/': 'sec_arch_processed.csv.gz', 
        'blockdealarch/': 'block_deals_arch_processed.csv.gz', 
        'boardmeetings/': 'board_meetings_processed.csv.gz', 
        'bulkdealarch/': 'bulk_deals_arch_processed.csv.gz',
        'indexratios/': 'nse_ratios_processed.csv.gz', 
        'monthlyadvdec/': 'monthly_adv_declines_processed.csv.gz', 
        'shortsellarch/': 'short_selling_arch_processed.csv.gz'
        }
dest_bucket = 'niftyde-analytics'

def download_dir(prefix, local, bucket, client=s3):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)
        

def pandas_compress():
    sec_arch = pd.read_csv("/tmp/secarch/sec_arch_processed.csv")
    block_deals_arch = pd.read_csv("/tmp/blockdealarch/block_deals_arch_processed.csv")
    bulk_deals_arch = pd.read_csv("/tmp/bulkdealarch/bulk_deals_arch_processed.csv")
    monthly_adv_dec = pd.read_csv("/tmp/monthlyadvdec/monthly_adv_declines_processed.csv")
    nse_ratios = pd.read_csv("/tmp/indexratios/nse_ratios_processed.csv")
    short_selling_arch = pd.read_csv("/tmp/shortsellarch/short_selling_arch_processed.csv")
    board_meetings = pd.read_csv("/tmp/boardmeetings/board_meetings_processed.csv")


    sec_arch.to_csv("/tmp/secarch/sec_arch_processed.csv.gz", compression='gzip')
    block_deals_arch.to_csv("/tmp/blockdealarch/block_deals_arch_processed.csv.gz", compression='gzip')
    bulk_deals_arch.to_csv("/tmp/bulkdealarch/bulk_deals_arch_processed.csv.gz", compression='gzip')
    monthly_adv_dec.to_csv("/tmp/monthlyadvdec/monthly_adv_declines_processed.csv.gz", compression='gzip')
    nse_ratios.to_csv("/tmp/indexratios/nse_ratios_processed.csv.gz", compression='gzip')
    short_selling_arch.to_csv("/tmp/shortsellarch/short_selling_arch_processed.csv.gz", compression='gzip')
    board_meetings.to_csv("/tmp/boardmeetings/board_meetings_processed.csv.gz", compression='gzip')


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        for prefix, processed_file in prefix_list.items():
            download_dir(prefix, '/tmp/', bucket, client=s3)
            dirs = os.listdir(f"/tmp/{prefix}")
            print(f"Downloaded to : {dirs}")
            
        pandas_compress()
        # for (root,dirs,files) in os.walk('/tmp/', topdown=True):
        #     print(root, dirs,files)
        for prefix, processed_file in prefix_list.items():
            file_loc = f"/tmp/" + prefix + processed_file
            s3.upload_file(file_loc, dest_bucket, prefix+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            #s3.upload_file(f'/tmp/secarch/{os.listdir('/tmp/secarch/')[0]}', dest_bucket, prefix+os.listdir('/tmp/secarch/')[0], ExtraArgs={'ServerSideEncryption': "AES256"})
        print(f"File uploaded to {dest_bucket}...")

        # Uploading .ok for the lambda trigger for analytics bucket
        with open('/tmp/.ok', mode='a'): 
            pass
        s3.upload_file('/tmp/.ok', dest_bucket, '.ok', ExtraArgs={'ServerSideEncryption': "AES256"})
        # In the end, upload .ok file for the next lambda to run
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
              
