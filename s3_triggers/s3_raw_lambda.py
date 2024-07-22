import boto3
import os
import re
import pandas as pd

print('Loading function...')

s3 = boto3.client('s3')
dest_bucket = 'niftyde-processed'

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
        

def pandas_preprocess(key):
    
        if '_sec_archives.csv' in key:
            sec_arch = pd.read_csv("/tmp/secarch/" + key)
            sec_arch["CA"] = '3'
            sec_arch = sec_arch.drop(["_id", "__v", "CH_MARKET_TYPE", "CH_SERIES", "TIMESTAMP", "CH_ISIN", "COP_DELIV_QTY", "COP_DELIV_PERC", "VWAP", "mTIMESTAMP", "createdAt", "updatedAt", "CA", "CH_TOTAL_TRADES"], axis=1)
            sec_arch['CH_TIMESTAMP'] = pd.to_datetime(sec_arch['CH_TIMESTAMP'], format='%Y-%m-%d')
            sec_arch.to_csv(f"/tmp/secarch/{key.split('.')[0]}_processed.csv", index=False)
            
            file_loc = f"/tmp/secarch/{key.split('.')[0]}_processed.csv"
            processed_file = f"{key.split('.')[0]}_processed.csv"
            s3.upload_file(file_loc, dest_bucket, 'secarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_short_selling_archives.csv' in key:
            short_selling_arch = pd.read_csv("/tmp/shortsellarch/" + key)
            short_selling_arch = short_selling_arch.drop(['_id', 'TIMESTAMP', 'createdAt', 'updatedAt', 'mTIMESTAMP', '__v'], axis=1)
            short_selling_arch['SS_DATE'] = pd.to_datetime(short_selling_arch['SS_DATE'])
            short_selling_arch.to_csv(f"/tmp/shortsellarch/{key.split('.')[0]}_processed.csv", index=False)

            file_loc = f"/tmp/shortsellarch/{key.split('.')[0]}_processed.csv"
            processed_file = f"{key.split('.')[0]}_processed.csv"
            s3.upload_file(file_loc, dest_bucket, 'shortsellarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_nse_ratios.csv' in key:
            nse_ratios = pd.read_csv("/tmp/indexratios/" + key)
            nse_ratios = nse_ratios.drop(['RequestNumber', 'Index Name'], axis=1)
            nse_ratios['HistoricalDate'] = pd.to_datetime(nse_ratios['HistoricalDate'])
            nse_ratios.to_csv(f"/tmp/indexratios/{key.split('.')[0]}_processed.csv", index=False)
            
            file_loc = f"/tmp/indexratios/{key.split('.')[0]}_processed.csv"
            processed_file = f"{key.split('.')[0]}_processed.csv"
            s3.upload_file(file_loc, dest_bucket, 'indexratios/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})

            
        elif '_monthly_adv_declines.csv' in key:
            monthly_adv_dec = pd.read_csv("/tmp/monthlyadvdec/" + key)
            monthly_adv_dec = monthly_adv_dec.drop(['_id', 'TIMESTAMP', 'ADM_MONTH', 'ADM_REC_TY'], axis=1)
            monthly_adv_dec['ADM_MONTH_YEAR_STRING'] = pd.to_datetime(monthly_adv_dec['ADM_MONTH_YEAR_STRING'])
            monthly_adv_dec.to_csv(f"/tmp/monthlyadvdec/{key.split('.')[0]}_processed.csv", index=False)
            
            file_loc = f"/tmp/monthlyadvdec/{key.split('.')[0]}_processed.csv"
            processed_file = f"{key.split('.')[0]}_processed.csv"
            s3.upload_file(file_loc, dest_bucket, 'monthlyadvdec/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})

            
        elif '_bulk_deals_archives.csv' in key:
            bulk_deals_arch = pd.read_csv("/tmp/bulkdealarch/" + key)
            bulk_deals_arch = bulk_deals_arch.drop(['_id', 'BD_REMARKS', 'TIMESTAMP', 'createdAt', 'updatedAt', '__v', 'mTIMESTAMP'], axis=1)
            bulk_deals_arch['BD_DT_DATE'] = pd.to_datetime(bulk_deals_arch['BD_DT_DATE'], format='%d-%b-%Y')
            bulk_deals_arch.to_csv(f"/tmp/bulkdealarch/{key.split('.')[0]}_processed.csv", index=False)
            
            file_loc = f"/tmp/bulkdealarch/{key.split('.')[0]}_processed.csv"
            processed_file = f"{key.split('.')[0]}_processed.csv"
            s3.upload_file(file_loc, dest_bucket, 'bulkdealarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_board_meetings.csv' in key:
            board_meetings = pd.read_csv("/tmp/boardmeetings/" + key)
            board_meetings = board_meetings.drop(['sm_isin', 'attachment', 'diff', 'sysTime', 'bm_timestamp'], axis=1)
            board_meetings['bm_date'] = pd.to_datetime(board_meetings['bm_date'])
            board_meetings.to_csv(f"/tmp/boardmeetings/{key.split('.')[0]}_processed.csv", index=False)
    
            file_loc = f"/tmp/boardmeetings/{key.split('.')[0]}_processed.csv"
            processed_file = f"{key.split('.')[0]}_processed.csv"
            s3.upload_file(file_loc, dest_bucket, 'boardmeetings/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_block_deals_archives.csv' in key:
            block_deals_arch = pd.read_csv("/tmp/blockdealarch/" + key)
            block_deals_arch = block_deals_arch.drop(["_id", "createdAt", "updatedAt", "mTIMESTAMP", "__v", "TIMESTAMP"], axis=1)
            block_deals_arch['BD_DT_DATE'] = pd.to_datetime(block_deals_arch['BD_DT_DATE'], format='%d-%b-%Y')
            block_deals_arch.to_csv(f"/tmp/blockdealarch/{key.split('.')[0]}_processed.csv", index=False)
            
            file_loc = f"/tmp/blockdealarch/{key.split('.')[0]}_processed.csv"
            processed_file = f"{key.split('.')[0]}_processed.csv"
            s3.upload_file(file_loc, dest_bucket, 'blockdealarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key'].split("/")[1]
    print(f"Bucket: {bucket}\nKey: {key}")

        
    try:
     
        if '_sec_archives.csv' in key:
            download_dir('secarch/', '/tmp/', bucket, client=s3)
            
        elif '_short_selling_archives.csv' in key:
            download_dir('shortsellarch/', '/tmp/', bucket, client=s3)
            
        elif '_nse_ratios.csv' in key:
            download_dir('indexratios/', '/tmp/', bucket, client=s3)
            
        elif '_monthly_adv_declines.csv' in key:
            download_dir('monthlyadvdec/', '/tmp/', bucket, client=s3)
            
        elif '_bulk_deals_archives.csv' in key:
            download_dir('bulkdealarch/', '/tmp/', bucket, client=s3)
            
        elif '_board_meetings.csv' in key:
            download_dir('boardmeetings/', '/tmp/', bucket, client=s3)
            
        elif '_block_deals_archives.csv' in key:
            download_dir('blockdealarch/', '/tmp/', bucket, client=s3)
            
        pandas_preprocess(key)
        # for (root,dirs,files) in os.walk('/tmp/', topdown=True):
        #     print(root, dirs,files)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
              

              
