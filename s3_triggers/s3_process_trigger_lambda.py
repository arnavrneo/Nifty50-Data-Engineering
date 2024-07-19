import boto3
import os
import pandas as pd

print('Loading function...')

s3 = boto3.client('s3')
prefix_list = {
        'secarch/': 'sec_arch_processed.csv', 
        'blockdealarch/': 'block_deals_arch_processed.csv', 
        'boardmeetings/': 'board_meetings_processed.csv', 
        'bulkdealarch/': 'bulk_deals_arch_processed.csv',
        'indexratios/': 'nse_ratios_processed.csv', 
        'monthlyadvdec/': 'monthly_adv_declines_processed.csv', 
        'shortsellarch/': 'short_selling_arch_processed.csv'
        }
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
        

def pandas_preprocess():
    sec_arch = pd.read_csv("/tmp/secarch/sec_arch.csv")
    block_deals_arch = pd.read_csv("/tmp/blockdealarch/block_deals_archives.csv")
    bulk_deals_arch = pd.read_csv("/tmp/bulkdealarch/bulk_deals_archives.csv")
    monthly_adv_dec = pd.read_csv("/tmp/monthlyadvdec/monthly_adv_declines.csv")
    nse_ratios = pd.read_csv("/tmp/indexratios/nse_ratios.csv")
    short_selling_arch = pd.read_csv("/tmp/shortsellarch/short_selling_archives.csv")
    board_meetings = pd.read_csv("/tmp/boardmeetings/board_meetings.csv")

    sec_arch = sec_arch.drop(["_id", "__v", "CH_MARKET_TYPE", "CH_SERIES", "TIMESTAMP", "CH_ISIN", "COP_DELIV_QTY", "COP_DELIV_PERC", "VWAP", "mTIMESTAMP", "createdAt", "updatedAt"], axis=1)
    block_deals_arch = block_deals_arch.drop(["_id", "createdAt", "updatedAt", "mTIMESTAMP", "__v", "TIMESTAMP"], axis=1)
    bulk_deals_arch = bulk_deals_arch.drop(['_id', 'BD_REMARKS', 'TIMESTAMP', 'createdAt', 'updatedAt', '__v', 'mTIMESTAMP'], axis=1)
    monthly_adv_dec = monthly_adv_dec.drop(['_id', 'TIMESTAMP', 'ADM_MONTH', 'ADM_REC_TY'], axis=1)
    nse_ratios = nse_ratios.drop(['RequestNumber', 'Index Name'], axis=1)
    short_selling_arch = short_selling_arch.drop(['_id', 'TIMESTAMP', 'createdAt', 'updatedAt', 'mTIMESTAMP', '__v'], axis=1)
    board_meetings = board_meetings.drop(['sm_isin', 'attachment', 'diff', 'sysTime', 'bm_timestamp'], axis=1)


    # change the datatypes
    sec_arch['CH_TIMESTAMP'] = pd.to_datetime(sec_arch['CH_TIMESTAMP'], format='%Y-%m-%d')
    block_deals_arch['BD_DT_DATE'] = pd.to_datetime(block_deals_arch['BD_DT_DATE'], format='%d-%b-%Y')
    bulk_deals_arch['BD_DT_DATE'] = pd.to_datetime(bulk_deals_arch['BD_DT_DATE'], format='%d-%b-%Y')
    monthly_adv_dec['ADM_MONTH_YEAR_STRING'] = pd.to_datetime(monthly_adv_dec['ADM_MONTH_YEAR_STRING'])
    nse_ratios['HistoricalDate'] = pd.to_datetime(nse_ratios['HistoricalDate'])
    short_selling_arch['SS_DATE'] = pd.to_datetime(short_selling_arch['SS_DATE'])
    board_meetings['bm_date'] = pd.to_datetime(board_meetings['bm_date'])

    sec_arch.to_csv("/tmp/secarch/sec_arch_processed.csv", index=False)
    block_deals_arch.to_csv("/tmp/blockdealarch/block_deals_arch_processed.csv", index=False)
    bulk_deals_arch.to_csv("/tmp/bulkdealarch/bulk_deals_arch_processed.csv", index=False)
    monthly_adv_dec.to_csv("/tmp/monthlyadvdec/monthly_adv_declines_processed.csv", index=False)
    nse_ratios.to_csv("/tmp/indexratios/nse_ratios_processed.csv", index=False)
    short_selling_arch.to_csv("/tmp/shortsellarch/short_selling_arch_processed.csv", index=False)
    board_meetings.to_csv("/tmp/boardmeetings/board_meetings_processed.csv", index=False)


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        for prefix, processed_file in prefix_list.items():
            download_dir(prefix, '/tmp/', bucket, client=s3)
            dirs = os.listdir(f"/tmp/{prefix}")
            print(f"Downloaded to : {dirs}")
            
        pandas_preprocess()
        # for (root,dirs,files) in os.walk('/tmp/', topdown=True):
        #     print(root, dirs,files)
        for prefix, processed_file in prefix_list.items():
            file_loc = f"/tmp/" + prefix + processed_file
            s3.upload_file(file_loc, dest_bucket, prefix+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            #s3.upload_file(f'/tmp/secarch/{os.listdir('/tmp/secarch/')[0]}', dest_bucket, prefix+os.listdir('/tmp/secarch/')[0], ExtraArgs={'ServerSideEncryption': "AES256"})
        print(f"File uploaded to {dest_bucket}...")
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
              
