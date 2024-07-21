import boto3
import os
import pandas as pd

print('Loading function...')

s3 = boto3.client('s3')
prefix_list = {
        'secarch/': 'sec_archives_analytics.csv', 
        'blockdealarch/': 'block_deals_archives_analytics.csv', 
        'boardmeetings/': 'board_meetings_analytics.csv', 
        'bulkdealarch/': 'bulk_deals_archives_analytics.csv',
        'indexratios/': 'nse_ratios_analytics.csv', 
        'monthlyadvdec/': 'monthly_adv_declines_analytics.csv', 
        'shortsellarch/': 'short_selling_archives_analytics.csv'
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
    sec_arch = pd.read_csv("/tmp/secarch/sec_archives_processed.csv")
    block_deals_arch = pd.read_csv("/tmp/blockdealarch/block_deals_archives_processed.csv")
    bulk_deals_arch = pd.read_csv("/tmp/bulkdealarch/bulk_deals_archives_processed.csv")
    monthly_adv_dec = pd.read_csv("/tmp/monthlyadvdec/monthly_adv_declines_processed.csv")
    nse_ratios = pd.read_csv("/tmp/indexratios/nse_ratios_processed.csv")
    short_selling_arch = pd.read_csv("/tmp/shortsellarch/short_selling_archives_processed.csv")
    board_meetings = pd.read_csv("/tmp/boardmeetings/board_meetings_processed.csv")
    
    # adv processing for analytics
    sec_arch['CH_TOT_TRADED_VAL'] = sec_arch["CH_TOT_TRADED_VAL"].apply(lambda x: '{:.0f}'.format(x))
    sec_arch['CH_TOT_TRADED_VAL'] = sec_arch['CH_TOT_TRADED_VAL'].astype(int)
    sec_arch['CH_TOT_TRADED_VAL'] = sec_arch['CH_TOT_TRADED_VAL'] / 1e7
    sec_arch = sec_arch.rename(columns={
            'CH_SYMBOL': 'SYMBOL', 
            'CH_TIMESTAMP': 'TIMESTAMP',
            'CH_TRADE_HIGH_PRICE': 'TRADE_HIGH_PRICE',
            'CH_TRADE_LOW_PRICE': 'TRADE_LOW_PRICE',
            'CH_OPENING_PRICE': 'TRADE_OPEN_PRICE',
            'CH_CLOSING_PRICE': 'TRADE_CLOSE_PRICE',
            'CH_LAST_TRADED_PRICE': 'LAST_TRADED_PRICE',
            'CH_PREVIOUS_CLS_PRICE': 'PREVIOUS_CLOSING_PRICE',
            'CH_TOT_TRADED_QTY': 'TOTAL_TRADING_QUANT',
            'CH_TOT_TRADED_VAL': 'TOTAL_TRADED_VAL_IN_CRORES',
            'CH_52WEEK_HIGH_PRICE': 'HIGH_52WEEK',
            'CH_52WEEK_LOW_PRICE': 'LOW_52WEEK'
            })
            
    block_deals_arch = block_deals_arch.rename(columns={
            'BD_DT_DATE': 'TIMESTAMP', 
            'BD_SYMBOL': 'SYMBOL',
            'BD_SCRIP_NAME': 'STOCK_NAME',
            'BD_CLIENT_NAME': 'CLIENT_NAME',
            'BD_BUY_SELL': 'BUY_SELL',
            'BD_QTY_TRD': 'QTY_TRADED',
            'BD_TP_WATP': 'TRADING_PRICE'
            })
            
    bulk_deals_arch = bulk_deals_arch.rename(columns={
            'BD_DT_DATE': 'TIMESTAMP', 
            'BD_SYMBOL': 'SYMBOL',
            'BD_SCRIP_NAME': 'STOCK_NAME',
            'BD_CLIENT_NAME': 'CLIENT_NAME',
            'BD_BUY_SELL': 'BUY_SELL',
            'BD_QTY_TRD': 'QTY_TRADED',
            'BD_TP_WATP': 'TRADING_PRICE'
            })
            
    monthly_adv_dec = monthly_adv_dec.rename(columns={
            'ADM_MONTH_YEAR_STRING': 'TIMESTAMP', 
            'ADM_ADVANCES': 'ADVANCES',
            'ADM_ADV_DCLN_RATIO': 'ADV_DEC_RATIO',
            'ADM_DECLINES': 'DECLINES'
            })
            
    nse_ratios = nse_ratios.rename(columns={ 
            'HistoricalDate': 'TIMESTAMP'
            })
            
    short_selling_arch = short_selling_arch.rename(columns={
            'SS_NAME': 'STOCK_NAME', 
            'SS_SYMBOL': 'SYMBOL',
            'SS_DATE': 'TIMESTAMP',
            'SS_QTY': 'QTY_SHORT_SOLD'
            })
            
    board_meetings = board_meetings.rename(columns={
            'bm_symbol': 'SYMBOL', 
            'bm_date': 'TIMESTAMP',
            'bm_purpose': 'PURPOSE',
            'bm_desc': 'DESC',
            'sm_indusrty': 'INDUSTRY',
            'sm_name': 'COMPANY_NAME'
            })
    

    sec_arch.to_csv("/tmp/secarch/sec_archives_analytics.csv", index=False)
    block_deals_arch.to_csv("/tmp/blockdealarch/block_deals_archives_analytics.csv", index=False)
    bulk_deals_arch.to_csv("/tmp/bulkdealarch/bulk_deals_archives_analytics.csv", index=False)
    monthly_adv_dec.to_csv("/tmp/monthlyadvdec/monthly_adv_declines_analytics.csv", index=False)
    nse_ratios.to_csv("/tmp/indexratios/nse_ratios_analytics.csv", index=False)
    short_selling_arch.to_csv("/tmp/shortsellarch/short_selling_archives_analytics.csv", index=False)
    board_meetings.to_csv("/tmp/boardmeetings/board_meetings_analytics.csv", index=False)


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
        # with open('/tmp/.ok', mode='a'): 
        #     pass
        # s3.upload_file('/tmp/.ok', dest_bucket, '.ok', ExtraArgs={'ServerSideEncryption': "AES256"})
        # In the end, upload .ok file for the next lambda to run
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
              
