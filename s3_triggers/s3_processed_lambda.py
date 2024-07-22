import boto3
import os
import re
import pandas as pd

print('Loading function...')

s3 = boto3.client('s3')
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
        
def pandas_preprocess(key):
    
        if '_sec_archives_processed.csv' in key:
            sec_arch = pd.read_csv("/tmp/secarch/" + key)
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
                    
            sec_arch.to_csv(f"/tmp/secarch/{key.split('_processed')[0]}_analytics.csv", index=False)
            file_loc = f"/tmp/secarch/{key.split('_processed')[0]}_analytics.csv"
            processed_file = f"{key.split('_processed')[0]}_analytics.csv"
            s3.upload_file(file_loc, dest_bucket, 'secarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_short_selling_archives_processed.csv' in key:
            short_selling_arch = pd.read_csv("/tmp/shortsellarch/" + key)
            short_selling_arch = short_selling_arch.rename(columns={
                    'SS_NAME': 'STOCK_NAME', 
                    'SS_SYMBOL': 'SYMBOL',
                    'SS_DATE': 'TIMESTAMP',
                    'SS_QTY': 'QTY_SHORT_SOLD'
                    })
                    
            short_selling_arch.to_csv(f"/tmp/shortsellarch/{key.split('_processed')[0]}_analytics.csv", index=False)
            file_loc = f"/tmp/shortsellarch/{key.split('_processed')[0]}_analytics.csv"
            processed_file = f"{key.split('_processed')[0]}_analytics.csv"
            s3.upload_file(file_loc, dest_bucket, 'shortsellarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_nse_ratios_processed.csv' in key:
            nse_ratios = pd.read_csv("/tmp/indexratios/" + key)
            nse_ratios = nse_ratios.rename(columns={ 
                    'HistoricalDate': 'TIMESTAMP'
                    })
                    
            nse_ratios.to_csv(f"/tmp/indexratios/{key.split('_processed')[0]}_analytics.csv", index=False)
            file_loc = f"/tmp/indexratios/{key.split('_processed')[0]}_analytics.csv"
            processed_file = f"{key.split('_processed')[0]}_analytics.csv"
            s3.upload_file(file_loc, dest_bucket, 'indexratios/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})

            
        elif '_monthly_adv_declines_processed.csv' in key:
            monthly_adv_dec = pd.read_csv("/tmp/monthlyadvdec/" + key)
            monthly_adv_dec = monthly_adv_dec.rename(columns={
                    'ADM_MONTH_YEAR_STRING': 'TIMESTAMP', 
                    'ADM_ADVANCES': 'ADVANCES',
                    'ADM_ADV_DCLN_RATIO': 'ADV_DEC_RATIO',
                    'ADM_DECLINES': 'DECLINES'
                    })
                    
            monthly_adv_dec.to_csv(f"/tmp/monthlyadvdec/{key.split('_processed')[0]}_analytics.csv", index=False)
            file_loc = f"/tmp/monthlyadvdec/{key.split('_processed')[0]}_analytics.csv"
            processed_file = f"{key.split('_processed')[0]}_analytics.csv"
            s3.upload_file(file_loc, dest_bucket, 'monthlyadvdec/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})

            
        elif '_bulk_deals_archives_processed.csv' in key:
            bulk_deals_arch = pd.read_csv("/tmp/bulkdealarch/" + key)
            bulk_deals_arch = bulk_deals_arch.rename(columns={
                    'BD_DT_DATE': 'TIMESTAMP', 
                    'BD_SYMBOL': 'SYMBOL',
                    'BD_SCRIP_NAME': 'STOCK_NAME',
                    'BD_CLIENT_NAME': 'CLIENT_NAME',
                    'BD_BUY_SELL': 'BUY_SELL',
                    'BD_QTY_TRD': 'QTY_TRADED',
                    'BD_TP_WATP': 'TRADING_PRICE'
                    })
                    
            bulk_deals_arch.to_csv(f"/tmp/bulkdealarch/{key.split('_processed')[0]}_analytics.csv", index=False)
            file_loc = f"/tmp/bulkdealarch/{key.split('_processed')[0]}_analytics.csv"
            processed_file = f"{key.split('_processed')[0]}_analytics.csv"
            s3.upload_file(file_loc, dest_bucket, 'bulkdealarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_board_meetings_processed.csv' in key:
            board_meetings = pd.read_csv("/tmp/boardmeetings/" + key)
            board_meetings = board_meetings.rename(columns={
                    'bm_symbol': 'SYMBOL', 
                    'bm_date': 'TIMESTAMP',
                    'bm_purpose': 'PURPOSE',
                    'bm_desc': 'DESC',
                    'sm_indusrty': 'INDUSTRY',
                    'sm_name': 'COMPANY_NAME'
                    })
                    
            board_meetings.to_csv(f"/tmp/boardmeetings/{key.split('_processed')[0]}_analytics.csv", index=False)
            file_loc = f"/tmp/boardmeetings/{key.split('_processed')[0]}_analytics.csv"
            processed_file = f"{key.split('_processed')[0]}_analytics.csv"
            s3.upload_file(file_loc, dest_bucket, 'boardmeetings/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})
            
        elif '_block_deals_archives_processed.csv' in key:
            block_deals_arch = pd.read_csv("/tmp/blockdealarch/" + key)
            block_deals_arch = block_deals_arch.rename(columns={
                    'BD_DT_DATE': 'TIMESTAMP', 
                    'BD_SYMBOL': 'SYMBOL',
                    'BD_SCRIP_NAME': 'STOCK_NAME',
                    'BD_CLIENT_NAME': 'CLIENT_NAME',
                    'BD_BUY_SELL': 'BUY_SELL',
                    'BD_QTY_TRD': 'QTY_TRADED',
                    'BD_TP_WATP': 'TRADING_PRICE'
                    })
                    
            block_deals_arch.to_csv(f"/tmp/blockdealarch/{key.split('_processed')[0]}_analytics.csv", index=False)
            file_loc = f"/tmp/blockdealarch/{key.split('_processed')[0]}_analytics.csv"
            processed_file = f"{key.split('_processed')[0]}_analytics.csv"
            s3.upload_file(file_loc, dest_bucket, 'blockdealarch/'+processed_file, ExtraArgs={'ServerSideEncryption': "AES256"})

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key'].split("/")[-1]
    
    try:
        
        if '_sec_archives_processed.csv' in key:
            download_dir('secarch/', '/tmp/', bucket, client=s3)
            
        elif '_short_selling_archives_processed.csv' in key:
            download_dir('shortsellarch/', '/tmp/', bucket, client=s3)
            
        elif '_nse_ratios_processed.csv' in key:
            download_dir('indexratios/', '/tmp/', bucket, client=s3)
            
        elif '_monthly_adv_declines_processed.csv' in key:
            download_dir('monthlyadvdec/', '/tmp/', bucket, client=s3)
            
        elif '_bulk_deals_archives_processed.csv' in key:
            download_dir('bulkdealarch/', '/tmp/', bucket, client=s3)
            
        elif '_board_meetings_processed.csv' in key:
            download_dir('boardmeetings/', '/tmp/', bucket, client=s3)
            
        elif '_block_deals_archives_processed.csv' in key:
            download_dir('blockdealarch/', '/tmp/', bucket, client=s3)
            
        pandas_preprocess(key)
        # for (root,dirs,files) in os.walk('/tmp/', topdown=True):
        #     print(root, dirs,files)
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
              
