USE ROLE accountadmin;

-- Creating external stage
CREATE or REPLACE STORAGE INTEGRATION aws_stage_data
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::211125565587:role/snowflake-aws-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://niftyde-analytics/');

-- Describe the integration object
-- Check Row 5 and Row 7
DESC INTEGRATION aws_stage_data;

-- Grant usage access to Sysadmin Role ---
GRANT usage ON INTEGRATION aws_stage_data TO ROLE sysadmin;

-- Create a file format  ---
USE DATABASE nifty50db;
USE SCHEMA dev;
CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER =1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';


-- Create stage for each table
CREATE or REPLACE STAGE stg_block_deals
storage_integration = aws_stage_data
url = 's3://niftyde-analytics/blockdealarch/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_board_meetings
storage_integration = aws_stage_data
url = 's3://niftyde-analytics/boardmeetings/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_bulk_deals
storage_integration = aws_stage_data
url = 's3://niftyde-analytics/bulkdealarch/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_monthly_adv_dec
storage_integration = aws_stage_data
url = 's3://niftyde-analytics/monthlyadvdec/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_nse_ratios
storage_integration = aws_stage_data
url = 's3://niftyde-analytics/indexratios/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_sec_archives
storage_integration = aws_stage_data
url = 's3://niftyde-analytics/secarch/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_short_sell_arch
storage_integration = aws_stage_data
url = 's3://niftyde-analytics/shortsellarch/'
file_format = csv_load_format;

-- List the data
LIST@stg_bulk_deals;
LIST@stg_monthly_adv_dec;