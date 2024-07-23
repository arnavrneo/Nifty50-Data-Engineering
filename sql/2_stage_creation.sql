USE ROLE accountadmin;

-- Creating external stage
CREATE or REPLACE STORAGE INTEGRATION aws_stage_data
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<REPLACE-AWS-SNOWFLAKE-ROLE-HERE'
    STORAGE_ALLOWED_LOCATIONS = ('<S3-ANALYTICS-BUCKET-URL>');

-- Describe the integration object
-- Check Row 5 and Row 7
DESC INTEGRATION aws_stage_data;

-- Grant usage access to Sysadmin Role ---
GRANT usage ON INTEGRATION aws_stage_data TO ROLE sysadmin;

-- Create a file format  ---
USE DATABASE nifty50db;
USE SCHEMA dev;
CREATE or REPLACE FILE FORMAT csv_load_format
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
url = '<S3-ANALYTICS-BUCKET-URL>/blockdealarch/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_board_meetings
storage_integration = aws_stage_data
url = '<S3-ANALYTICS-BUCKET-URL>/boardmeetings/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_bulk_deals
storage_integration = aws_stage_data
url = '<S3-ANALYTICS-BUCKET-URL>/bulkdealarch/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_monthly_adv_dec
storage_integration = aws_stage_data
url = '<S3-ANALYTICS-BUCKET-URL>/monthlyadvdec/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_nse_ratios
storage_integration = aws_stage_data
url = '<S3-ANALYTICS-BUCKET-URL>/indexratios/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_sec_archives
storage_integration = aws_stage_data
url = '<S3-ANALYTICS-BUCKET-URL>/secarch/'
file_format = csv_load_format;

CREATE or REPLACE STAGE stg_short_sell_arch
storage_integration = aws_stage_data
url = '<S3-ANALYTICS-BUCKET-URL>/shortsellarch/'
file_format = csv_load_format;

-- List the data
LIST@stg_bulk_deals;
LIST@stg_block_deals;
LIST@stg_monthly_adv_dec;


-- Creating a pipeline

CREATE or REPLACE PIPE block_deals_pipe AUTO_INGEST=true AS
COPY INTO BLOCK_DEALS FROM @stg_block_deals ON_ERROR = continue;

CREATE or REPLACE PIPE board_meetings_pipe AUTO_INGEST=true AS
COPY INTO BOARD_MEETINGS FROM @stg_board_meetings ON_ERROR = continue;

CREATE or REPLACE PIPE bulk_deals_pipe AUTO_INGEST=true AS
COPY INTO BULK_DEALS FROM @stg_bulk_deals ON_ERROR = continue;

CREATE or REPLACE PIPE monthly_adv_dec_pipe AUTO_INGEST=true AS
COPY INTO MONTHLY_ADV_DEC FROM @stg_monthly_adv_dec ON_ERROR = continue;

CREATE or REPLACE PIPE nse_ratios_pipe AUTO_INGEST=true AS
COPY INTO NSE_RATIOS FROM @stg_nse_ratios ON_ERROR = continue;

CREATE or REPLACE PIPE sec_archives_pipe AUTO_INGEST=true AS
COPY INTO SEC_ARCHIVES FROM @stg_sec_archives ON_ERROR = continue;

CREATE or REPLACE PIPE short_sell_arch_pipe AUTO_INGEST=true AS
COPY INTO SHORT_SELL_ARCH FROM @stg_short_sell_arch ON_ERROR = continue;

SHOW pipes;

-- truncate table block_deals;

-- SELECT * FROM block_deals;
