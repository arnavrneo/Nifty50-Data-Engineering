 USE ROLE sysadmin;

 -- Creating the nifty50 database
 CREATE DATABASE nifty50DB;

 -- Creating two schemas: dev and prod
 CREATE SCHEMA dev;
 CREATE SCHEMA prod;

 -- Using DB
 USE DATABASE nifty50DB;
 
 -- Using dev schema
 USE SCHEMA dev;

 -- Creating dev schema tables
 CREATE or REPLACE TABLE SEC_ARCHIVES (
    CH_SYMBOL	varchar(30) NOT NULL,
    CH_TIMESTAMP	date NOT NULL PRIMARY KEY,
    CH_TRADE_HIGH_PRICE	float,
    CH_TRADE_LOW_PRICE	float,
    CH_OPENING_PRICE	float,
    CH_CLOSING_PRICE	float,
    CH_LAST_TRADED_PRICE	float,
    CH_PREVIOUS_CLS_PRICE	float,
    CH_TOT_TRADED_QTY	integer,
    CH_TOT_TRADED_VAL	float,
    CH_52WEEK_HIGH_PRICE	float,
    CH_52WEEK_LOW_PRICE	float,
    CH_TOTAL_TRADES integer
 );

 CREATE or REPLACE TABLE BLOCK_DEALS (
    BD_DT_DATE	date NOT NULL PRIMARY KEY,
    BD_SYMBOL	varchar(30) NOT NULL,
    BD_SCRIP_NAME	varchar(30),
    BD_CLIENT_NAME	varchar(30),
    BD_BUY_SELL	varchar(30),
    BD_QTY_TRD	integer,
    BD_TP_WATP  float
 );

 CREATE or REPLACE TABLE BULK_DEALS (
    BD_DT_DATE	date NOT NULL PRIMARY KEY,
    BD_SYMBOL	varchar(30) NOT NULL,
    BD_SCRIP_NAME	varchar(30),
    BD_CLIENT_NAME	varchar(30),
    BD_BUY_SELL	varchar(30),
    BD_QTY_TRD	integer,
    BD_TP_WATP  float
 );

 CREATE or REPLACE TABLE MONTHLY_ADV_DEC (
    ADM_MONTH_YEAR_STRING	date NOT NULL PRIMARY KEY,
    ADM_ADVANCES	float,
    ADM_ADV_DCLN_RATIO	float,
    ADM_DECLINES    float
 );

 CREATE or REPLACE TABLE NSE_RATIOS (
    INDEX_NAME varchar(30) NOT NULL,	
    HistoricalDate	date NOT NULL PRIMARY KEY,
    OPEN	float,
    HIGH	float,
    LOW     float,	
    CLOSE   float
 );

 CREATE or REPLACE TABLE SHORT_SELL_ARCH (
    SS_NAME varchar(30) NOT NULL,	
    SS_SYMBOL	varchar(30),
    SS_DATE	date NOT NULL PRIMARY KEY,
    SS_QTY float
 );

 CREATE or REPLACE TABLE BOARD_MEETINGS (
    bm_symbol   varchar(30) NOT NULL,	
    bm_date	    date NOT NULL PRIMARY KEY,
    bm_purpose	varchar(255),
    bm_desc	    varchar(255),
    sm_indusrty	varchar(50),
    sm_name     varchar(255)
 );