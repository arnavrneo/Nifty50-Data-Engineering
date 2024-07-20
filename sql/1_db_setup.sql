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
    SYMBOL	varchar(255) NOT NULL,
    TIMESTAMP	date NOT NULL PRIMARY KEY,
    TRADE_HIGH_PRICE	float,
    TRADE_LOW_PRICE	float,
    TRADE_OPEN_PRICE	float,
    TRADE_CLOSE_PRICE	float,
    LAST_TRADED_PRICE	float,
    PREVIOUS_CLOSING_PRICE	float,
    TOTAL_TRADING_QUANT	integer,
    TOTAL_TRADED_VAL_IN_CRORES	float,
    HIGH_52WEEK	float,
    LOW_52WEEK	float
 );

 CREATE or REPLACE TABLE BLOCK_DEALS (
    TIMESTAMP	date NOT NULL PRIMARY KEY,
    SYMBOL	varchar(255) NOT NULL,
    STOCK_NAME	varchar(255),
    CLIENT_NAME	varchar(255),
    BUY_SELL	varchar(255),
    QTY_TRADED	integer,
    TRADING_PRICE  float
 );

 CREATE or REPLACE TABLE BULK_DEALS (
    TIMESTAMP	date NOT NULL PRIMARY KEY,
    SYMBOL	varchar(255) NOT NULL,
    STOCK_NAME	varchar(255),
    CLIENT_NAME	varchar(255),
    BUY_SELL	varchar(255),
    QTY_TRADED	integer,
    TRADING_PRICE  float
 );

 CREATE or REPLACE TABLE MONTHLY_ADV_DEC (
    TIMESTAMP	date NOT NULL PRIMARY KEY,
    ADVANCES	float,
    ADV_DEC_RATIO	float,
    DECLINES    float
 );

 CREATE or REPLACE TABLE NSE_RATIOS (
    INDEX_NAME varchar(255) NOT NULL,	
    TIMESTAMP	date NOT NULL PRIMARY KEY,
    OPEN	float,
    HIGH	float,
    LOW     float,	
    CLOSE   float
 );

 CREATE or REPLACE TABLE SHORT_SELL_ARCH (
    STOCK_NAME varchar(255) NOT NULL,	
    SYMBOL	varchar(255),
    TIMESTAMP	date NOT NULL PRIMARY KEY,
    QTY_SHORT_SOLD float
 );

 CREATE or REPLACE TABLE BOARD_MEETINGS (
    SYMBOL   varchar(255) NOT NULL,	
    TIMESTAMP	    date NOT NULL PRIMARY KEY,
    PURPOSE	varchar(255),
    DESC	    varchar(255),
    INDUSTRY	varchar(255),
    COMPANY_NAME     varchar(255)
 );

 -- Using prod schema
 -- USE SCHEMA prod;

 