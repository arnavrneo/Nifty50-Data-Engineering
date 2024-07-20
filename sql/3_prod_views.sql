use database nifty50db;
use schema dev;

-- BLOCK DEALS
-- 1) Top 5 block deals
select
    symbol,
    client_name,
    qty_traded,
    buy_sell,
    trading_price
from block_deals
order by qty_traded DESC
limit 5;

-- 2) Total buy and sell with max of each grouped by timestamp
select
    timestamp,
    COUNT(CASE WHEN buy_sell = 'BUY' THEN buy_sell END) AS total_buy,
    COUNT(CASE WHEN buy_sell = 'SELL' THEN buy_sell END) AS total_sell,
    MAX(CASE WHEN buy_sell = 'BUY' THEN qty_traded END) AS max_buy,
    MAX(CASE WHEN buy_sell = 'SELL' THEN qty_traded END) AS max_sell
from block_deals
GROUP BY timestamp DESC;


-- BOARD MEETINGS
-- when, by whom and why
select
    timestamp,
    company_name,
    purpose
from board_meetings
order by timestamp DESC
limit 10;

-- BULK DEALS
-- 1) Top 5 bulk deals
select
    symbol,
    client_name,
    qty_traded,
    buy_sell,
    trading_price
from bulk_deals
order by qty_traded DESC
limit 5;

-- 2) Total buy and sell with max of each grouped by timestamp
select
    timestamp,
    COUNT(CASE WHEN buy_sell = 'BUY' THEN buy_sell END) AS total_buy,
    COUNT(CASE WHEN buy_sell = 'SELL' THEN buy_sell END) AS total_sell,
    MAX(CASE WHEN buy_sell = 'BUY' THEN qty_traded END) AS max_buy,
    MAX(CASE WHEN buy_sell = 'SELL' THEN qty_traded END) AS max_sell
from bulk_deals
GROUP BY timestamp DESC;

-- monthly advances and declines