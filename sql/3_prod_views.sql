use database nifty50db;
use schema prod;

-- BLOCK DEALS
-- 1) Top 5 block deals
CREATE or REPLACE VIEW prod.top_block_deals AS 
select
    symbol,
    client_name,
    qty_traded,
    buy_sell,
    trading_price
from dev.block_deals
order by qty_traded DESC
limit 5;

-- 2) Total buy and sell with max of each grouped by timestamp
CREATE or REPLACE VIEW prod.total_buy_sell AS
select
    timestamp,
    COUNT(CASE WHEN buy_sell = 'BUY' THEN buy_sell END) AS total_buy,
    COUNT(CASE WHEN buy_sell = 'SELL' THEN buy_sell END) AS total_sell,
    MAX(CASE WHEN buy_sell = 'BUY' THEN qty_traded END) AS max_buy,
    MAX(CASE WHEN buy_sell = 'SELL' THEN qty_traded END) AS max_sell
from dev.block_deals
GROUP BY timestamp;


-- BOARD MEETINGS
-- when, by whom and why
CREATE or REPLACE VIEW prod.board_meeting AS
select
    timestamp,
    company_name,
    purpose
from dev.board_meetings
order by timestamp DESC
limit 10;

-- BULK DEALS
-- 1) Top 5 bulk deals
CREATE or REPLACE VIEW prod.top_bulk_deals AS
select
    symbol,
    client_name,
    qty_traded,
    buy_sell,
    trading_price
from dev.bulk_deals
order by qty_traded DESC
limit 5;

-- 2) Total buy and sell with max of each grouped by timestamp
CREATE or REPLACE VIEW prod.max_buy_sell AS
select
    timestamp,
    COUNT(CASE WHEN buy_sell = 'BUY' THEN buy_sell END) AS total_buy,
    COUNT(CASE WHEN buy_sell = 'SELL' THEN buy_sell END) AS total_sell,
    MAX(CASE WHEN buy_sell = 'BUY' THEN qty_traded END) AS max_buy,
    MAX(CASE WHEN buy_sell = 'SELL' THEN qty_traded END) AS max_sell
from dev.bulk_deals
GROUP BY timestamp;

-- monthly advances and declines
-- 1) month wise results
CREATE or REPLACE VIEW prod.month_wise_adv_dec AS
select 
    timestamp,
    advances,
    declines
from dev.monthly_adv_dec
order by timestamp DESC
limit 12;

-- NSE RATIOS
-- 1) open/close
CREATE or REPLACE VIEW prod.nse_open_close AS
select
    timestamp,
    open,
    close
from dev.nse_ratios
order by timestamp ASC;

-- 2) high/low
CREATE or REPLACE VIEW prod.nse_high_low AS
select
    timestamp,
    high,
    low
from dev.nse_ratios
order by timestamp ASC;


-- SHORT SELL ARCH
CREATE or REPLACE VIEW prod.short_sell AS
select
    timestamp,
    symbol,
    qty_short_sold
from dev.short_sell_arch
order by timestamp ASC;

-- SEC ARCHIVES
-- 1) lineplot: high and low
CREATE or REPLACE VIEW prod.sec_high_low AS
select
    timestamp,
    symbol,
    trade_high_price,
    trade_low_price
from dev.sec_archives
order by timestamp ASC;

-- 2) lineplot: open and close
CREATE or REPLACE VIEW prod.sec_open_close AS
select
    timestamp,
    symbol,
    trade_open_price,
    trade_close_price
from dev.sec_archives
order by timestamp ASC;

-- 3) barplot: total traded val in crores
CREATE or REPLACE VIEW prod.sec_total_traded_value AS
select
    timestamp,
    symbol,
    total_traded_val_in_crores,
from dev.sec_archives
order by total_traded_val_in_crores DESC
limit 10;

-- 4) total traded quantity
CREATE or REPLACE VIEW prod.sec_total_traded_qty AS
select
    timestamp,
    symbol,
    total_trading_quant,
from dev.sec_archives
order by total_trading_quant DESC
limit 10;

-- 5) current closing - previous closing
CREATE or REPLACE VIEW prod.sec_cur_prev_close AS
select
    timestamp,
    symbol,
    (trade_close_price - previous_closing_price) AS diff_curr_close_prev_close
from dev.sec_archives
order by timestamp ASC
limit 10;

-- 6) 52week high and low
CREATE or REPLACE VIEW prod.sec_52week AS
select
    timestamp,
    symbol,
    high_52week,
    low_52week
from dev.sec_archives
order by timestamp ASC
limit 15;