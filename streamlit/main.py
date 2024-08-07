# Import python packages
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# Get the current credentials
conn = st.connection("snowflake")
session = conn.session()

block_deals = session.sql(f"select * from prod.top_block_deals;").to_pandas()
block_total = session.sql(f"select * from prod.total_block_buy_sell;").to_pandas()
board_meetings = session.sql(f"select * from prod.board_meeting;").to_pandas()
monthly_adv_dec = session.sql(f"select * from prod.month_wise_adv_dec;").to_pandas()
nse_open_close = session.sql(f"select * from prod.nse_open_close;").to_pandas()
nse_high_low = session.sql(f"select * from prod.nse_high_low;").to_pandas()
short_sell_arch = session.sql(f"select * from prod.short_sell;").to_pandas()
bulk_deals = session.sql(f"select * from prod.top_bulk_deals;").to_pandas()
bulk_total = session.sql(f"select * from prod.max_bulk_buy_sell;").to_pandas()
sec_arch_hl = session.sql(f"select * from prod.sec_high_low;").to_pandas()
sec_arch_oc = session.sql(f"select * from prod.sec_open_close;").to_pandas()
sec_arch_52 = session.sql(f"select * from prod.sec_52week;").to_pandas()
sec_arch_qty = session.sql(f"select * from prod.sec_total_traded_qty;").to_pandas()
sec_arch_val = session.sql(f"select * from prod.sec_total_traded_value;").to_pandas()
sec_arch_cur_prev = session.sql(f"select * from prod.sec_cur_prev_close;").to_pandas()


st.header("Nifty50 Dashboard")
st.markdown(
    "This dashboard showcases an end-to-end automated <a href='https://github.com/arnavrneo/Nifty50-Data-Engineering'>data engineering pipeline</a>. The data is fetched from *nseindia* website. <br/>*Note: The data may be stale.*",
    unsafe_allow_html=True,
)

# Overview
st.subheader("Pipeline Overview")
st.image("streamlit/pipeline-overview.png")


# Board Meetings
with st.expander(label="Upcoming/Past Board Meetings"):
    st.dataframe(
        board_meetings,
        hide_index=True,
        column_config={
            "TIMESTAMP": "Date",
            "SYMBOL": "Stock Symbol",
            "PURPOSE": "Purpose of Meeting",
        },
    )

# Security Archives
# 1) High/Low
st.subheader("Stocks High/Low")
st.caption(f"Updated: {sec_arch_hl['TIMESTAMP'][0]}")
st.bar_chart(
    sec_arch_hl,
    x="SYMBOL",
    y=["TRADE_HIGH_PRICE", "TRADE_LOW_PRICE"],
    horizontal=False,
    x_label="Stock",
    y_label="Price",
    color="SYMBOL",
)

# 2) Open/Close
st.subheader("Stocks Open/Close")
st.caption(f"Updated: {sec_arch_oc['TIMESTAMP'][0]}")
st.bar_chart(
    sec_arch_oc,
    x="SYMBOL",
    y=["TRADE_OPEN_PRICE", "TRADE_CLOSE_PRICE"],
    horizontal=False,
    x_label="Stock",
    y_label="Price",
    color="SYMBOL",
)

# 3) 52 week
# 4) Closing price
col9, col10 = st.columns([0.6, 0.4])
with col9:
    st.subheader("Stocks 52 Weeks")
    st.caption(f"Updated: {sec_arch_52['TIMESTAMP'][0]}")
    st.line_chart(
        sec_arch_52,
        x="SYMBOL",
        y=["HIGH_52WEEK", "LOW_52WEEK"],
        x_label="Stock",
        y_label="Price",
    )

with col10:
    st.subheader("Price change")
    with st.expander("About price change"):
        st.write("Price change refers to the difference between a stock's closing price on a trading day and its closing price on the previous trading day.")
    st.caption(f"Updated: {sec_arch_cur_prev['TIMESTAMP'][0]}")
    st.bar_chart(
        sec_arch_cur_prev,
        x="SYMBOL",
        y="DIFF_CURR_CLOSE_PREV_CLOSE",
        x_label="Stock",
        y_label="Daily price change",
    )

# 5) Total traded vol
# 6) Total traded qty

col11, col12 = st.columns(2)

with col11:
    st.subheader("Total Traded Value")
    st.caption(f"Updated: {sec_arch_val['TIMESTAMP'][0]}")
    st.bar_chart(
        sec_arch_val,
        horizontal=True,
        x="SYMBOL",
        y="TOTAL_TRADED_VAL_IN_CRORES",
        x_label="Traded Value (in Crores)",
        y_label="Stock",
    )

with col12:
    st.subheader("Total Traded Quantity")
    st.caption(f"Updated: {sec_arch_qty['TIMESTAMP'][0]}")
    tra_vol = alt.Chart(sec_arch_qty).encode(
        alt.Theta("TOTAL_TRADING_QUANT:Q").stack(True),
        alt.Radius("TOTAL_TRADING_QUANT").scale(type="sqrt", zero=True, rangeMin=20),
        color="SYMBOL:N",
    )

    asd1 = tra_vol.mark_arc(innerRadius=20, stroke="#fff")

    st.altair_chart(asd1)

# Block Trades
st.subheader("Block Deals")
st.caption(f"Updated: {block_deals['TIMESTAMP'][0]}")
with st.expander("About Block Deals"):
    st.write("A block deal refers to a single transaction where the exchange of shares involves quantities exceeding Rs. 5,00,000 or cases where the total traded value exceeds Rs. 10 crores.")
col1, col2 = st.columns([0.7, 0.3])
with col1:
    st.bar_chart(
        block_deals,
        x="CLIENT_NAME",
        y="QTY_TRADED",
        horizontal=True,
        x_label="Units Traded",
        y_label="Client",
        color="SYMBOL",
    )

with col2:
    st.markdown(
        f"<h4>Total Buy: <span style='color:green'>{block_total['TOTAL_BUY'].values[0]}</span> </h4>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<h7>Max Buy: <span style='color:grey'>{block_total['MAX_BUY'].values[0]}</span> </h7>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<h4>Total Sell: <span style='color:red'>{block_total['TOTAL_SELL'].values[0]}</span> </h4>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<h7>Max Sell: <span style='color:grey'>{block_total['MAX_SELL'].values[0]}</span> </h7>",
        unsafe_allow_html=True,
    )


# Bulk Trades
st.subheader("Bulk Deals")
st.caption(f"Updated: {bulk_deals['TIMESTAMP'][0]}")
with st.expander("About Bulk Deals"):
    st.write("Bulk Deal refers to a transaction wherein a singular entity, such as an institutional investor or a notable trader, engages in the substantial buying or selling of a significant quantity of a company's shares within a single trade. ")

col1, col2 = st.columns([0.3, 0.7])
with col2:
    st.bar_chart(
        bulk_deals,
        x="CLIENT_NAME",
        y="QTY_TRADED",
        horizontal=True,
        x_label="Units Traded",
        y_label="Client",
        color="SYMBOL",
    )

with col1:
    st.markdown(
        f"<h4>Total Buy: <span style='color:green'>{bulk_total['TOTAL_BUY'].values[0]}</span> </h4>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<h7>Max Buy: <span style='color:grey'>{bulk_total['MAX_BUY'].values[0]}</span> </h7>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<h4>Total Sell: <span style='color:red'>{bulk_total['TOTAL_SELL'].values[0]}</span> </h4>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<h7>Max Sell: <span style='color:grey'>{bulk_total['MAX_SELL'].values[0]}</span> </h7>",
        unsafe_allow_html=True,
    )

# Monthly adv dec

st.subheader("Monthly Adv/Dec")
st.caption(f"Updated: {max(monthly_adv_dec['TIMESTAMP'])}")
with st.expander("About Monthly Advances and Declines"):
    st.write("Advances and declines refers generally to the number of stocks (or other assets in a particular market) that closed at a higher and those that closed at a lower price than the previous day, respectively. They help in predicting whether a price trend is likely to continue or reverse.")
c1 = (
    alt.Chart(monthly_adv_dec)
    .mark_line()
    .encode(
        x="TIMESTAMP",
        y=alt.Y("ADVANCES", scale=alt.Scale(domain=[900, 1800])),
        color=alt.value("#20d70c"),
    )
    .properties(width=800, height=300)
)

c2 = (
    alt.Chart(monthly_adv_dec)
    .mark_line()
    .encode(
        x="TIMESTAMP",
        y=alt.Y("DECLINES", scale=alt.Scale(domain=[900, 1800])),
        color=alt.value("#cb450c"),
    )
    .properties(width=800, height=300)
)

line_51 = c1.mark_line(color="#20d70c").encode(
    alt.Y("ADVANCES:Q", scale=alt.Scale(domain=[900, 1800])).axis(titleColor="#20d70c")
)

line_61 = c2.mark_line(color="#cb450c").encode(
    alt.Y("DECLINES:Q", scale=alt.Scale(domain=[900, 1800])).axis(titleColor="#cb450c")
)

c_y = alt.layer(line_51, line_61).resolve_scale(y="independent")

st.altair_chart(c_y, use_container_width=True)

# Short Sell
st.subheader("Short Sold Stocks")
st.caption(f"Updated: {max(short_sell_arch['TIMESTAMP'])}")
with st.expander("About short selling stocks"):
    st.write("Short selling a stock is when a trader borrows shares from a broker and immediately sells them with the expectation that the share price will fall shortly after. If it does, the trader can buy the shares back at the lower price, return them to the broker, and keep the difference, minus any loan interest, as profit. For example: You borrow 10 shares of a company, then immediately sell them on the stock market for say Rs. 10 each, generating Rs. 100. If the price drops to Rs. 5 per share, you could use your Rs. 100 to buy back all 10 shares for only Rs. 50, then return the shares to the broker. In the end, you netted Rs. 50 on the short (minus any commissions, fees and interest).")
st.scatter_chart(
    short_sell_arch,
    x="TIMESTAMP",
    y="QTY_SHORT_SOLD",
    color="SYMBOL",
    x_label="Date",
    y_label="Qty. Short Sold",
)

# NSE Ratios
# 1) OPEN CLOSE
st.subheader("Nifty50 Open/Close")
st.caption(f"Updated: {max(nse_open_close['TIMESTAMP'])}")
c3 = (
    alt.Chart(nse_open_close)
    .mark_line()
    .encode(
        x="TIMESTAMP",
        y=alt.Y("OPEN", scale=alt.Scale(domain=[20000, 26000])),
        color=alt.value("#20d70c"),
    )
    .properties(width=800, height=300)
)

c4 = (
    alt.Chart(nse_open_close)
    .mark_line()
    .encode(
        x="TIMESTAMP",
        y=alt.Y("CLOSE", scale=alt.Scale(domain=[20000, 26000])),
        color=alt.value("#cb450c"),
    )
    .properties(width=800, height=300)
)

line_A = c3.mark_line(color="#20d70c").encode(
    alt.Y("OPEN:Q", scale=alt.Scale(domain=[20000, 26000])).axis(titleColor="#20d70c")
)

line_B = c4.mark_line(color="#cb450c").encode(
    alt.Y("CLOSE:Q", scale=alt.Scale(domain=[20000, 26000])).axis(titleColor="#cb450c")
)

c_b = alt.layer(line_A, line_B).resolve_scale(y="independent")
st.altair_chart(c_b, use_container_width=True)

# 2) HIGH LOW
st.subheader("Nifty50 High/Low")
st.caption(f"Updated: {max(nse_high_low['TIMESTAMP'])}")
c5 = (
    alt.Chart(nse_high_low)
    .mark_line()
    .encode(
        x="TIMESTAMP",
        y=alt.Y("HIGH", scale=alt.Scale(domain=[20000, 26000])),
        color=alt.value("#20d70c"),
    )
    .properties(width=800, height=300)
)

c6 = (
    alt.Chart(nse_high_low)
    .mark_line()
    .encode(
        x="TIMESTAMP",
        y=alt.Y("LOW", scale=alt.Scale(domain=[20000, 26000])),
        color=alt.value("#cb450c"),
    )
    .properties(width=800, height=300)
)

line_5 = c5.mark_line(color="#20d70c").encode(
    alt.Y("HIGH:Q", scale=alt.Scale(domain=[20000, 26000])).axis(titleColor="#20d70c")
)

line_6 = c6.mark_line(color="#cb450c").encode(
    alt.Y("LOW:Q", scale=alt.Scale(domain=[20000, 26000])).axis(titleColor="#cb450c")
)

c_c = alt.layer(line_5, line_6).resolve_scale(y="independent")
st.altair_chart(c_c, use_container_width=True)

a1, a2, a3 = st.columns(3)
with a2:
    st.markdown(
        "<center>Made by: <a href='https://www.github.com/arnavrneo'>arnavrneo</a></center>",
        unsafe_allow_html=True,
    )

# Test Code
# import streamlit as st
# import pandas as pd
# import altair as alt

# block_deals = pd.read_csv("processed/block_deals_arch.csv")
# block_total = pd.read_csv("processed/block_total_buy_sell.csv")
# board_meetings = pd.read_csv("processed/board_meetings.csv")
# bulk_deals = pd.read_csv("processed/bulk_deals_arch.csv")
# bulk_total = pd.read_csv("processed/bulk_total_buy_sell.csv")
# monthly_adv_dec = pd.read_csv("processed/monthly_adv_dec.csv")
# nse_ratios = pd.read_csv("processed/nse_ratios.csv")
# nse_open_close = pd.read_csv("processed/nse_open_close.csv")
# nse_high_low = pd.read_csv("processed/nse_high_low.csv")
# short_sell_arch = pd.read_csv("processed/short_selling_arch.csv")

# sec_arch = pd.read_csv("processed/sec_arch.csv")
# sec_arch_hl = pd.read_csv("processed/sec_high_low.csv")
# sec_arch_oc = pd.read_csv("processed/sec_open_close.csv")
# sec_arch_52 = pd.read_csv("processed/sec_52week.csv")
# sec_arch_qty = pd.read_csv("processed/sec_total_traded_qty.csv")
# sec_arch_val = pd.read_csv("processed/sec_total_trade_value.csv")
# sec_arch_cur_prev = pd.read_csv("processed/sec_cur_prev_close.csv")


# st.header("Nifty50 Dashboard")
# st.markdown("This dashboard showcases an end-to-end automated data engineering pipeline <link-here>. The data is fetched from *nseindia* website. It may be stale", unsafe_allow_html=True)

# # Board Meetings
# with st.expander(label="Upcoming/Past Board Meetings", icon="🗒️"):
#     st.dataframe(board_meetings, hide_index=True,
#         column_config={
#         "TIMESTAMP": "Date",
#         "SYMBOL": "Stock Symbol",
#         "PURPOSE": "Purpose of Meeting"
#     })

# # Security Archives
# # 1) High/Low
# st.subheader("Stocks High/Low")
# st.caption(f"Updated: {sec_arch_hl['TIMESTAMP'][0]}")
# st.bar_chart(sec_arch_hl, x='SYMBOL', y=['TRADE_HIGH_PRICE', 'TRADE_LOW_PRICE'], horizontal=False, x_label="Stock", y_label="Price", color='SYMBOL')

# # 2) Open/Close
# st.subheader("Stocks Open/Close")
# st.caption(f"Updated: {sec_arch_oc['TIMESTAMP'][0]}")
# st.bar_chart(sec_arch_oc, x='SYMBOL', y=['TRADE_OPEN_PRICE', 'TRADE_CLOSE_PRICE'], horizontal=False, x_label="Stock", y_label="Price", color='SYMBOL')

# # 3) 52 week
# # 4) Closing price
# col9, col10 = st.columns([0.6, 0.4])
# with col9:
#     st.subheader("Stocks 52 Weeks")
#     st.caption(f"Updated: {sec_arch_52['TIMESTAMP'][0]}")
#     st.line_chart(sec_arch_52, x='SYMBOL', y=['HIGH_52WEEK', 'LOW_52WEEK'], x_label="Stock", y_label="Price")

# with col10:
#     st.subheader("Price change")
#     st.caption(f"Updated: {sec_arch_cur_prev['TIMESTAMP'][0]}")
#     st.bar_chart(sec_arch_cur_prev, x='SYMBOL', y='DIFF_CURR_CLOSE_PREV_CLOSE', x_label="Stock", y_label="Daily price change")

# # 5) Total traded vol
# # 6) Total traded qty

# col11, col12 = st.columns(2)

# with col11:
#     st.subheader("Total Traded Value")
#     st.caption(f"Updated: {sec_arch_val['TIMESTAMP'][0]}")
#     st.bar_chart(sec_arch_val, horizontal=True, x='SYMBOL', y='TOTAL_TRADED_VAL_IN_CRORES', x_label="Traded Value (in Crores)", y_label="Stock")

# with col12:
#     st.subheader("Total Traded Quantity")
#     st.caption(f"Updated: {sec_arch_qty['TIMESTAMP'][0]}")
#     tra_vol = alt.Chart(sec_arch_qty).encode(
#         alt.Theta("TOTAL_TRADING_QUANT:Q").stack(True),
#         alt.Radius("TOTAL_TRADING_QUANT").scale(type="sqrt", zero=True, rangeMin=20),
#         color="SYMBOL:N",
#     )

#     asd1 = tra_vol.mark_arc(innerRadius=20, stroke="#fff")

#     st.altair_chart(asd1)

# # Block Trades
# st.subheader("Block Deals")
# st.caption(f"Updated: {block_deals['TIMESTAMP'][0]}")
# col1, col2 = st.columns([0.7, 0.3])
# with col1:
#     st.bar_chart(block_deals, x='CLIENT_NAME', y='QTY_TRADED', horizontal=True, x_label="Units Traded", y_label="Client", color='SYMBOL')

# with col2:
#     st.markdown(f"<h4>Total Buy: <span style='color:green'>{block_total['TOTAL_BUY'].values[0]}</span> </h4>", unsafe_allow_html=True)
#     st.markdown(f"<h7>Max Buy: <span style='color:grey'>{block_total['MAX_BUY'].values[0]}</span> </h7>", unsafe_allow_html=True)
#     st.markdown(f"<h4>Total Sell: <span style='color:red'>{block_total['TOTAL_SELL'].values[0]}</span> </h4>", unsafe_allow_html=True)
#     st.markdown(f"<h7>Max Sell: <span style='color:grey'>{block_total['MAX_SELL'].values[0]}</span> </h7>", unsafe_allow_html=True)


# # Bulk Trades
# st.subheader("Bulk Deals")
# st.caption(f"Updated: {bulk_deals['TIMESTAMP'][0]}")
# col1, col2 = st.columns([0.3, 0.7])
# with col2:
#     st.bar_chart(bulk_deals, x='CLIENT_NAME', y='QTY_TRADED', horizontal=False, x_label="Units Traded", y_label="Client", color='SYMBOL')

# with col1:
#     st.markdown(f"<h4>Total Buy: <span style='color:green'>{bulk_total['TOTAL_BUY'].values[0]}</span> </h4>", unsafe_allow_html=True)
#     st.markdown(f"<h7>Max Buy: <span style='color:grey'>{bulk_total['MAX_BUY'].values[0]}</span> </h7>", unsafe_allow_html=True)
#     st.markdown(f"<h4>Total Sell: <span style='color:red'>{bulk_total['TOTAL_SELL'].values[0]}</span> </h4>", unsafe_allow_html=True)
#     st.markdown(f"<h7>Max Sell: <span style='color:grey'>{bulk_total['MAX_SELL'].values[0]}</span> </h7>", unsafe_allow_html=True)

# # Monthly adv dec

# st.subheader("Monthly Adv/Dec")

# c1 = alt.Chart(monthly_adv_dec).mark_line().encode(
#     x='TIMESTAMP',
#     y=alt.Y('ADVANCES', scale=alt.Scale(domain=[900,1800])),
#     color=alt.value("#20d70c")
# ).properties(
#     width=800,
#     height=300
# )

# c2 = alt.Chart(monthly_adv_dec).mark_line().encode(
#     x='TIMESTAMP',
#     y=alt.Y('DECLINES', scale=alt.Scale(domain=[900,1800])),
#     color=alt.value("#cb450c")
# ).properties(
#     width=800,
#     height=300
# )

# c = c1 + c2

# st.altair_chart(c, use_container_width=True)

# # Short Sell
# #st.bar_chart(short_sell_arch, x='SYMBOL', y='QTY_SHORT_SOLD', x_label="Stock", y_label="Qty. Short Sold", color='SYMBOL')
# st.subheader("Short Sold Stocks")
# a = alt.Chart(short_sell_arch).mark_rect().encode(
#     alt.X("TIMESTAMP:O").title("Date").axis(labelAngle=0),
#     alt.Y("SYMBOL:O").title("Stock"),
#     alt.Color("QTY_SHORT_SOLD", scale=alt.Scale(scheme='yellowgreenblue')).title(None),
#     tooltip=[
#         alt.Tooltip("QTY_SHORT_SOLD", title="Qty. Short Sold"),
#         alt.Tooltip("SYMBOL", title="Stock"),
#     ],
# ).configure_view(
#     step=20,
#     strokeWidth=20
# ).configure_axis(
#     domain=False
# )
# st.altair_chart(a, use_container_width=True)

# # NSE Ratios
# # 1) OPEN CLOSE
# st.subheader("Nifty50 Open/Close")
# c3 = alt.Chart(nse_open_close).mark_line().encode(
#     x='TIMESTAMP',
#     y=alt.Y('OPEN', scale=alt.Scale(domain=[20000,26000])),
#     color=alt.value("#20d70c")
# ).properties(
#     width=800,
#     height=300
# )

# c4 = alt.Chart(nse_open_close).mark_line().encode(
#     x='TIMESTAMP',
#     y=alt.Y('CLOSE', scale=alt.Scale(domain=[20000,26000])),
#     color=alt.value("#cb450c")
# ).properties(
#     width=800,
#     height=300
# )

# line_A = c3.mark_line(color='#20d70c').encode(
#     alt.Y('OPEN:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#20d70c')
# )

# line_B = c4.mark_line(color='#cb450c').encode(
#     alt.Y('CLOSE:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#cb450c')
# )

# c_b = alt.layer(line_A, line_B).resolve_scale(y='independent')
# st.altair_chart(c_b, use_container_width=True)

# # 2) HIGH LOW
# st.subheader("Nifty50 High/Low")
# c5 = alt.Chart(nse_high_low).mark_line().encode(
#     x='TIMESTAMP',
#     y=alt.Y('HIGH', scale=alt.Scale(domain=[20000,26000])),
#     color=alt.value("#20d70c")
# ).properties(
#     width=800,
#     height=300
# )

# c6 = alt.Chart(nse_high_low).mark_line().encode(
#     x='TIMESTAMP',
#     y=alt.Y('LOW', scale=alt.Scale(domain=[20000,26000])),
#     color=alt.value("#cb450c")
# ).properties(
#     width=800,
#     height=300
# )

# line_5 = c5.mark_line(color='#20d70c').encode(
#     alt.Y('HIGH:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#20d70c')
# )

# line_6 = c6.mark_line(color='#cb450c').encode(
#     alt.Y('LOW:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#cb450c')
# )

# c_c = alt.layer(line_5, line_6).resolve_scale(y='independent')
# st.altair_chart(c_c, use_container_width=True)
