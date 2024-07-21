import streamlit as st
import pandas as pd
import altair as alt

block_deals = pd.read_csv("processed/block_deals_arch.csv")
block_total = pd.read_csv("processed/block_total_buy_sell.csv")
board_meetings = pd.read_csv("processed/board_meetings.csv")
bulk_deals = pd.read_csv("processed/bulk_deals_arch.csv")
bulk_total = pd.read_csv("processed/bulk_total_buy_sell.csv")
monthly_adv_dec = pd.read_csv("processed/monthly_adv_dec.csv")
nse_ratios = pd.read_csv("processed/nse_ratios.csv")
nse_open_close = pd.read_csv("processed/nse_open_close.csv")
nse_high_low = pd.read_csv("processed/nse_high_low.csv") 
short_sell_arch = pd.read_csv("processed/short_selling_arch.csv")

sec_arch = pd.read_csv("processed/sec_arch.csv")
sec_arch_hl = pd.read_csv("processed/sec_high_low.csv")
sec_arch_oc = pd.read_csv("processed/sec_open_close.csv")
sec_arch_52 = pd.read_csv("processed/sec_52week.csv")
sec_arch_qty = pd.read_csv("processed/sec_total_traded_qty.csv")
sec_arch_val = pd.read_csv("processed/sec_total_trade_value.csv")
sec_arch_cur_prev = pd.read_csv("processed/sec_cur_prev_close.csv")


st.header("Nifty50 Dashboard")
st.markdown("This dashboard showcases an end-to-end automated data engineering pipeline <link-here>. The data is fetched from *nseindia* website. It may be stale", unsafe_allow_html=True)

# Board Meetings
with st.expander(label="Upcoming/Past Board Meetings", icon="🗒️"):
    st.dataframe(board_meetings, hide_index=True,
        column_config={
        "TIMESTAMP": "Date",
        "SYMBOL": "Stock Symbol",
        "PURPOSE": "Purpose of Meeting"
    })

# Security Archives
# 1) High/Low
st.subheader("Stocks High/Low")
st.caption(f"Updated: {sec_arch_hl['TIMESTAMP'][0]}")
st.bar_chart(sec_arch_hl, x='SYMBOL', y=['TRADE_HIGH_PRICE', 'TRADE_LOW_PRICE'], horizontal=False, x_label="Stock", y_label="Price", color='SYMBOL')

# 2) Open/Close
st.subheader("Stocks Open/Close")
st.caption(f"Updated: {sec_arch_oc['TIMESTAMP'][0]}")
st.bar_chart(sec_arch_oc, x='SYMBOL', y=['TRADE_OPEN_PRICE', 'TRADE_CLOSE_PRICE'], horizontal=False, x_label="Stock", y_label="Price", color='SYMBOL')

# 3) 52 week
# 4) Closing price
col9, col10 = st.columns([0.6, 0.4])
with col9:
    st.subheader("Stocks 52 Weeks")
    st.caption(f"Updated: {sec_arch_52['TIMESTAMP'][0]}")
    st.line_chart(sec_arch_52, x='SYMBOL', y=['HIGH_52WEEK', 'LOW_52WEEK'], x_label="Stock", y_label="Price")

with col10:
    st.subheader("Price change")
    st.caption(f"Updated: {sec_arch_cur_prev['TIMESTAMP'][0]}")
    st.bar_chart(sec_arch_cur_prev, x='SYMBOL', y='DIFF_CURR_CLOSE_PREV_CLOSE', x_label="Stock", y_label="Daily price change")

# 5) Total traded vol
# 6) Total traded qty

col11, col12 = st.columns(2)

with col11:
    st.subheader("Total Traded Value")
    st.caption(f"Updated: {sec_arch_val['TIMESTAMP'][0]}")
    st.bar_chart(sec_arch_val, horizontal=True, x='SYMBOL', y='TOTAL_TRADED_VAL_IN_CRORES', x_label="Traded Value (in Crores)", y_label="Stock")

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
col1, col2 = st.columns([0.7, 0.3])
with col1:
    st.bar_chart(block_deals, x='CLIENT_NAME', y='QTY_TRADED', horizontal=True, x_label="Units Traded", y_label="Client", color='SYMBOL')

with col2:
    st.markdown(f"<h4>Total Buy: <span style='color:green'>{block_total['TOTAL_BUY'].values[0]}</span> </h4>", unsafe_allow_html=True)
    st.markdown(f"<h7>Max Buy: <span style='color:grey'>{block_total['MAX_BUY'].values[0]}</span> </h7>", unsafe_allow_html=True)
    st.markdown(f"<h4>Total Sell: <span style='color:red'>{block_total['TOTAL_SELL'].values[0]}</span> </h4>", unsafe_allow_html=True)
    st.markdown(f"<h7>Max Sell: <span style='color:grey'>{block_total['MAX_SELL'].values[0]}</span> </h7>", unsafe_allow_html=True)



# Bulk Trades
st.subheader("Bulk Deals")
st.caption(f"Updated: {bulk_deals['TIMESTAMP'][0]}")
col1, col2 = st.columns([0.3, 0.7])
with col2:
    st.bar_chart(bulk_deals, x='CLIENT_NAME', y='QTY_TRADED', horizontal=False, x_label="Units Traded", y_label="Client", color='SYMBOL')

with col1:
    st.markdown(f"<h4>Total Buy: <span style='color:green'>{bulk_total['TOTAL_BUY'].values[0]}</span> </h4>", unsafe_allow_html=True)
    st.markdown(f"<h7>Max Buy: <span style='color:grey'>{bulk_total['MAX_BUY'].values[0]}</span> </h7>", unsafe_allow_html=True)
    st.markdown(f"<h4>Total Sell: <span style='color:red'>{bulk_total['TOTAL_SELL'].values[0]}</span> </h4>", unsafe_allow_html=True)
    st.markdown(f"<h7>Max Sell: <span style='color:grey'>{bulk_total['MAX_SELL'].values[0]}</span> </h7>", unsafe_allow_html=True)

# Monthly adv dec

st.subheader("Monthly Adv/Dec")

c1 = alt.Chart(monthly_adv_dec).mark_line().encode(
    x='TIMESTAMP',
    y=alt.Y('ADVANCES', scale=alt.Scale(domain=[900,1800])),
    color=alt.value("#20d70c")
).properties(
    width=800,
    height=300
)

c2 = alt.Chart(monthly_adv_dec).mark_line().encode(
    x='TIMESTAMP',
    y=alt.Y('DECLINES', scale=alt.Scale(domain=[900,1800])),
    color=alt.value("#cb450c")
).properties(
    width=800,
    height=300
)

c = c1 + c2

st.altair_chart(c, use_container_width=True)

# Short Sell
#st.bar_chart(short_sell_arch, x='SYMBOL', y='QTY_SHORT_SOLD', x_label="Stock", y_label="Qty. Short Sold", color='SYMBOL')
st.subheader("Short Sold Stocks")
a = alt.Chart(short_sell_arch).mark_rect().encode(
    alt.X("TIMESTAMP:O").title("Date").axis(labelAngle=0),
    alt.Y("SYMBOL:O").title("Stock"),
    alt.Color("QTY_SHORT_SOLD", scale=alt.Scale(scheme='yellowgreenblue')).title(None),
    tooltip=[
        alt.Tooltip("QTY_SHORT_SOLD", title="Qty. Short Sold"),
        alt.Tooltip("SYMBOL", title="Stock"),
    ],
).configure_view(
    step=20,
    strokeWidth=20
).configure_axis(
    domain=False
)
st.altair_chart(a, use_container_width=True)

# NSE Ratios
# 1) OPEN CLOSE
st.subheader("Nifty50 Open/Close")
c3 = alt.Chart(nse_open_close).mark_line().encode(
    x='TIMESTAMP',
    y=alt.Y('OPEN', scale=alt.Scale(domain=[20000,26000])),
    color=alt.value("#20d70c")
).properties(
    width=800,
    height=300
)

c4 = alt.Chart(nse_open_close).mark_line().encode(
    x='TIMESTAMP',
    y=alt.Y('CLOSE', scale=alt.Scale(domain=[20000,26000])),
    color=alt.value("#cb450c")
).properties(
    width=800,
    height=300
)

line_A = c3.mark_line(color='#20d70c').encode(
    alt.Y('OPEN:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#20d70c')
)

line_B = c4.mark_line(color='#cb450c').encode(
    alt.Y('CLOSE:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#cb450c')
)

c_b = alt.layer(line_A, line_B).resolve_scale(y='independent')
st.altair_chart(c_b, use_container_width=True)

# 2) HIGH LOW
st.subheader("Nifty50 High/Low")
c5 = alt.Chart(nse_high_low).mark_line().encode(
    x='TIMESTAMP',
    y=alt.Y('HIGH', scale=alt.Scale(domain=[20000,26000])),
    color=alt.value("#20d70c")
).properties(
    width=800,
    height=300
)

c6 = alt.Chart(nse_high_low).mark_line().encode(
    x='TIMESTAMP',
    y=alt.Y('LOW', scale=alt.Scale(domain=[20000,26000])),
    color=alt.value("#cb450c")
).properties(
    width=800,
    height=300
)

line_5 = c5.mark_line(color='#20d70c').encode(
    alt.Y('HIGH:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#20d70c')
)

line_6 = c6.mark_line(color='#cb450c').encode(
    alt.Y('LOW:Q', scale=alt.Scale(domain=[20000,26000])).axis(titleColor='#cb450c')
)

c_c = alt.layer(line_5, line_6).resolve_scale(y='independent')
st.altair_chart(c_c, use_container_width=True)


