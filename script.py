import requests
import pandas as pd
import json
import math
import re
import datetime
from fastapi import HTTPException


class Helper:
    headers = {
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    }

    niftyindices_headers = {
        "Connection": "keep-alive",
        "sec-ch-ua": '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "DNT": "1",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua-mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36",
        "Content-Type": "application/json; charset=UTF-8",
        "Origin": "https://niftyindices.com",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://niftyindices.com/reports/historical-data",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    }

    nifty50 = ["ADANIENT",
            "ADANIPORTS",
            "APOLLOHOSP",
            "ASIANPAINT",
            "AXISBANK",
            "BAJAJ-AUTO",
            "BAJFINANCE",
            "BAJAJFINSV",
            "BPCL",
            "BHARTIARTL",
            "BRITANNIA",
            "CIPLA",
            "COALINDIA",
            "DIVISLAB",
            "DRREDDY",
            "EICHERMOT",
            "GRASIM",
            "HCLTECH",
            "HDFCBANK",
            "HDFCLIFE",
            "HEROMOTOCO",
            "HINDALCO",
            "HINDUNILVR",
            "ICICIBANK",
            "ITC",
            "INDUSINDBK",
            "INFY",
            "JSWSTEEL",
            "KOTAKBANK",
            "LTIM",
            "LT",
            "M&M",
            "MARUTI",
            "NTPC",
            "NESTLEIND",
            "ONGC",
            "POWERGRID",
            "RELIANCE",
            "SBILIFE",
            "SHRIRAMFIN",
            "SBIN",
            "SUNPHARMA",
            "TCS",
            "TATACONSUM",
            "TATAMOTORS",
            "TATASTEEL",
            "TECHM",
            "TITAN",
            "ULTRACEMCO",
            "WIPRO"
        ]

    @staticmethod
    def fetch_data_from_nse(payload):
        try:
            result = requests.get(payload, headers=Helper.headers).json()
        except ValueError:
            session = requests.Session()
            result = session.get("http://nseindia.com", headers=Helper.headers)
            result = session.get(payload, headers=Helper.headers).json()
        return result

    @staticmethod
    def convert_csv_to_dict(csv):
        csv_dict = csv.to_dict(orient="records")
        for record in csv_dict:
            for key, value in record.items():
                if isinstance(value, float):
                    if pd.notna(value) and math.isfinite(value):
                        record[key] = round(value, 2)
                    else:
                        record[key] = str(value)
        return csv_dict

    @staticmethod
    def transform_financial_year(financial_year):
        from datetime import datetime

        start_year, end_year = map(int, financial_year.split("-"))

        start_date = datetime(start_year, 4, 1)
        end_date = datetime(end_year + 1, 3, 31)

        from_date_str = start_date.strftime("%b-%Y")
        to_date_str = end_date.strftime("%b-%Y")

        return from_date_str, to_date_str

    @staticmethod
    def process_security_wise_archive_data(historical_data):
        rounded_data = Helper.convert_csv_to_dict(historical_data)
        processed_data = []
        for entry in rounded_data:
            processed_entry = {
                "date": entry["CH_TIMESTAMP"],
                "symbol": entry["CH_SYMBOL"],
                "series": entry["CH_SERIES"],
                "high_price": entry["CH_TRADE_HIGH_PRICE"],
                "low_price": entry["CH_TRADE_LOW_PRICE"],
                "opening_price": entry["CH_OPENING_PRICE"],
                "closing_price": entry["CH_CLOSING_PRICE"],
                "last_traded_price": entry["CH_LAST_TRADED_PRICE"],
                "previous_close": entry["CH_PREVIOUS_CLS_PRICE"],
                "total_traded_qty": entry["CH_TOT_TRADED_QTY"],
                "total_traded_val": entry["CH_TOT_TRADED_VAL"],
                "52_week_high_price": entry["CH_52WEEK_HIGH_PRICE"],
                "52_week_low_price": entry["CH_52WEEK_LOW_PRICE"],
                "total_trades": entry["CH_TOTAL_TRADES"],
            }
            processed_data.append(processed_entry)
        return processed_data

    @staticmethod
    def process_bulk_block_deal_archive_data(historical_data):
        rounded_data = Helper.convert_csv_to_dict(historical_data)
        processed_data = []
        for entry in rounded_data:
            processed_entry = {
                "date": entry["BD_DT_DATE"],
                "symbol": entry["BD_SYMBOL"],
                "script_name": entry["BD_SCRIP_NAME"],
                "client_name": entry["BD_CLIENT_NAME"],
                "transaction_type": entry["BD_BUY_SELL"],
                "quantity": entry["BD_QTY_TRD"],
                "Trade Price/Weighted Average Trade Price": entry["BD_TP_WATP"],
            }
            processed_data.append(processed_entry)
        return processed_data

    @staticmethod
    def process_short_selling_archives_data(historical_data):
        rounded_data = Helper.convert_csv_to_dict(historical_data)
        processed_data = []
        for entry in rounded_data:
            processed_entry = {
                "date": entry["SS_DATE"],
                "name": entry["SS_NAME"],
                "symbol": entry["SS_SYMBOL"],
                "quantity": entry["SS_QTY"],
            }
            processed_data.append(processed_entry)
        return processed_data

    @staticmethod
    def process_monthly_advances_declines_data(data):
        rounded_data = Helper.convert_csv_to_dict(data)
        processed_data = []
        for entry in rounded_data:
            processed_entry = {
                "record_type": entry["ADM_REC_TY"],
                "month": entry["ADM_MONTH_YEAR_STRING"],
                "advances": entry["ADM_ADVANCES"],
                "declines": entry["ADM_DECLINES"],
                "advances_declines_ratio": entry["ADM_ADV_DCLN_RATIO"],
            }
            processed_data.append(processed_entry)
        return processed_data

    @staticmethod
    def process_board_meetings_data(data):
        rounded_data = Helper.convert_csv_to_dict(data)
        processed_data = []
        for entry in rounded_data:
            processed_entry = {
                "start_date": entry["bm_date"],
                "symbol": entry["bm_symbol"],
                "purpose": entry["bm_purpose"],
                "description": entry["bm_desc"],
                "company_name": entry["sm_name"],
            }
            processed_data.append(processed_entry)
        return processed_data

    @staticmethod
    def process_index_data(data):
        data_json = data.to_json(orient="records")

        try:
            data = json.loads(data_json)
        except json.JSONDecodeError:
            return {"error": "Invalid JSON data"}

        processed_data = []
        for entry in data:
            processed_entry = {
                "index_name": entry["indexCloseOnlineRecords"]["EOD_INDEX_NAME"],
                "open_value": entry["indexCloseOnlineRecords"]["EOD_OPEN_INDEX_VAL"],
                "high_value": entry["indexCloseOnlineRecords"]["EOD_HIGH_INDEX_VAL"],
                "close_value": entry["indexCloseOnlineRecords"]["EOD_CLOSE_INDEX_VAL"],
                "low_value": entry["indexCloseOnlineRecords"]["EOD_LOW_INDEX_VAL"],
                "timestamp": entry["indexCloseOnlineRecords"]["EOD_TIMESTAMP"],
                "traded_quantity": entry["indexTurnoverRecords"]["HIT_TRADED_QTY"],
                "turnover": entry["indexTurnoverRecords"]["HIT_TURN_OVER"],
            }
            processed_data.append(processed_entry)
        return processed_data

    @staticmethod
    def process_index_ratios(historical_data):
        rounded_data = Helper.convert_csv_to_dict(historical_data)
        processed_data = []
        for entry in rounded_data:
            processed_entry = {
                "index_name": entry["INDEX_NAME"],
                "date": entry["HistoricalDate"],
                "open": entry["OPEN"],
                "high": entry["HIGH"],
                "low": entry["LOW"],
                "close": entry["CLOSE"],
            }
            processed_data.append(processed_entry)
        return processed_data


class NSEIndicesData(Helper):
    def __init__(self):
        pass

    def fetch_nse_index_symbols(self):
        url = "http://nseindia.com/api/allIndices"
        try:
            response = self.fetch_data_from_nse(url)
            data = response.get("data", [])
            index_symbols = [index["indexSymbol"] for index in data]
            index_csv = pd.DataFrame(index_symbols, columns=["Indices"])
            return index_csv
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching index symbols: {e}"
            )

    def get_nse_indices_symbols(self):
        return self.fetch_nse_index_symbols()

    def get_nifty_50_indices(self):

        nifty50_df = pd.DataFrame({"symbol": self.nifty50})
        return nifty50_df

    def index_pe_pb_div(self, symbol, start_date, end_date, index_name):
        start_date = datetime.datetime.strptime(start_date, "%d-%b-%Y").strftime(
            "%d %b %Y"
        )
        end_date = datetime.datetime.strptime(end_date, "%d-%b-%Y").strftime("%d %b %Y")

        data = {
            "cinfo": f"{{'name':'{symbol}','startDate':'{start_date}','endDate':'{end_date}','indexName':'{index_name}'}}"
        }
        payload = requests.post(
            "https://niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString",
            headers=self.niftyindices_headers,
            json=data,
        ).json()
        payload = json.loads(payload["d"])

        if not payload:
            raise HTTPException(status_code=404, detail="No historical data found.")

        payload = pd.DataFrame.from_records(payload)
        return payload

    def get_nse_indices_ratios(
        self, symbol: str, start_date: str, end_date: str, index_name: str
    ):
        try:
            historical_ratios_data = self.index_pe_pb_div(
                symbol, start_date, end_date, index_name
            )
            processed_data = self.process_index_ratios(historical_ratios_data)
            return historical_ratios_data
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching historical ratios data: {e}"
            )


class NSEEquitiesData(Helper):
    def __init__(self):
        pass

    def security_wise_archive(self, symbol, start_date, end_date, series="ALL"):
        base_url = "https://www.nseindia.com/api/historical/securityArchives"
        customized_request_url = f"{base_url}?from={start_date}&to={end_date}&symbol={symbol.upper()}&dataType=priceVolumeDeliverable&series={series.upper()}"
        response = self.fetch_data_from_nse(customized_request_url)

        payload = response.get("data", [])

        if not payload:
            raise HTTPException(
                status_code=404, detail=f"No data found for the specified parameters."
            )

        return payload

    def get_security_wise_archive(
        self, symbol: str, start_date: str, end_date: str, series: str
    ):
        try:
            historical_data = self.security_wise_archive(
                symbol, start_date, end_date, series
            )
            # processed_data = self.process_security_wise_archive_data(historical_data) #  For json
            return historical_data
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error fetching security-wise archive data: {e}",
            )

    def bulk_deals_archives(self, start_date, end_date):
        base_url = "https://www.nseindia.com/api/historical/bulk-deals"
        customized_request_url = f"{base_url}?from={start_date}&to={end_date}"
        response = self.fetch_data_from_nse(customized_request_url)

        payload = response.get("data", [])

        if not payload:
            raise HTTPException(
                status_code=404, detail=f"No data found for the specified parameters."
            )

        return pd.DataFrame(payload)

    def get_bulk_deals_archives(self, start_date: str, end_date: str):
        try:
            historical_data = self.bulk_deals_archives(start_date, end_date)
            # processed_data = self.process_bulk_block_deal_archive_data(historical_data)
            return historical_data
            # return {"bulk_deal_archive_data": processed_data}
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching bulk-deals archive data: {e}"
            )

    def block_deals_archives(self, start_date, end_date):
        base_url = "https://www.nseindia.com/api/historical/block-deals"
        customized_request_url = f"{base_url}?from={start_date}&to={end_date}"
        response = self.fetch_data_from_nse(customized_request_url)

        payload = response.get("data", [])

        if not payload:
            raise HTTPException(
                status_code=404, detail=f"No data found for the specified parameters."
            )

        return pd.DataFrame(payload)

    def get_block_deals_archives(self, start_date: str, end_date: str):
        try:
            historical_data = self.block_deals_archives(start_date, end_date)
            # processed_data = self.process_bulk_block_deal_archive_data(historical_data)
            return historical_data
            # return {"block_deal_archive_data": processed_data}
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching block deals archive data: {e}"
            )

    def short_selling_archives(self, start_date, end_date):
        base_url = "https://www.nseindia.com/api/historical/short-selling"
        customized_request_url = f"{base_url}?from={start_date}&to={end_date}"
        response = self.fetch_data_from_nse(customized_request_url)

        payload = response.get("data", [])

        if not payload:
            raise HTTPException(
                status_code=404, detail=f"No data found for the specified parameters."
            )

        return pd.DataFrame(payload)

    def get_short_selling_archives(self, start_date: str, end_date: str):
        try:
            historical_data = self.short_selling_archives(start_date, end_date)
            # processed_data = self.process_short_selling_archives_data(historical_data)
            return historical_data
            # return {"short_selling_archive_data": processed_data}
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error fetching short-selling archive data: {e}",
            )

    def nse_monthly_advances_and_declines(self, year):
        base_url = "https://www.nseindia.com/api/historical/advances-decline-monthly"
        customized_request_url = f"{base_url}?year={year}"
        response = self.fetch_data_from_nse(customized_request_url)

        payload = response.get("data", [])

        if not payload:
            raise HTTPException(
                status_code=404, detail=f"No data found for the specified parameters."
            )

        return pd.DataFrame(payload)

    def get_nse_monthly_advances_and_declines(self, year: str):
        if not re.match(r"\d{4}", year):
            raise HTTPException(
                status_code=422, detail="Invalid year format. Please use 'YYYY' format."
            )

        try:
            historical_data = self.nse_monthly_advances_and_declines(year)
            # processed_data = self.process_monthly_advances_declines_data(historical_data)
            return historical_data
            # return {"monthly_advances_and_declines_data": processed_data}
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error fetching advances and declines data: {e}",
            )

    def nse_equity_tickers(self):
        try:
            symbols = pd.read_csv(
                "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
            )
            tickers = symbols["SYMBOL"].tolist()
            tickers_csv = pd.DataFrame({"tickers": tickers})
            return tickers_csv
        except Exception as e:
            return f"Error fetching equity tickers: {e}"

    def get_nse_equity_tickers(self):
        return self.nse_equity_tickers()

    def board_meetings(self, start_date, end_date):
        base_url = "https://www.nseindia.com/api/corporate-board-meetings"

        customized_request_url = (
            f"{base_url}?index=equities&from={start_date}&to={end_date}"
        )
        response = self.fetch_data_from_nse(customized_request_url)

        if not response:
            raise HTTPException(
                status_code=404, detail=f"No data found for the specified parameters."
            )

        if isinstance(response, list):
            payload = response
        else:
            payload = response.get("data", [])

        return pd.DataFrame(payload)

    def get_board_meetings(self, start_date: str, end_date: str):
        try:
            data = self.board_meetings(start_date, end_date)
            # processed_data = self.process_board_meetings_data(data)
            return data
            # return {"board_meetings_data": processed_data}
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching board meetings data: {e}"
            )


if __name__ == "__main__":

    """
    Endpoints to consider
        1) security_archives -- Done
        2) bulk_deals_archives -- Done
        3) block_deals_archives -- Done
        4) short_selling_archives -- Done
        5) monthly_advances_declines -- Done
        6) equity_tickers_list -- Done
        7) board_meetings -- Done
        8) index_ratios -- Done
        9) index_symbols -- Done

    """

    # NSE Indices
    indicesClass = NSEIndicesData()
    # indices = indicesClass.get_nse_indices_symbols()
    # indices.to_csv(f"nse_indices_{datetime.datetime.now().strftime('%d-%m-%Y')}.csv", index=False)
    # nifty50_indices = indicesClass.get_nifty_50_indices()
    # nifty50_indices.to_csv("nifty50_indices.csv", index=False)
    # ratios = indicesClass.get_nse_indices_ratios('Nifty 50', '12-May-2024', '12-Jul-2024', 'Nifty 50')
    # ratios.to_csv(f"nse_ratios_{datetime.datetime.now().strftime('%d-%m-%Y')}.csv", index=False)

    # NSE Equities
    equitiesClass = NSEEquitiesData()
    # tickers change occasionlly
    # ticker_csv = equitiesClass.get_nse_equity_tickers()
    # ticker_csv.to_csv("nse_ticker.csv", index=False)

    sec_archives = equitiesClass.get_security_wise_archive(
        "TCS", "14-07-2024", "16-07-2024", "ALL"
    )
    #sec_archives.to_csv(f"sec_archives_{datetime.datetime.now().strftime('%d-%m-%Y')}.csv", index=False)
    # bulk_deals_archives = equitiesClass.get_bulk_deals_archives('16-07-2024', '16-07-2024')
    # block_deals_archives = equitiesClass.get_block_deals_archives('16-07-2024', '16-07-2024')
    # short_selling_archives = equitiesClass.get_short_selling_archives('16-07-2024', '16-07-2024')
    # monthly_adv_declines = equitiesClass.get_nse_monthly_advances_and_declines('2024')
    # board_meetings = equitiesClass.get_board_meetings('16-07-2024', '16-07-2024')

    print(
        f"""
        {sec_archives}
    """
    )
