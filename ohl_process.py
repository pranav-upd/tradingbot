from datetime import datetime
import logging

from sqlalchemy import false

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.market_context.index_snapshot import IndexSnapshotRepository
from algo_scripts.algotrade.scripts.trade_utils.time_manager import get_today_date_as_str, get_current_ist_time_as_str
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_ohl_signals import SgOhlSignalsRepository, SgOhlSignals
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.tradingview_signals import TVSignalsRepository
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import (
    Base, get_db_session)
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_intraday_screener_signals import SgIntradayScreenerSignalsRepository
import json
import os
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from collections import Counter

load_dotenv()

fyers_token_primary = os.getenv('FYERS_ACCESS_TOKEN')
fyers_client_id = os.getenv('FYERS_CLIENT_ID')
fyers_token = fyersModel.FyersModel(
    token=fyers_token_primary,
    is_async=False,
    client_id=fyers_client_id,
    log_path="."
)
#Get the latest data for ohl and intraday alerts stocks
#get_ohl_stocks_intra_screener(logger)
#get_intraday_screener_bwis(logger_two)
#---------------------------------------------------------------------------
intra_alerts_repo = SgIntradayScreenerSignalsRepository()
tv_repo = TVSignalsRepository(get_db_session())
index_repo = IndexSnapshotRepository()
ohl_data = SgOhlSignalsRepository()
index_data = index_repo.get_snapshot_by_date_and_index(get_today_date_as_str(), 2) #Change to get_today_date_as_str
ohl_repo = SgOhlSignalsRepository()
logger = logging.getLogger("IntradayScreenerOHLLogger")
"""
import csv

# Replace with your actual file path if different
file_path = 'tv_signal.csv'
#os.system("python algo_scripts/algotrade/scripts/trading_style/intraday/strategies/intraday_screener/market_context/get_index_performance.py")
#os.system("python3 algo_scripts/algotrade/scripts/trading_style/intraday/strategies/intraday_screener/ohl/get_open_high_low.py")
#os.system("python3 algo_scripts/algotrade/scripts/trading_style/intraday/strategies/intraday_screener/scanner/get_intra_stock_alerts.py")
data_as_dicts = []
tv_input_data = []

with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data_as_dicts.append(dict(row))

# Print first 3 rows to check
for val in data_as_dicts:
    tv_input_data.append([*val.values()])

print(tv_input_data)
tv_signal_status = tv_repo.bulk_insert_trade_signals(tv_input_data)
"""

def get_quotes(fyers_token, symbols, logger):
    # Ensure NSE prefix
    symbols_string = ",".join([f"NSE:{s.upper()}-EQ" for s in symbols])
    data = {"symbols": symbols_string}
    logger.info(f"Fetching quotes for: {symbols_string}")

    try:
        response = fyers_token.quotes(data)
        print(response)
        logger.info(f"Quotes response: {response}")

        if response.get("s") == "ok":
            return response
        else:
            return {"status": "Invalid Token", "reason": "Token is invalid or response malformed"}
    except Exception as e:
        logger.exception("Exception occurred while fetching quotes.")
        return {"status": "Invalid Token", "reason": str(e)}

def get_intra_stock_data(fyers_token, STOCK_SYMBOLS, logger):
    response_data = None

    response_data = get_quotes(fyers_token, STOCK_SYMBOLS, logger)


    print(response_data)
    if not response_data or "d" not in response_data:
        logger.error("Failed to fetch LTP from all tokens.")
        return []

    stock_data = []
    for stock in response_data["d"]:
        try:
            symbol = stock["n"]
            last_price = stock["v"]["lp"]
            prev_close = stock["v"]["prev_close_price"]
            change_percent = stock["v"]["chp"]

            logger.info(json.dumps({
                "symbol": symbol, "ltp": last_price,
                "prev_close": prev_close, "change_percent": change_percent
            }))

            stock_data.append({
                "symbol": symbol,
                "ltp": last_price,
                "prev_close": prev_close,
                "change_percent": change_percent
            })
        except KeyError as ke:
            logger.warning(f"Key missing in ETF quote: {stock} - {ke}")
    return stock_data



if "BULLISH" in str(index_data[0].breadth_trend):
    ohl_data = ohl_repo.get_by_screener_date_and_screener(get_today_date_as_str())
    ohl_data_filtered_high = [x[4] for x in ohl_data if "Low" in x[3] and "PRB" in x[-1]]
    ohl_data_dict = tv_repo.check_stocks_by_date_and_screener(ohl_data_filtered_high, get_today_date_as_str())
    ltp_bullish = get_intra_stock_data(fyers_token, [i[0][0] for i in ohl_data_dict], logger)
    def get_intra_get_level(stock_name:str):
        intra_get_level = intra_alerts_repo.fetch_signals_by_date_stock_and_screeners(get_today_date_as_str(), stock_name[4:-3])
        if len(intra_get_level)!=0:
            return intra_get_level[0].level
        else:
            return -1
    ltp_bullish =[{"symbol":z["symbol"],"to_buy":False,"to_sell":True} for z in ltp_bullish if z["ltp"]>get_intra_get_level(z["symbol"]) and get_intra_get_level(z["symbol"])!=-1]
    print(ltp_bullish)


else:
    ohl_data = ohl_repo.get_by_screener_date_and_screener(get_today_date_as_str())
    ohl_data_filtered_high = [x[4] for x in ohl_data if "High" in x[3] and "PRB" in x[-1]]
    ohl_data_dict = tv_repo.check_stocks_by_date_and_screener(ohl_data_filtered_high, get_today_date_as_str())
    ltp_bearish = get_intra_stock_data(fyers_token, [i[0][0] for i in ohl_data_dict], logger)
    print(ltp_bearish)
    def get_intra_get_level(stock_name:str):
        intra_get_level = intra_alerts_repo.fetch_signals_by_date_stock_and_screeners(get_today_date_as_str(), stock_name[4:-3])
        if len(intra_get_level)!=0:
            return intra_get_level[0].level
        else:
            return -1
    ltp_bearish =[{"symbol":z["symbol"],"to_buy":False,"to_sell":True} for z in ltp_bearish if z["ltp"]<get_intra_get_level(z["symbol"]) and get_intra_get_level(z["symbol"])!=-1]
    print(ltp_bearish)
