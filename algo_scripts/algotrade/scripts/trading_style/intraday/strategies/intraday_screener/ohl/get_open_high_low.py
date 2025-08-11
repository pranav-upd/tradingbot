from datetime import datetime
import logging
import json
import os
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.market_context.index_snapshot import IndexSnapshotRepository
from algo_scripts.algotrade.scripts.trade_utils.time_manager import get_today_date_as_str
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_ohl_signals import SgOhlSignalsRepository
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.tradingview_signals import TVSignalsRepository
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import get_db_session
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_intraday_screener_signals import SgIntradayScreenerSignalsRepository

load_dotenv()

def get_quotes(fyers_token, symbols, logger):
    """Fetches quotes for a list of symbols."""
    symbols_string = ",".join([f"NSE:{s.upper()}-EQ" for s in symbols])
    data = {"symbols": symbols_string}
    logger.info(f"Fetching quotes for: {symbols_string}")

    try:
        response = fyers_token.quotes(data)
        logger.info(f"Quotes response: {response}")

        if response.get("s") == "ok":
            return response
        else:
            logger.error(f"Fyers API error: {response.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        logger.exception("Exception occurred while fetching quotes.")
        return None

def get_intra_stock_data(fyers_token, stock_symbols, logger):
    """Fetches intraday stock data (LTP, etc.) for a list of symbols."""
    response_data = get_quotes(fyers_token, stock_symbols, logger)

    if not response_data or "d" not in response_data:
        logger.error("Failed to fetch LTP from all tokens.")
        return []

    stock_data = []
    for stock in response_data["d"]:
        try:
            stock_data.append({
                "symbol": stock["n"],
                "ltp": stock["v"]["lp"],
                "prev_close": stock["v"]["prev_close_price"],
                "change_percent": stock["v"]["chp"]
            })
        except KeyError as ke:
            logger.warning(f"Key missing in quote: {stock} - {ke}")
    return stock_data

def get_ohl_stocks_intra_screener(logger):
    """
    This function fetches Open-High-Low (OHL) data, processes it based on market trend (bullish/bearish),
    and returns potential buy and sell signals.
    """
    fyers_token_primary = os.getenv('FYERS_ACCESS_TOKEN')
    fyers_client_id = os.getenv('FYERS_CLIENT_ID')
    fyers_token = fyersModel.FyersModel(
        token=fyers_token_primary,
        is_async=False,
        client_id=fyers_client_id,
        log_path="."
    )

    db_session = get_db_session()
    intra_alerts_repo = SgIntradayScreenerSignalsRepository()
    tv_repo = TVSignalsRepository(db_session)
    index_repo = IndexSnapshotRepository()
    ohl_repo = SgOhlSignalsRepository()

    today_str = get_today_date_as_str()
    index_data = index_repo.get_snapshot_by_date_and_index(today_str, 2)

    buy_signals = []
    sell_signals = []

    if not index_data:
        logger.error("Could not retrieve index data. Skipping OHL processing.")
        return buy_signals, sell_signals

    def get_intra_get_level(stock_name: str):
        """Fetches the intraday level for a given stock."""
        level_data = intra_alerts_repo.fetch_signals_by_date_stock_and_screeners(today_str, stock_name[4:-3])
        return level_data[0].level if level_data else -1

    if "BULLISH" in str(index_data[0].breadth_trend):
        logger.info("Market trend is BULLISH. Looking for buy signals.")
        ohl_data = ohl_repo.get_by_screener_date_and_screener(today_str)
        ohl_data_filtered = [x[4] for x in ohl_data if "Low" in x[3] and "PRB" in x[-1]]
        ohl_data_dict = tv_repo.check_stocks_by_date_and_screener(ohl_data_filtered, today_str)

        stock_symbols = [i[0][0] for i in ohl_data_dict]
        ltp_data = get_intra_stock_data(fyers_token, stock_symbols, logger)

        for stock in ltp_data:
            level = get_intra_get_level(stock["symbol"])
            if level != -1 and stock["ltp"] > level:
                buy_signals.append({"symbol": stock["symbol"], "to_buy": True, "to_sell": False, "ltp": stock["ltp"], "level": level})
        logger.info(f"Found {len(buy_signals)} potential buy signals.")

    else:
        logger.info("Market trend is BEARISH. Looking for sell signals.")
        ohl_data = ohl_repo.get_by_screener_date_and_screener(today_str)
        ohl_data_filtered = [x[4] for x in ohl_data if "High" in x[3] and "PRB" in x[-1]]
        ohl_data_dict = tv_repo.check_stocks_by_date_and_screener(ohl_data_filtered, today_str)

        stock_symbols = [i[0][0] for i in ohl_data_dict]
        ltp_data = get_intra_stock_data(fyers_token, stock_symbols, logger)

        for stock in ltp_data:
            level = get_intra_get_level(stock["symbol"])
            if level != -1 and stock["ltp"] < level:
                sell_signals.append({"symbol": stock["symbol"], "to_buy": False, "to_sell": True, "ltp": stock["ltp"], "level": level})
        logger.info(f"Found {len(sell_signals)} potential sell signals.")

    return buy_signals, sell_signals
