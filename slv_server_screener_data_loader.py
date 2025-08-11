#uvicorn algo_scripts.algotrade.scripts.server.slv_server_data_loader:app --log-level debug
from get_intra_stock_alerts import get_intraday_stock_alerts

from algo_scripts.algotrade.scripts.trade_utils.trade_logger import get_trade_actions_dynamic_logger, \
    get_sell_logger_name, get_screener_logger_name

from fastapi import FastAPI, HTTPException,Depends

import logging

from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.market_context.get_sector_advance_decline import \
    load_sector_advance_decline_to_db
from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.ohl.get_open_high_low import \
    get_ohl_stocks_intra_screener
from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.rank.index_contrib.get_idx_contrib_db import \
    get_intraday_screener_index_contributors

# suppress SQL text logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
from dotenv import load_dotenv

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.screener.screener_log import \
    ScreenerLogRepository
from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.bwis.get_intra_bwis_stocks import \
    get_intraday_screener_bwis
from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.market_context.get_index_performance import \
    load_index_performance_to_db
from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.market_context.get_sector_performance import \
    load_sector_performance_to_db
from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.rank.toppers.get_intra_top_gainers_losers import \
    load_fno_top_gainers_losers_headless, load_fno_toppers_groupwise_headless
from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.intraday_screener.sr_breakout.get_breakout_breakdown_stocks import \
    get_sr_breakout_stocks, get_filtered_sr_breakout_stocks

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import (
    get_db_session, SessionScoped, initialize_global_session, close_global_session, cleanup
)
from algo_scripts.algotrade.scripts.trade_utils.time_manager import get_current_ist_time_as_str
import os
import atexit


from datetime import datetime
import pytz
from fastapi.middleware.cors import CORSMiddleware



load_dotenv(override=True)
SOURCE_REPO = os.getenv('SOURCE_REPO')

# Basic logging configuration for fallback logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (change this in production)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Register FastAPI startup event
@app.on_event("startup")
def startup():
    """Initialize global session at startup."""
    initialize_global_session()

# Register FastAPI shutdown event
@app.on_event("shutdown")
def shutdown():
    """Cleanup database connections when the app shuts down."""
    close_global_session()
    cleanup()

# Also register cleanup in case the container is forcefully stopped
atexit.register(cleanup)


def convert_to_ist(utc_time_str):
    # Parse the UTC datetime string
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    # Define UTC and IST timezones
    utc_zone = pytz.timezone("UTC")
    ist_zone = pytz.timezone("Asia/Kolkata")
    # Localize the UTC time and convert to IST
    utc_time = utc_zone.localize(utc_time)
    ist_time = utc_time.astimezone(ist_zone)
    # Format IST time as human-readable string
    return ist_time.strftime("%Y-%m-%d %I:%M:%S %p %Z")

@app.get("/intraday/screener/load_fno_top_rankers")
async def load_fno_top_rankers():
    logger_prefix = "load_screener_"
    screener_name = "top_gainer_loser"
    process_logger_name = get_screener_logger_name(logger_prefix,screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:

        load_fno_top_gainers_losers_headless(logger)

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(500, detail=f"{screener_name} failed")
    finally:
        session.close()


@app.get("/intraday/screener/fno_toppers/{group_name}")
async def load_fno_top_rankers(group_name: str):
    logger_prefix = "load_screener_"
    screener_name = group_name
    process_logger_name = get_screener_logger_name(logger_prefix,screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:

        load_fno_toppers_groupwise_headless(logger,group_name)

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(500, detail=f"{screener_name} failed")
    finally:
        session.close()

# Endpoint to trigger a limit order action
@app.get("/intraday/screener/index_performance/loader/")
async def index_performance_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "index_performance"
    process_logger_name = get_screener_logger_name(logger_prefix,screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:
        load_index_performance_to_db(logger)

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(500, detail=f"{screener_name} failed")
    finally:
        session.close()

@app.get("/intraday/screener/resistance_breakout/loader/")
async def resistance_breakout_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "resistance_breakout"
    process_logger_name = get_screener_logger_name(logger_prefix,screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:

        get_filtered_sr_breakout_stocks(logger,"RESISTANCE")

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(500, detail=f"{screener_name} failed")
    finally:
        session.close()

@app.get("/intraday/screener/support_breakdown/loader/")
async def support_breakdown_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "support_breakdown"
    process_logger_name = get_screener_logger_name(logger_prefix,screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:

        get_filtered_sr_breakout_stocks(logger,"SUPPORT")

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(500, detail=f"{screener_name} failed")
    finally:
        session.close()


@app.get("/intraday/screener/bwis/loader/")
async def bwis_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "bwis"
    process_logger_name = get_screener_logger_name(logger_prefix,screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:

        get_intraday_screener_bwis(logger)

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(500, detail=f"{screener_name} failed")
    finally:
        session.close()

@app.get("/intraday/screener/sector_performance/loader/")
async def sector_performance_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "sector_performance"
    process_logger_name = get_screener_logger_name(logger_prefix, screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:
        load_sector_performance_to_db(logger) # Call the modified function

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(status_code=500, detail=f"{screener_name} processing failed: {str(e)}")
    finally:
        session.close()

@app.get("/intraday/screener/index_contributor/loader/")
async def index_contributor_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "index_contributor"
    process_logger_name = get_screener_logger_name(logger_prefix, screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:
        get_intraday_screener_index_contributors(logger) # Call the modified function

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully to Database"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(status_code=500, detail=f"{screener_name} processing failed: {str(e)}")
    finally:
        session.close()

@app.get("/intraday/screener/sector_advance_decline/loader/")
async def sector_advance_decline_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "sector_advance_decline"
    process_logger_name = get_screener_logger_name(logger_prefix, screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:
        load_sector_advance_decline_to_db(logger) # Call the modified function

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(status_code=500, detail=f"{screener_name} processing failed: {str(e)}")
    finally:
        session.close()

@app.get("/intraday/screener/open_high_low/loader/")
async def open_high_low_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "open_high_low"
    process_logger_name = get_screener_logger_name(logger_prefix, screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:
        buy_signals, sell_signals = get_ohl_stocks_intra_screener(logger)
        logger.info(f"OHL Buy Signals found: {len(buy_signals)}")

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(status_code=500, detail=f"{screener_name} processing failed: {str(e)}")
    finally:
        session.close()

@app.get("/intraday/screener/intraday_alerts/loader/")
async def intraday_alerts_loader_action():
    logger_prefix = "load_screener_"
    screener_name = "intraday_alerts"
    process_logger_name = get_screener_logger_name(logger_prefix, screener_name)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger

    session = next(get_db_session())
    repo = ScreenerLogRepository(session)
    log_entry = repo.start_log(process_logger_name)

    logger.info(f"Started Processing {screener_name} at {get_current_ist_time_as_str()}")
    try:
        get_intraday_stock_alerts(logger)

        logger.info(f"Completed Processing {screener_name} at {get_current_ist_time_as_str()}")
        repo.complete_log(log_entry.log_id, status="COMPLETED")
        return {"status": f"{screener_name} loaded successfully"}
    except Exception as e:
        # mark failure and capture message
        repo.complete_log(log_entry.log_id, status="FAILED", error_message=str(e))
        logger.error(f"Failed Processing {screener_name}: {e}", exc_info=True)
        # re-raise or convert to HTTP error
        raise HTTPException(status_code=500, detail=f"{screener_name} processing failed: {str(e)}")
    finally:
        session.close()


@app.get("/screener_data_loader/health")
def health_check():
    return {"status": "Screener_Data_Loader healthy"}
