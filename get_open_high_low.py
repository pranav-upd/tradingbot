import tempfile
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_ohl_signals import \
    SgOhlSignalsRepository,today_ist
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.gsheets_logger import multi_log_ORB_OHL_to_gsheets
from algo_scripts.algotrade.scripts.trade_utils.time_manager import get_current_ist_time_as_str, get_screener_run_id, \
    get_today_date_as_str
import time
import csv
import os
from datetime import datetime
import pytz
import glob
import logging
import re
import pandas as pd

from dotenv import load_dotenv

from algo_scripts.algotrade.scripts.trading_style.intraday.core.trade_analyzer.stock_analyzer import \
    get_sectors_for_stock

load_dotenv()
INTRADAY_SCREENER_EMAIL = os.getenv("INTRADAY_SCREENER_EMAIL")
INTRADAY_SCREENER_PWD = os.getenv("INTRADAY_SCREENER_PWD")

def safe_float(s, default_value=None):
    """Convert a string with NBSPs and extra text to a float.
       Returns default_value (None by default) if conversion fails or no number found."""
    try:
        if s is None: # Handle explicit None input
            return default_value
        text = str(s).replace('\xa0', ' ').strip()
        if not text: # Handle empty string input
            return default_value
        # Find all numbers, including negative ones and decimals
        match = re.findall(r"-?\d+\.?\d*", text)
        if match:
            # Take the first matched number
            return float(match[0])
        return default_value
    except (ValueError, TypeError): # Handle cases where conversion to float fails
        return default_value


def read_csv_and_delete(file_path, stock_type):
    data = []

    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)

            # 1. Read and clean headers
            original_headers_raw = next(reader)
            original_headers_cleaned = [header.strip().replace('\ufeff', '') for header in original_headers_raw]
            original_headers_lower = [h.strip().lower().replace('\n', ' ') for h in original_headers_cleaned]
            print(f"Parsed headers: {original_headers_lower}")

            # 2. Build final headers
            final_output_headers = list(original_headers_cleaned)
            final_output_headers.insert(0, 'Timestamp')

            if "Symbol" in final_output_headers:
                symbol_col_idx_in_final = final_output_headers.index("Symbol")
                final_output_headers.pop(symbol_col_idx_in_final)
                final_output_headers.insert(symbol_col_idx_in_final, "STOCKNAME")
                final_output_headers.insert(symbol_col_idx_in_final + 1, "Parameters")

            if "LTP" in final_output_headers:
                ltp_idx = final_output_headers.index("LTP")
                final_output_headers[ltp_idx] = "Price"
                final_output_headers.insert(ltp_idx + 1, "Change")
                final_output_headers.insert(ltp_idx + 2, "Percentage")
            else:
                print("Warning: 'LTP' column not found in original headers.")

            final_output_headers.append("TRADE_TYPE")
            final_output_headers.append("STOCK_TYPE")

            data.append(final_output_headers)

            # Helper for fuzzy index detection
            def find_index(keywords):
                return next((i for i, h in enumerate(original_headers_lower)
                             if all(k in h for k in keywords)), -1)

            # 3. Parse each row
            current_time_ist = get_current_ist_time_as_str()
            for row_original in reader:
                if not row_original:
                    continue

                try:
                    row_original = [cell.strip().replace('\xa0', ' ') for cell in row_original]

                    symbol_idx = find_index(["symbol"])
                    ltp_idx_original = find_index(["ltp"])
                    momentum_idx_original = find_index(["momentum"])
                    open_idx_original = find_index(["open"])
                    deviation_idx_original = find_index(["deviation", "pivot"])
                    todays_range_idx_original = find_index(["today", "range"])
                    ohl_idx_original = find_index(["ohl"])

                    required_indices = [symbol_idx, ltp_idx_original, momentum_idx_original,
                                        open_idx_original, deviation_idx_original,
                                        todays_range_idx_original, ohl_idx_original]

                    max_required_idx = max(required_indices)
                    if any(idx == -1 for idx in required_indices):
                        print(f"Skipping row due to missing required columns: {row_original}")
                        continue

                    if len(row_original) <= max_required_idx:
                        print(f"Padding row (too short): {row_original}")
                        row_original += [''] * (max_required_idx + 1 - len(row_original))

                    symbol_col = row_original[symbol_idx]
                    ltp_col = row_original[ltp_idx_original]
                    momentum_col = row_original[momentum_idx_original]
                    open_col = row_original[open_idx_original]
                    deviation_col = row_original[deviation_idx_original]
                    todays_range_col = row_original[todays_range_idx_original]
                    ohl_col = row_original[ohl_idx_original]

                    print(f"Row : {row_original}")
                    if '\n\xa0\xa0\n' in symbol_col:
                        stock_name, parameters = symbol_col.split('\n\xa0\xa0\n', maxsplit=1)
                    else:
                        stock_name = symbol_col.strip()
                        parameters = ""

                    ltp_parts = ltp_col.split('\n')
                    price = ltp_parts[0].strip()
                    change = ""
                    percentage = ""
                    if len(ltp_parts) > 1:
                        change_percentage_str = ltp_parts[1]
                        change_match = re.search(r"(-?\d+\.?\d*)", change_percentage_str)
                        percentage_match = re.search(r"\((-?\d+\.?\d*)%\)", change_percentage_str)
                        change = change_match.group(1).strip() if change_match else ""
                        percentage = percentage_match.group(1).strip() if percentage_match else ""

                    percentage_float = safe_float(percentage)
                    trade_type = "SELL" if percentage_float is not None and percentage_float < 0 else "BUY"

                    modified_row = [
                        current_time_ist,
                        stock_name.strip(),
                        parameters.strip(),
                        price,
                        change,
                        percentage,
                        momentum_col,
                        open_col,
                        deviation_col,
                        todays_range_col,
                        ohl_col,
                        trade_type,
                        stock_type
                    ]
                    data.append(modified_row)

                except Exception as e:
                    print(f"Error processing row {row_original}: {e}. Skipping.")
                    continue

        os.remove(file_path)
        print(f"✅ Deleted: {file_path}")

    except FileNotFoundError:
        print(f"❌ Error: File not found at {file_path}")
    except Exception as e:
        print(f"❌ Unexpected error in read_csv_and_delete: {e}")

    return data



def get_ohl_stocks():
    pass

def write_to_db(db_objects, logger=None):
    db_repo = SgOhlSignalsRepository()
    session = db_repo.session

    if not db_objects:
        if logger:
            logger.warning("No objects provided to write_to_db.")
        return

    try:
        logger.info("▶️ Connecting to DB")
        session.bulk_save_objects(db_objects)
        session.commit()
        logger.info(f"✅ Batch insert completed successfully for {len(db_objects)} records.")
    except Exception as commit_error:
        session.rollback()
        logger.error(f"❌ Error during batch insert: {commit_error}")



def write_to_db_delete(csv_data, logger=None):
    repo = SgOhlSignalsRepository()
    if not csv_data or len(csv_data) < 1:
        if logger: logger.warning("No data provided to write_to_db.")
        return

    headers = csv_data[0]
    for i, row in enumerate(csv_data[1:]):
        try:
            row_dict = {headers[k]: row[k] for k in range(len(headers))}

            # Get values safely using .get()
            stockname = row_dict.get("STOCKNAME")
            data_stock = stockname.split("\n")
            stockname = data_stock[0]
            trade_type = row_dict.get("TRADE_TYPE")
            parameters = row_dict.get("Parameters")
            price = safe_float(row_dict.get("Price"))
            change = safe_float(row_dict.get("Change"))

            # *** CRITICAL FIX HERE: Provide a default value for percentage if it's None ***
            percentage = safe_float(row_dict.get("Percentage"), default_value=0.0) # Default to 0.0 if cannot parse
            bullish_milestone_tags, bearish_milestone_tags = "", ""
            if percentage > 0 and len(data_stock)>2:
                for i_stock in  data_stock[2:-2]:
                    bullish_milestone_tags += i_stock
            elif percentage <= 0 and len(data_stock)>2:
                for i_stock in  data_stock[2:-2]:
                    bearish_milestone_tags += i_stock
            else:
                pass

            momentum = safe_float(row_dict.get("Momentum"))
            open_price = safe_float(row_dict.get("Open"))
            deviation = row_dict.get("Deviation from Pivots")
            today_range = row_dict.get("Today's Range")
            ohl = row_dict.get("OHL")
            stock_type = "OHL"
            timestamp = row_dict.get("Timestamp")

            volume_data = None

            rec = [
                f"run_{i:04d}",             # 0: screener_run_id (you can replace with a variable if needed)
                today_ist(),           # 1: screener_date
                "OHL",
                # 2: screener_type
                stockname,        # 3: screener        # 3: screener
                stockname,             # 4: stock_name
                trade_type,            # 5: trade_type
                volume_data,           # 6: screener_rank → assuming volume_data holds rank. Adjust if not.
                price,                 # 7: price
                change,                # 8: change
                percentage,            # 9: percentage
                momentum,              #10: momentum
                open_price,            #11: open
                deviation,             #12: deviation_from_pivots
                today_range,           #13: todays_range
                ohl,                   #14: ohl
                stock_type,            #15: stock_type
                "TODO",              #16: weekly_trend → insert real value if available
                bullish_milestone_tags,           #18: bullish_milestone_tags → insert real value if available
                bearish_milestone_tags,           #19: bearish_milestone_tags → insert real value if available
            ]

            repo.insert(rec)
            if logger: logger.debug("Inserted %s", stockname)
        except Exception as e:
            stockname_safe = row_dict.get("STOCKNAME", "N/A") if 'row_dict' in locals() else "N/A"
            if logger: logger.error(f"Error during DB insert for row {i+1} ({stockname_safe}): {e}. Row data: {row}")

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_ohl_signals import SgOhlSignals

def transform_csv_data_to_db_records(csv_data_final):
    """
    Transforms raw CSV data (list of lists) into a list of SgOhlSignals model instances for batch DB insert.
    """
    if not csv_data_final or len(csv_data_final) < 2:
        return []

    headers = csv_data_final[0]
    rows = csv_data_final[1:]

    db_objects = []
    screener_rank = 1

    for i, row in enumerate(rows):
        try:
            print("Row to be transformed -> " + str(row))
            # 🚨 Check if header count matches row count
            if len(row) != len(headers):
                print(f"❌ Skipping row {i+1}: Header-Row length mismatch. Headers={len(headers)}, Row={len(row)} → Row: {row}")
                continue

            row_dict = dict(zip(headers, row))  # safer than index mapping

            # ✅ Validate required fields
            required_fields = ["Price", "Momentum", "Open", "Deviation from Pivots", "Today's Range", "OHL"]
            if any(not row_dict.get(field) for field in required_fields):
                print(f"❌ Skipping row {i+1}: Missing required fields → Row: {row}")
                continue

            stockname = row_dict.get("STOCKNAME", "").strip()
            data_stock = stockname.split("\n")

            # Extract clean stock name and parameters
            if '\n' in stockname:
                stockname_parts = stockname.split('\n')
                stockname_clean = stockname_parts[0].strip()
            else:
                stockname_clean = stockname.strip()

            trade_type = row_dict.get("TRADE_TYPE", "").strip()
            price = safe_float(row_dict.get("Price"))
            change = safe_float(row_dict.get("Change"))
            percentage = safe_float(row_dict.get("Percentage"), default_value=0.0)

            bullish_milestone_tags, bearish_milestone_tags = "", ""
            if percentage > 0 and len(data_stock) > 2:
                bullish_milestone_tags = ''.join(data_stock[2:-2]).strip()
            elif percentage <= 0 and len(data_stock) > 2:
                bearish_milestone_tags = ''.join(data_stock[2:-2]).strip()

            momentum = safe_float(row_dict.get("Momentum"))
            open_price = safe_float(row_dict.get("Open"))
            deviation = row_dict.get("Deviation from Pivots")
            today_range = row_dict.get("Today's Range")
            ohl = row_dict.get("OHL")
            stock_type = row_dict.get("STOCK_TYPE", "OHL")
            run_id = get_screener_run_id()
            screener_rank += 1

            # Get sector
            sector_info = get_sectors_for_stock(stockname_clean)
            sector_quants = sector_info.get('QUANTS_SECTOR', '')
            sector_intra = sector_info.get('INTRA_SCRNR', '')

            # Ensure both are strings
            sector_quants = sector_quants if isinstance(sector_quants, str) else str(sector_quants)
            sector_intra = sector_intra if isinstance(sector_intra, str) else str(sector_intra)

            if sector_info and isinstance(sector_info, dict):
                parts = []

                if pd.notna(sector_quants) and str(sector_quants).strip():
                    parts.append(str(sector_quants).strip())

                if pd.notna(sector_intra) and str(sector_intra).strip():
                    parts.append(str(sector_intra).strip())

                sector_name = " | ".join(parts)

            else:
                sector_name = ""

            # Construct DB object
            signal = SgOhlSignals(
                screener_run_id=run_id,
                screener_date=today_ist(),
                screener_type="OPEN_HIGH_LOW",
                screener=ohl,
                stock_name=stockname_clean,
                trade_type=trade_type,
                screener_rank=screener_rank,
                price=price,
                change=change,
                percentage=percentage,
                momentum=momentum,
                open=open_price,
                deviation_from_pivots=deviation,
                todays_range=today_range,
                ohl=ohl,
                stock_type=stock_type,
                sector=sector_name,
                weekly_trend="",
                bullish_milestone_tags=bullish_milestone_tags,
                bearish_milestone_tags=bearish_milestone_tags
            )
            print("signal -> " + str(signal))
            db_objects.append(signal)

        except Exception as e:
            print(f"❌ Error transforming row {i+1}: {e} → Row: {row}")

    return db_objects



def get_ohl_stocks_intra_screener(logger):
    delete_ohl_screener_records_from_db(logger)
    ist_zone = pytz.timezone("Asia/Kolkata")
    time_now = datetime.now()
    ist_now = time_now.astimezone(ist_zone)
    date_time = ist_now.strftime("%Y-%m-%d %I:%M:%S %p %Z")
    user_data_dir = tempfile.mkdtemp(prefix="chrome-ud-ohl")

    delete_ohl_csv_files()
    chromeOptions = webdriver.ChromeOptions()
    download_dir = os.getcwd()
    logger.info(f"Downloading {download_dir}..")
    prefs = {"download.default_directory" : download_dir}
    chromeOptions.add_experimental_option("prefs",prefs)
    #Optional: Add headless argument for running without a visible browser UI
    chromeOptions.add_argument("--headless")
    chromeOptions.add_argument("--disable-gpu")
    chromeOptions.add_argument("--no-sandbox")
    chromeOptions.add_argument(f"--user-data-dir={user_data_dir}")
    driver = webdriver.Chrome(options=chromeOptions)
    driver.set_window_size(1920, 1080)
    driver.get("https://intradayscreener.com/login")

    try:
        logger.info("Attempting login.")
        # login_link = WebDriverWait(driver, 30).until(
        #     EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/div[1]/input'))
        # )
        # login_link.click()

        email_field = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/div[1]/input'))
        )
        email_field.send_keys(INTRADAY_SCREENER_EMAIL)

        password_field = driver.find_element(By.XPATH, '/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/div[2]/div/input')
        password_field.send_keys(INTRADAY_SCREENER_PWD)

        login_button = driver.find_element(By.XPATH, '/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/button')
        driver.execute_script("arguments[0].click();", login_button)
        logger.info("Login successful. Waiting for page to load...")
        time.sleep(5)

        try:
            chart_close_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[2]/div/div/div[1]/button'))
            )
            chart_close_button.click()
            logger.info("Chart window closed.")
            time.sleep(1)
        except TimeoutException:
            logger.info("Chart window close button not found or already closed.")
        except Exception as e:
            logger.warning(f"Error closing chart window: {e}")

        try:
            session_close_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[1]/div[1]/button'))
            )
            session_close_button.click()
            logger.info("Session bar closed.")
            time.sleep(1)
        except TimeoutException:
            logger.info("Session bar close button not found or already closed.")
        except Exception as e:
            logger.warning(f"Error closing session bar: {e}")

        intraday_menu = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[1]/div[2]/nav/div/ul/li[2]'))
        )
        intraday_menu.click()
        time.sleep(2)

        logger.info("Intraday Menu Clicked.")

        ohl_menu = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[1]/div[2]/nav/div/ul/li[2]/div/a[9]'))
        )
        ohl_menu.click()
        time.sleep(3)

        logger.info("OHL Menu Clicked. On OHL Scanner page.")

        # Refresh the page
        driver.refresh()
        logger.info("Page refreshed after OHL menu click.")
        time.sleep(5)  # Wait for page elements to reload

        csv_data_cash = []
        csv_data_fno = []

        try:
            cash_tab = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[2]/div/button[2]'))
            )
            cash_tab.click()
            time.sleep(2)
            logger.info("Cash tab clicked.")

            export_cash_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[2]/app-ohlc-scanner/div[1]/div[3]/div[1]/div[2]/button[1]'))
            )
            export_cash_button.click()
            logger.info("CASH export button clicked. Waiting for download...")
            time.sleep(15)

            file_name = "Open High Low.csv"
            csv_file_path = os.path.join(download_dir, file_name)

            csv_data_cash = read_csv_and_delete(csv_file_path, "CASH")
            # csv_data_cash = read_csv_and_delete(
            #     r"C:\trad-fin\algo_platform_01_2025\algo_scripts\algotrade\scripts\trading_style\intraday\strategies\intraday_screener\ohl\Open High Low.csv", "CASH")
            logger.info(f"CASH data processed. Found {len(csv_data_cash) - 1 if csv_data_cash else 0} rows.")
        except TimeoutException:
            logger.info("Cash market tab/export button not found or not clickable within timeout, skipping CASH data export.")
        except Exception as e:
            logger.error(f"Error processing CASH data: {e}")

        try:
            fno_tab = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[2]/div/button[1]'))
            )
            fno_tab.click()
            time.sleep(2)

            logger.info("FNO tab clicked.")

            export_fno_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[2]/app-ohlc-scanner/div[1]/div[3]/div[1]/div[2]/button[1]'))
            )
            export_fno_button.click()
            logger.info("FNO export button clicked. Waiting for download...")
            time.sleep(15)

            file_name = "Open High Low.csv"
            csv_file_path = os.path.join(download_dir, file_name)
            # csv_data_fno = read_csv_and_delete(
            #     r"C:\trad-fin\algo_platform_01_2025\algo_scripts\algotrade\scripts\trading_style\intraday\strategies\intraday_screener\ohl\Open High Low.csv", "FNO")
            csv_data_fno = read_csv_and_delete(csv_file_path, "FNO")

            logger.info(f"FNO data processed. Found {len(csv_data_fno) - 1 if csv_data_fno else 0} rows.")
        except TimeoutException:
            logger.error("Timeout while processing FNO data (tab or export button).")
        except Exception as e:
            logger.error(f"Error processing FNO data: {e}")

        csv_data_final = []
        if csv_data_cash:
            csv_data_final.extend(csv_data_cash)
            if csv_data_fno and len(csv_data_fno) > 1:
                csv_data_final.extend(csv_data_fno[1:])
        elif csv_data_fno:
            csv_data_final.extend(csv_data_fno)
        else:
            logger.warning("No data found for either CASH or FNO. Returning empty lists.")
            return [], []

        if not csv_data_final or len(csv_data_final) <= 1:
            logger.warning("No combined CSV data available to process after filtering/combination.")
            return [], []

        transformed_csv_data_final = transform_csv_data_to_db_records(csv_data_final)
        #multi_log_ORB_OHL_to_gsheets(transformed_csv_data_final, sheetname="OHL")
        write_to_db(transformed_csv_data_final, logger)

        headers = csv_data_final[0]
        rows = csv_data_final[1:]

        if "TRADE_TYPE" not in headers:
            logger.error("TRADE_TYPE column not found in final headers. Cannot filter stocks.")
            return [], []

        trade_type_idx = headers.index("TRADE_TYPE")
        buy_stocks = [row for row in rows if row[trade_type_idx] == "BUY"]
        sell_stocks = [row for row in rows if row[trade_type_idx] == "SELL"]

        buy_stocks_dicts = [
            {headers[i]: row[i] for i in range(len(headers))}
            for row in buy_stocks
        ]

        sell_stocks_dicts = [
            {headers[i]: row[i] for i in range(len(headers))}
            for row in sell_stocks
        ]

        return buy_stocks_dicts, sell_stocks_dicts

    except TimeoutException as te:
        logger.error(f"A WebDriver element operation timed out: {te}")
        return [], []
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_ohl_stocks_intra_screener: {e}")
        return [], []
    finally:
        logger.info("Quitting WebDriver.")
        try:
            driver.quit()
            shutil.rmtree(user_data_dir, ignore_errors=True)
        except Exception:
            pass

def delete_ohl_csv_files():
    """
    Deletes all CSV files in the specified folder that start with 'Open High Low'.
    """
    try:
        folder_path = r"C:\trad-fin\algo_platform_01_2025\algo_scripts\algotrade\scripts\trading_style\intraday\strategies\intraday_screener\ohl"
        file_pattern = os.path.join(folder_path, "Open High Low*.csv")

        files_to_delete = glob.glob(file_pattern)

        for file in files_to_delete:
            os.remove(file)
            print(f"Deleted: {file}")

        if not files_to_delete:
            print("No matching files found to delete.")

    except Exception as e:
        print(f"An error occurred in delete_ohl_csv_files: {e}")

def delete_ohl_screener_records_from_db(logger):
    """
    deletes
    """
    db_repo = SgOhlSignalsRepository()  # ✅ Connect to DB
    screener_date = get_today_date_as_str()
    screener_type = "OPEN_HIGH_LOW"

    try:
        logger.info("Starting Deletion " + get_current_ist_time_as_str())
        deleted_count = db_repo.delete_by_date_and_type(screener_date,screener_type)
        logger.info(f"Completed Deletion of {deleted_count} records at " + get_current_ist_time_as_str())
    except Exception as e:
        logger.error(f"❌ Error deleting records: {str(e)}")

    try:
        # ✅ Commit changes to the DB **after processing each rank_type**
        db_repo.session.commit()
        logger.info("✅ Data deleted successfully from the database.")
    except Exception as commit_error:
        logger.error(f"❌ Error committing changes to the database: {str(commit_error)}")
        db_repo.session.rollback()  # ✅ Rollback only this batch

if __name__ == "__main__":
    logger = logging.getLogger("IntradayScreenerOHLLogger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    logger.info("Starting OHL Screener process.")
    buy_signals, sell_signals = get_ohl_stocks_intra_screener(logger)
    logger.info(f"OHL Buy Signals found: {len(buy_signals)}")
    for signal in buy_signals:
        logger.info(f"  BUY: {signal.get('STOCKNAME')} (Price: {signal.get('Price')}, %: {signal.get('Percentage')})")
    logger.info(f"OHL Sell Signals found: {len(sell_signals)}")
    for signal in sell_signals:
        logger.info(f"  SELL: {signal.get('STOCKNAME')} (Price: {signal.get('Price')}, %: {signal.get('Percentage')})")
    logger.info("OHL Screener process finished.")