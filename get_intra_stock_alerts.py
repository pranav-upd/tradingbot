import csv, os, logging

# suppress SQL text logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
import tempfile
import shutil
from datetime import datetime
import os
import time
import pytz
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
load_dotenv()

from algo_scripts.algotrade.scripts.trade_utils.time_manager import get_current_ist_time_as_str, get_today_date_as_str,get_screener_run_id

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_intraday_screener_signals import \
    SgIntradayScreenerSignalsRepository, SgIntradayScreenerSignals


INTRADAY_SCREENER_EMAIL = os.getenv("INTRADAY_SCREENER_EMAIL")
INTRADAY_SCREENER_PWD = os.getenv("INTRADAY_SCREENER_PWD")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_csv_and_delete(download_dir,file_name, stock_type, strength, date_time,logger):

    data = []
    csv_file = os.path.join(download_dir, file_name)

    logger.info(f"Reading {csv_file}..")
    try:
        with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)

            # Clean headers to remove hidden BOM markers or extra spaces
            headers = next(reader)
            ist_zone = pytz.timezone("Asia/Kolkata")
            time_now = datetime.now()
            ist_now = time_now.astimezone(ist_zone)
            screener_date = get_today_date_as_str()
            run_time_str = ist_now.strftime("%H:%M")  # ✅ Get current run time for run_history
            screener_rank = 1

            screener_run_id  = get_screener_run_id()

            for row in reader:
                if len(row) > 0:
                    # Split the 'Stock Name' and 'Parameters' column
                    try:
                        stock_name, parameters = row[0].split('\xa0\xa0', maxsplit=1)
                    except ValueError:
                        stock_name = row[0]
                        parameters = ""
                    # Split the 'LTP' column into Price, Change, and Percentage
                    ltp_parts = row[1].split('\n')
                    price = ltp_parts[0].strip()
                    change, vol_change = ltp_parts[1].split('(')
                    vol_change = vol_change.strip(')%')
                    # Determine Trade Type based on percentage
                    trade_type = "SELL" if float(vol_change) < 0 else "BUY"
                    if trade_type == "BUY":
                        bullish_tags = parameters.strip()
                        bearish_tags = ""
                    else:
                        bearish_tags = parameters.strip()
                        bullish_tags = ""

                    # Construct the modified row
                    modified_row = [
                        date_time,
                        screener_date,
                        "BEST_INTRADAY_STOCKS",
                        strength,
                        stock_name.strip(),
                        stock_type,
                        trade_type,
                        price,
                        vol_change,
                        row[2],
                        row[3],  # Deviation from Pivots
                        row[4],  # TODAYS RANGE
                        row[5],
                        screener_run_id,
                        f"{run_time_str}-RUN",  # run_history (first time)
                        1,  # signal_count starts at 1 for new entries
                        f"{run_time_str}-BEST_{strength}",
                        screener_rank,
                        bullish_tags,
                        bearish_tags
                    ]
                    data.append(modified_row)
                    screener_rank += 1

            # Delete the file after processing
        logger.info(f"Reading completed")
        logger.info(f"Deleting {csv_file}..")
        os.remove(csv_file)

    except FileNotFoundError:
        logger.error(f"Error: File not found at {file_name}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    return data


def get_intraday_stock_alerts(logger):
    logger.info("Starting best intraday screeners script.")
    ist_zone = pytz.timezone("Asia/Kolkata")
    time_now = datetime.now()
    ist_now = time_now.astimezone(ist_zone)
    date_time = ist_now.strftime("%Y-%m-%d %H:%M:%S")
    user_data_dir = tempfile.mkdtemp(prefix="chrome-ud-intra-alerts")


    chromeOptions = webdriver.ChromeOptions()

    logger.info("Getting Directory.")
    cwd = os.getcwd()
    download_dir= cwd
    logger.info("Directory Fetched -> " + str(download_dir))

    prefs = {"download.default_directory": download_dir}


    chromeOptions.add_experimental_option("prefs",prefs)
    chromeOptions.add_argument("--headless=new")
    chromeOptions.add_argument("--no-sandbox")
    chromeOptions.add_argument("--disable-dev-shm-usage")
    chromeOptions.add_argument(f"--user-data-dir={user_data_dir}")
    driver = webdriver.Chrome(options=chromeOptions)
    driver.set_window_size(1920, 1080)
    driver.get("https://intradayscreener.com/opening-range-breakout")
    logger.info("Starting best intraday screeners script.")

    try:
        # Step 1: Login
        logger.info("Attempting login.")
        driver.get("https://intradayscreener.com/login")
        # Fill email and password
        email_field = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/div[1]/input'))
        )
        email_field.send_keys(INTRADAY_SCREENER_EMAIL)

        password_field = driver.find_element(By.XPATH, '/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/div[2]/div/input')
        password_field.send_keys(INTRADAY_SCREENER_PWD)

        login_button = driver.find_element(By.XPATH, '/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/button')
        login_button.click()
        logger.info("Login successful.")
        # Wait for a few seconds to allow the login process to complete
        WebDriverWait(driver, 10)

        # Step 2: Use JavaScript to click 'F&O' after login
        time.sleep(8)

        chart_close_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[2]/div/div/div[1]/button'))
        )
        chart_close_button.click()
        time.sleep(5)

        session_close_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[1]/div[1]/button'))
        )
        session_close_button.click()
        time.sleep(5)

        logger.info("Windows Closed.")

        # Step 3: Expand 'Intraday' menu

        logger.info("Expanding Intraday menu.")
        intraday_menu = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[1]/div[2]/nav/div/ul/li[2]'))
        )
        intraday_menu.click()
        time.sleep(2)


        intra_alerts_link = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/div/app-home-layout/div[1]/app-nav-bar/div[1]/div[2]/nav/div/ul/li[2]/div/a[5]'))
        )
        intra_alerts_link.click()
        logger.info("Intra Alerts Link clicked.")
        time.sleep(8)
        export_tag = "//button[contains(text(), 'CSV')]"

        export = WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, export_tag))
                )
        export.click()
        logger.info(f"Export clicked")
        time.sleep(10)
        file_name = "All Intrady Alerts.csv"
        stock_type = "Intraday Alerts"
        strength = "Intraday"
        logger.info(f"Waiting for File" + str(file_name))
        wait_for_file(download_dir, file_name,logger)
        logger.info(f"File written")

        each_data = read_csv_and_delete(download_dir,file_name, stock_type=stock_type, strength=strength, date_time = date_time,logger=logger)
        write_to_db(each_data,logger)

    except TimeoutException:
        logging.error("An element failed to load in the expected time.")

    finally:
        run_completed_time = get_current_ist_time_as_str()
        print("Run Completed " + run_completed_time)
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)

def write_to_db(scraped_data, logger):
    """
    Writes the extracted data to the database in a single batch.
    Always inserts all provided records without checking for existing entries.
    """
    db_repo = SgIntradayScreenerSignalsRepository()
    session = db_repo.session
    screener_name = ""
    logger.info("▶️ Connecting to DB for bulk insert")

    if not scraped_data:
        logger.info("No data to insert; exiting.")
        return

    new_entries = []
    for record in scraped_data:
        try:
            (
                screener_run_time,
                screener_date,
                screener_type,
                screener,
                stock_name,
                stock_type,
                trade_type,
                ltp,
                vol_change,
                alerts,
                deviation_from_pivots,
                todays_range,
                level,
                run_id,
                run_history,
                signal_count,
                tags,
                screener_rank,
                bullish_milestone_tags,
                bearish_milestone_tags,
            ) = record

            # Normalize types
            if isinstance(screener_date, str):
                screener_date = datetime.strptime(screener_date, "%Y-%m-%d").date()
                screener_name = screener

            ltp_val = float(ltp) if ltp is not None else None
            vol_val = float(vol_change) if vol_change is not None else None
            sig_count = int(signal_count) if signal_count is not None else 1

            new_entries.append(
                SgIntradayScreenerSignals(
                    screener_run_time=screener_run_time,
                    screener_date=screener_date,
                    screener_type=screener_type,
                    screener=screener,
                    stock_name=stock_name,
                    stock_type=stock_type,
                    trade_type=trade_type,
                    ltp=ltp_val,
                    alerts=alerts,
                    vol_change=vol_val,
                    deviation_from_pivots=deviation_from_pivots,
                    todays_range=todays_range,
                    level=level,
                    run_id=run_id,
                    run_history=run_history,
                    signal_count=sig_count,
                    tags=tags,
                    bullish_milestone_tags=bullish_milestone_tags,
                    bearish_milestone_tags=bearish_milestone_tags,
                    screener_rank=screener_rank
                )
            )
        except Exception as e:
            logger.error(f"❌ Error processing record {record}: {e}")

    try:
        session.bulk_save_objects(new_entries)
        session.commit()
        logger.info(f"✅ Bulk insert completed: inserted {len(new_entries)} records for {screener_name}.")
    except Exception as e:
        logger.error(f"❌ Bulk insert failed: {e}")
        session.rollback()

def wait_for_file(download_dir, file_name,logger, timeout=60, poll_interval=1):
    """
    Wait until file_name appears in download_dir and (optionally) finishes writing.
    Raises TimeoutException if not ready within timeout seconds.
    """
    target = os.path.join(download_dir, file_name)
    logger.info(str(target))
    end_time = time.time() + timeout

    # 1) Wait for the file to appear
    while time.time() < end_time:
        if os.path.exists(target):
            logger.info("File exists")
            break
        logger.info("Sleeping")
        time.sleep(poll_interval)
    else:
        logger.info("TimeoutException")
        raise TimeoutException(f"Timed out after {timeout}s waiting for {file_name} to appear")

    # 2) (Optional) Wait for size to stabilize
    last_size = -1
    while time.time() < end_time:
        current_size = os.path.getsize(target)
        if current_size == last_size:
            # size hasn’t changed since last check: assume write complete
            return
        last_size = current_size
        time.sleep(poll_interval)

    raise TimeoutException(f"Timed out after {timeout}s waiting for {file_name} to finish writing")

# Main loop to keep the scheduler running
if __name__ == "__main__":
    logger = logging.getLogger("FallbackLogger")  # Initialize fallback logger
    get_intraday_stock_alerts(logger)
