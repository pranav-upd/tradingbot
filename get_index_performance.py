import schedule
import logging
# suppress SQL text logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

from algo_scripts.algotrade.scripts.trading_style.intraday.strategies.scalping_915.scalp_src.update_nse_index import \
    write_nse_to_db

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select,WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import re
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.market_context.index_snapshot import \
    IndexSnapshot, IndexSnapshotRepository

from algo_scripts.algotrade.scripts.trade_utils.time_manager import get_current_ist_time_as_str, get_today_date_as_str
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import \
    get_db_session
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.market_context.index_master import IndexMasterRepository

# 1) Configure the root logger (you can skip this and configure your named logger directly if you prefer)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)

def job():
    logger = logging.getLogger("FallbackLogger")
    load_index_performance_to_db(logger)

def load_index_performance_to_db(logger, max_retries: int = 3, backoff: int = 5):
    run_delete_snapshots_for_today(logger)
    logger.info(f"🔄 Started load_index_performance_to_db at  " + get_current_ist_time_as_str())
    URL = "https://intradayscreener.com/stock-market-today"
    last_exc = None

    for attempt in range(1, max_retries + 1):
        logger.info(f"🔄 Scrape attempt {attempt}/{max_retries}")
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_window_size(1920, 1080)
        wait = WebDriverWait(driver, 15)
        logger.info(f"🔄 Chrome Parameters loaded")

        try:
            # --- Retry the initial page load ---
            for load_try in range(1, max_retries + 1):
                try:
                    driver.get(URL)
                    break
                except WebDriverException as we:
                    logger.info("WebDriverException")
                    last_exc = we
                    msg = (
                            f"⏱️ driver.get() attempt {load_try}/{max_retries} failed: {we}"
                            + (" — retrying…" if load_try < max_retries else " — giving up on load")
                    )
                    logger.warning(msg)
                    if load_try == max_retries:
                        raise
                    time.sleep(backoff)
                    # Hardcoded XPath data (from your input)
            xpath_data = [
                {"name": "NIFTY 50", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[1]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[1]/span[4]"},
                {"name": "GIFT NIFTY", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[2]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[2]/span[4]"},
                {"name": "NIFTY BANK", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[3]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[3]/span[4]"},
                {"name": "NIFTY_MIDCAP_100", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[4]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[4]/span[4]"},
                {"name": "NIFTY_SMLCAP_100", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[5]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[5]/span[4]"},
                {"name": "NIFTY_FIN_SERVICE", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[6]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[6]/span[4]"},
                {"name": "INDIA_VIX", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[7]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[7]/span[4]"},
                {"name": "SENSEX", "value_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[8]/span[2]", "percent_xpath": "/html/body/app-root/div/app-home-layout/div[1]/app-index-panel/div/div[1]/span[8]/span[4]"}

            ]

            # Results list
            results = []

            for entry in xpath_data:
                name = entry["name"]
                logger.info(f"Extracting {name}")
                try:
                    value = wait.until(EC.presence_of_element_located((By.XPATH, entry["value_xpath"]))).text.strip()
                    logger.info(f"Extracting {value}")
                    raw_pct = driver.find_element(By.XPATH, entry["percent_xpath"]).text.strip()
                    percent = re.sub(r"[()%]", "", raw_pct)
                    logger.info(f"Extracting {percent}")
                    results.append([name, value, percent])
                except Exception as e:
                    logger.info(f"❌ Failed to extract {name}: {e}")
                    results.append([name, "N/A", "N/A"])

            # XPaths
            advances_xpath = "/html/body/app-root/div/app-home-layout/div[2]/app-dashboard/div/div[2]/div[1]/div/div/div[2]/div[1]/div/div[2]/div[1]/span"
            declines_xpath = "/html/body/app-root/div/app-home-layout/div[2]/app-dashboard/div/div[2]/div[1]/div/div/div[2]/div[1]/div/div[2]/div[1]/span/span"
            dropdown_xpath = "/html/body/app-root/div/app-home-layout/div[2]/app-dashboard/div/div[2]/div[1]/div/div/div[2]/div[1]/div/div[2]/div[1]/select"

            # Get dropdown options
            dropdown_element = wait.until(EC.presence_of_element_located((By.XPATH, dropdown_xpath)))
            dropdown = Select(dropdown_element)

            # Scrape data - advances declines
            adv_decline_records = []
            for i in range(len(dropdown.options)):
                dropdown.select_by_index(i)
                time.sleep(2)
                index_name = dropdown.options[i].text.strip()
                logger.info(str(index_name))
                adv, dec = get_advance_decline(wait,advances_xpath,declines_xpath,logger)
                adv_decline_records.append({"Index": index_name, "Advances": adv, "Declines": dec})

            logger.info(str(adv_decline_records))

            # build a map: index name → (advances, declines)
            adv_decl_map = {
                rec["Index"]: (rec["Advances"], rec["Declines"])
                for rec in adv_decline_records
            }
            all_names = {r[0] for r in results} | {rec["Index"] for rec in adv_decline_records}
            merged_data = []
            for name in all_names:
                # find value/percent if we scraped it, else default
                value, pct = next(
                    ((v,p) for n,v,p in results if n == name),
                    (0, 0)
                )
                # find adv/dec if we have it, else default
                adv, dec = adv_decl_map.get(name, (0,0))
                merged_data.append([name, value, pct, adv, dec])

            logger.info(str(merged_data))

            # # Save to CSV
            # output_file = "index_data_extracted.csv"
            # with open(output_file, "w", newline="", encoding="utf-8") as f:
            #     writer = csv.writer(f)
            #     writer.writerow(["name", "value", "percentage"])
            #     writer.writerows(results)
            #logger.info(f"✅ Done. Saved to: {output_file}")

            logger.info("✅ Scrape successful")
            write_to_db(logger, merged_data)
            #write_nse_to_db()

            return  # exit on first successful attempt


        except (TimeoutException, WebDriverException) as e:
            last_exc = e
            logger.warning(f"⚠️ Attempt {attempt} failed: {e}. Retrying in {backoff}s…")
            time.sleep(backoff)

        finally:
            try:
                driver.quit()
            except Exception:
                pass

    # if we get here, all retries failed
    logger.error(f"❌ All {max_retries} attempts failed, aborting.")
    raise last_exc

def get_advance_decline(wait,advances_xpath,declines_xpath,logger):
    full_text = wait.until(EC.presence_of_element_located((By.XPATH, advances_xpath))).text.strip()
    logger.info(f"Scraped raw text: {full_text}")
    if "|" in full_text:
        adv, dec = [val.strip() for val in full_text.split("|")]
    else:
        adv = wait.until(EC.presence_of_element_located((By.XPATH, advances_xpath))).text.strip()
        dec = wait.until(EC.presence_of_element_located((By.XPATH, declines_xpath))).text.strip()
    return int(adv), int(dec)

def write_to_db(logger, data):
    """
    data: List of [name, value_str, percent_str] as returned from load_index_performance_to_db
    """
    logger.info("Writing to DB " + get_current_ist_time_as_str())
    session = next(get_db_session())
    idx_repo = IndexMasterRepository(session)

    snapshots = []
    for name, value_str, percent_str,advance,decline in data:
        master = idx_repo.get_index_by_symbol(name)
        if not master:
            logger.warning(f"No IndexMaster found for '{name}', skipping insert.")
            continue

        # convert strings to floats
        try:
            # coerce to string, strip commas, parse
            value = float(str(value_str).replace(",", ""))
            percent = float(str(percent_str))
        except Exception as e:
            # log once and default both fields to 0.0
            logger.warning(f"Parsing numeric fields for '{name}' failed ({e}); defaulting value & percent to 0.0")
            value, percent = 0.0, 0.0

        # build snapshot instance
        snapshot = IndexSnapshot(
            index_id = master.index_id,
            index_value              = value,
            percent_change  = percent,
            snapshot_date = get_today_date_as_str(),
            total_advancing = advance,
            total_declining = decline

        )
        snapshots.append(snapshot)

    if snapshots:
        session.bulk_save_objects(snapshots)
        session.commit()
        logger.info(f"✅ Inserted {len(snapshots)} index performance snapshots." + get_current_ist_time_as_str())
    else:
        logger.info("⚠️ No snapshots to insert.")

    session.close()

def run_delete_snapshots_for_today(logger) -> int:
    """
    Production-grade runner to delete snapshots for today's date.
    Sets up logging, session, and handles errors cleanly.
    """
    logger.info("Snapshot deletion started at " + get_current_ist_time_as_str())
    deleted = 0
    session = None
    try:
        date_str = get_today_date_as_str()
        session = next(get_db_session())
        repo = IndexSnapshotRepository(session)
        deleted = repo.delete_snapshots_by_date(date_str)
    except Exception as e:
        logger.error("Snapshot deletion process failed.", exc_info=e)
        raise
    finally:
        if session:
            session.close()
            logger.info("Database session closed.")
    logger.info("Snapshot deletion completed: %d records removed " + get_current_ist_time_as_str(), deleted)
    return deleted

def main():
    schedule.every(3).minutes.do(job)
    job()
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    logger = logging.getLogger("FallbackLogger")  # Initialize fallback logger
    load_index_performance_to_db(logger)

