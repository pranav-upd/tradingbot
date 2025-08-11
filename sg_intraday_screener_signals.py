import pytz
import traceback
import os
from typing import Union,List
from datetime import datetime
from sqlalchemy import Column, String, Float, Integer, DateTime, Boolean, Text, Date, Index
from sqlalchemy.dialects.mysql import DATETIME
from dotenv import load_dotenv
from dateutil import parser
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import get_db_session, Base
from datetime import date
import logging
# suppress SQL text logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


# ✅ Load environment variables
load_dotenv()

# ✅ Define IST Timezone
IST = pytz.timezone("Asia/Kolkata")

def now_ist():
    """Return the current datetime in IST."""
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(IST)

def today_ist():
    """Return today's date in IST format (YYYY-MM-DD)."""
    return now_ist().date()

### **✅ Updated Database Model with Nullable Fields**
class SgIntradayScreenerSignals(Base):
    __tablename__ = "sg_intraday_screener_signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    screener_run_time = Column(DATETIME, nullable=True, default=now_ist)  # ✅ Nullable
    screener_date = Column(Date, nullable=False, default=today_ist)  # ✅ Must be Non-Nullable (Unique Key)
    screener_type = Column(String(50), nullable=True)  # ✅ Newly added column
    screener = Column(String(50), nullable=False)  # ✅ Must be Non-Nullable (Unique Key)
    stock_name = Column(String(50), nullable=False)  # ✅ Must be Non-Nullable (Unique Key)
    trade_type = Column(String(50), nullable=False)  # ✅ Must be Non-Nullable (Unique Key)

    is_active = Column(Boolean, nullable=True, default=True)  # ✅ Nullable
    is_processed = Column(Boolean, nullable=True, default=False)  # ✅ Nullable
    run_id = Column(Text, nullable=True)  # ✅ Nullable
    run_history = Column(Text, nullable=True)  # ✅ Nullable
    strategy = Column(String(100), nullable=True)  # ✅ Nullable
    stock_type = Column(String(100), nullable=True)  # ✅ Nullable
    tags = Column(String(2000), nullable=True)  # ✅ Nullable
    signal_count = Column(Integer, nullable=True, default=1)  # ✅ Nullable
    ltp = Column(Float, nullable=True)  # ✅ Nullable
    break_type = Column(String(50), nullable=True)  # ✅ Nullable
    ohl_type = Column(String(50), nullable=True)  # ✅ Nullable
    break_price = Column(Float, nullable=True)  # ✅ Nullable
    break_time = Column(DATETIME, nullable=True)  # ✅ Nullable
    price_change = Column(Float, nullable=True)  # ✅ Nullable
    alerts = Column(String(50), nullable=True)  # ✅ Nullable
    vol_change = Column(Float, nullable=True)  # ✅ Nullable
    oi_change = Column(Float, nullable=True)  # ✅ Nullable
    deviation_from_pivots = Column(String(100), nullable=True)  # ✅ Nullable
    stock_momentum_score = Column(Float, nullable=True)  # ✅ Nullable
    stock_outperformance_score = Column(Float, nullable=True)  # ✅ Nullable
    sector_performance_score = Column(Float, nullable=True)  # ✅ Nullable
    sector_rank = Column(Integer, nullable=True)  # ✅ Nullable
    screener_rank = Column(Integer, nullable=True)  # ✅ Nullable
    index_contribution = Column(Float, nullable=True)  # ✅ Nullable
    todays_range = Column(String(100), nullable=True)
    level = Column(Float, nullable=False)# ✅ Nullable
    S3 = Column(Float, nullable=True)  # ✅ Nullable
    S2 = Column(Float, nullable=True)  # ✅ Nullable
    S1 = Column(Float, nullable=True)  # ✅ Nullable
    R1 = Column(Float, nullable=True)  # ✅ Nullable
    R2 = Column(Float, nullable=True)  # ✅ Nullable
    R3 = Column(Float, nullable=True)  # ✅ Nullable
    bullish_milestone_tags = Column(String(2500), nullable=True)  # ✅ New Column
    bearish_milestone_tags = Column(String(2500), nullable=True)  # ✅ New Column
    updated_time = Column(DateTime, default=now_ist, onupdate=now_ist)  # ✅ Nullable
    """
    __table_args__ = (
        Index('unique_stock_entry', 'screener', 'screener_date', 'stock_name', 'trade_type', 'stock_type', unique=True),
    )
    """

### **✅ Repository Class with `is_processed` Support**
class SgIntradayScreenerSignalsRepository:
    def __init__(self):
        """Creates a new session internally so that it does not need to be passed explicitly."""
        self.session = next(get_db_session())

    def to_ist(self, dt_str):
        """Convert a datetime string (various formats) to IST datetime object."""
        if not dt_str:
            return None
        try:
            dt = parser.parse(dt_str)
            return dt.replace(tzinfo=pytz.utc).astimezone(IST)  # Convert UTC → IST
        except Exception as e:
            print(f"❌ Error converting {dt_str} to IST:", e)
            return None

    ### **✅ Modified Upsert Function with `is_processed` Column**
    def upsert(self, data: List[List[str]], screener: str):
        """
        Insert new records if they do not exist. If they exist, update them.
        """
        try:
            self.session.rollback()  # ✅ Ensure no pending transactions

            column_names = data[0]
            for row in data[1:]:
                record = {column_names[i]: row[i] if row[i] else None for i in range(len(column_names))}
                record["screener"] = screener
                record["is_active"] = True
                record["is_processed"] = False
                record["screener_date"] = today_ist()

                if "screener_run_time" in record:
                    record["screener_run_time"] = self.to_ist(record["screener_run_time"])
                if "break_time" in record:
                    record["break_time"] = self.to_ist(record["break_time"])

                # Convert numeric fields safely
                for key in ["price_change", "stock_momentum_score", "ltp", "index_contribution", "break_price", "S3", "S2", "S1", "R1", "R2", "R3"]:
                    if key in record and record[key] is not None:
                        try:
                            record[key] = float(record[key])
                        except ValueError:
                            record[key] = None

                # ✅ Standardize stock_name (strip spaces & uppercase)
                record["stock_name"] = record["stock_name"].strip().upper()

                # ✅ Log if an entry already exists
                existing_entry = self.session.query(SgIntradayScreenerSignals).filter_by(
                    screener=record["screener"],
                    screener_date=record["screener_date"],
                    stock_name=record["stock_name"],
                    trade_type=record["trade_type"]
                ).first()

                if existing_entry:
                    print(f"🔄 Updating existing entry for {record['stock_name']}")
                    existing_entry.ltp = record.get("ltp", existing_entry.ltp)
                    existing_entry.price_change = record.get("price_change", existing_entry.price_change)
                    existing_entry.updated_time = now_ist()
                    existing_entry.signal_count += 1
                    existing_entry.is_processed = False
                    existing_entry.run_history = (
                        existing_entry.run_history + f", {record['screener_run_time'].strftime('%H:%M')}-RUN"
                        if existing_entry.run_history else f"{record['screener_run_time'].strftime('%H:%M')}-RUN"
                    )
                    existing_entry.tags = (
                        existing_entry.tags + f", {record['screener_run_time'].strftime('%H:%M')}-{record['tags']}"
                        if existing_entry.tags else f"{record['screener_run_time'].strftime('%H:%M')}-{record['tags']}"
                    )

                    # ✅ Handling bullish_milestone_tags
                    existing_bullish_tags = set(existing_entry.bullish_milestone_tags.split()) if existing_entry.bullish_milestone_tags else set()
                    new_bullish_tags = set(record.get("bullish_milestone_tags", "").split())

                    # Add only the missing values
                    updated_bullish_tags = existing_bullish_tags.union(new_bullish_tags)
                    existing_entry.bullish_milestone_tags = " ".join(updated_bullish_tags) if updated_bullish_tags else None

                    # ✅ Handling bearish_milestone_tags
                    existing_bearish_tags = set(existing_entry.bearish_milestone_tags.split()) if existing_entry.bearish_milestone_tags else set()
                    new_bearish_tags = set(record.get("bearish_milestone_tags", "").split())

                    # Add only the missing values
                    updated_bearish_tags = existing_bearish_tags.union(new_bearish_tags)
                    existing_entry.bearish_milestone_tags = " ".join(updated_bearish_tags) if updated_bearish_tags else None
                else:
                    print(f"➕ Inserting new entry for {record['stock_name']}")
                    new_entry = SgIntradayScreenerSignals(**record)
                    self.session.add(new_entry)

            self.session.commit()
            print("✅ Data upserted successfully!")

        except Exception as e:
            print("❌ Error in upsert operation:")
            traceback.print_exc()
            self.session.rollback()

    def fetch_signals_by_date_stock_and_screeners(
            self,
            screener_date: Union[date, str],
            stock_name: str,
    ) -> List[SgIntradayScreenerSignals]:
        """
        Fetch all signals for a given date, stock, and one or more screeners.
        """
        # normalize date
        if isinstance(screener_date, str):
            screener_date = parser.parse(screener_date).date()

        # normalize stock_name
        stock_name = stock_name.strip().upper()

        return (
            self.session
            .query(SgIntradayScreenerSignals)
            .filter(
                SgIntradayScreenerSignals.screener_date == screener_date,
                SgIntradayScreenerSignals.stock_name == stock_name,
            )
            .all()
        )
    def delete_by_date_and_type(
            self,
            screener_date: Union[date, str],
            screener_type: str
    ) -> int:
        """
        Delete all SgIntradayScreenerSignals for the given date and screener_type.
        Returns the number of rows deleted.
        """
        # normalize date
        if isinstance(screener_date, str):
            screener_date = parser.parse(screener_date).date()

        try:
            deleted_count = (
                self.session
                .query(SgIntradayScreenerSignals)
                .filter(
                    SgIntradayScreenerSignals.screener_date == screener_date,
                    SgIntradayScreenerSignals.screener_type == screener_type
                )
                .delete(synchronize_session='fetch')
            )
            self.session.commit()
            print(f"🗑️ Deleted {deleted_count} records for date={screener_date} and type={screener_type}")
            return deleted_count

        except Exception as e:
            print("❌ Error deleting records:", e)
            self.session.rollback()
            return 0

    def delete_by_date_type_and_screeners(
            self,
            screener_date: Union[date, str],
            screener_type: str,
    ) -> int:
        """
        Delete all SgIntradayScreenerSignals for the given date, screener_type,
        and any of the screener names in the `screeners` list.
        Returns the number of rows deleted.
        """
        # normalize date
        if isinstance(screener_date, str):
            screener_date = parser.parse(screener_date).date()

        try:
            deleted_count = (
                self.session
                .query(SgIntradayScreenerSignals)
                .filter(
                    SgIntradayScreenerSignals.screener_date == screener_date,
                    SgIntradayScreenerSignals.screener_type == screener_type,
                )
                .delete(synchronize_session='fetch')
            )
            self.session.commit()
            print(f"🗑️ Deleted {deleted_count} records for "
                  f"date={screener_date}, type={screener_type}")
            return deleted_count

        except Exception as e:
            print("❌ Error deleting records:", e)
            self.session.rollback()
            return 0

### **✅ Main Function with Sample Data**
if __name__ == "__main__":
    from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import engine, Base
    Base.metadata.create_all(engine)

    # Create a repository instance
    repo = SgIntradayScreenerSignalsRepository()

    # Define dummy data
    dummy_screener = "DUMMY_SCREENER"
    dummy_screener_type = "DUMMY_TYPE"
    dummy_stock_name = "DUMMYSTOCK"
    dummy_trade_type = "BUY"
    dummy_level = 123.45
    dummy_date = today_ist()

    dummy_data = [
        ["stock_name", "trade_type", "level", "screener_type"],
        [dummy_stock_name, dummy_trade_type, dummy_level, dummy_screener_type]
    ]

    # --- Test Insertion ---
    print("\n--- Testing Insertion ---")
    print(f"Inserting dummy entry for {dummy_stock_name}")
    repo.upsert(dummy_data, dummy_screener)

    # --- Test Fetching ---
    print("\n--- Testing Fetch ---")
    fetched_signals = repo.fetch_signals_by_date_stock_and_screeners(
        screener_date=dummy_date,
        stock_name=dummy_stock_name
    )
    if fetched_signals:
        print(f"✅ Successfully fetched {len(fetched_signals)} signal(s) for {dummy_stock_name}.")
        for sig in fetched_signals:
            print(f"  - ID: {sig.id}, Screener: {sig.screener}, Stock: {sig.stock_name}, Level: {sig.level}")
    else:
        print(f"❌ Failed to fetch signal for {dummy_stock_name}.")


    # --- Test Deletion ---
    print("\n--- Testing Deletion ---")
    print(f"Deleting dummy entry with screener_type: {dummy_screener_type}")
    deleted_count = repo.delete_by_date_and_type(
        screener_date=dummy_date,
        screener_type=dummy_screener_type
    )
    if deleted_count > 0:
        print(f"✅ Successfully deleted {deleted_count} record(s).")
    else:
        print("❌ No records were deleted.")

    # --- Verify Deletion ---
    print("\n--- Verifying Deletion ---")
    fetched_signals_after_delete = repo.fetch_signals_by_date_stock_and_screeners(
        screener_date=dummy_date,
        stock_name=dummy_stock_name
    )
    if not fetched_signals_after_delete:
        print(f"✅ Verification successful. No signals found for {dummy_stock_name} after deletion.")
    else:
        print(f"❌ Verification failed. Found {len(fetched_signals_after_delete)} signal(s) for {dummy_stock_name}.")
