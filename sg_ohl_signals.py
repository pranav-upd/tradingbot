import pytz
from datetime import datetime, date
from sqlalchemy import Column, String, Float, Integer, Date, Index,DateTime, func
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from dateutil import parser
from typing import Union

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import (
    get_db_session,
    Base,
    engine,
)

load_dotenv()

IST = pytz.timezone("Asia/Kolkata")

def now_ist():
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(IST)

def today_ist() -> date:
    return now_ist().date()

# Define the OHL model
class SgOhlSignals(Base):
    __tablename__ = "sg_ohl_signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    screener_run_id = Column(String(50), nullable=True)
    screener_date = Column(Date, nullable=False, default=today_ist)
    screener_type = Column(String(50), nullable=True)
    screener = Column(String(50), nullable=False)
    stock_name = Column(String(50), nullable=False)
    trade_type = Column(String(50), nullable=False)
    screener_rank = Column(Integer, nullable=True)
    price = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    percentage = Column(Float, nullable=False)
    momentum = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    deviation_from_pivots = Column(String(50), nullable=False)
    todays_range = Column(String(50), nullable=False)
    ohl = Column(String(20), nullable=False)
    stock_type = Column(String(10), nullable=False)
    weekly_trend = Column(String(50), nullable=True)
    sector = Column(String(1000), nullable=True)  # ✅ New Column
    bullish_milestone_tags = Column(String(2500), nullable=True)  # ✅ New Column
    bearish_milestone_tags = Column(String(2500), nullable=True)  # ✅ New Column
    last_updated_time = Column(DateTime, default=now_ist, onupdate=now_ist, nullable=False)

    __table_args__ = (
        Index(
            "unique_stock_entry",
            "screener",
            "screener_date",
            "stock_name",
            "trade_type",
            "stock_type",
            unique=True,
        ),
    )

class SgOhlSignalsRepository:
    def __init__(self):
        self._session_factory = get_db_session

    def _get_session(self):
        return next(self._session_factory())

    def insert(self, data: list):
        with self._get_session() as session:
            try:
                entry = SgOhlSignals(
                    screener_run_id=data[0],
                    screener_date=data[1] if data[1] else today_ist(),
                    screener_type=data[2],
                    screener=data[3],
                    stock_name=data[4],
                    trade_type=data[5],
                    screener_rank=data[6],
                    price=data[7],
                    change=data[8],
                    percentage=data[9],
                    momentum=data[10],
                    open=data[11],
                    deviation_from_pivots=data[12],
                    todays_range=data[13],
                    ohl=data[14],
                    stock_type=data[15],
                    weekly_trend=data[16] if len(data) > 16 else None,
                    sector=data[17] if len(data) > 17 else None,
                    bullish_milestone_tags=data[18] if len(data) > 18 else None,
                    bearish_milestone_tags=data[19] if len(data) > 19 else None,
                )
                session.add(entry)
                session.commit()
            except Exception as e:
                session.rollback()
                print("Error inserting data:", e)

    def get_data(self, date: str | None = None):
        with self._get_session() as session:
            try:
                query = session.query(SgOhlSignals)
                if date:
                    from datetime import datetime as _dt
                    date_obj = _dt.strptime(date, "%Y-%m-%d").date()
                    query = query.filter(func.date(SgOhlSignals.screener_date) == date_obj)
                result = query.all()
                return [
                    [
                        row.screener_run_id,
                        row.screener_date,
                        row.screener_type,
                        row.screener,
                        row.stock_name,
                        row.trade_type,
                        row.screener_rank,
                        row.price,
                        row.change,
                        row.percentage,
                        row.momentum,
                        row.open,
                        row.deviation_from_pivots,
                        row.todays_range,
                        row.ohl,
                        row.stock_type,
                        row.weekly_trend,
                        row.sector,
                        row.bullish_milestone_tags,
                        row.bearish_milestone_tags,
                    ]
                    for row in result
                ]
            except Exception as e:
                print("Error retrieving data:", e)
                return []

    def get_by_screener_date_and_screener(self, screener_date: str | date):
        with self._get_session() as session:
            try:
                if isinstance(screener_date, str):
                    from datetime import datetime as _dt
                    date_obj = _dt.strptime(screener_date, "%Y-%m-%d").date()
                else:
                    date_obj = screener_date
                result = (
                    session.query(SgOhlSignals)
                    .filter(
                        SgOhlSignals.screener_date == date_obj,
                        #SgOhlSignals.screener == screener,
                        )
                    .all()
                )
                return [
                    [
                        row.screener_run_id,
                        row.screener_date,
                        row.screener_type,
                        row.screener,
                        row.stock_name,
                        row.trade_type,
                        row.screener_rank,
                        row.price,
                        row.change,
                        row.percentage,
                        row.momentum,
                        row.open,
                        row.deviation_from_pivots,
                        row.todays_range,
                        row.ohl,
                        row.stock_type,
                        row.weekly_trend,
                        row.sector,
                        row.bullish_milestone_tags,
                        row.bearish_milestone_tags,
                    ]
                    for row in result
                ]
            except Exception as e:
                print("Error retrieving data by screener_date and screener:", e)
                return []

    def update_weekly_trend(
            self,
            screener_date: str | date,
            screener: str,
            stock_name: str,
            weekly_trend: str,
    ):
        with self._get_session() as session:
            try:
                if isinstance(screener_date, str):
                    from datetime import datetime as _dt
                    date_obj = _dt.strptime(screener_date, "%Y-%m-%d").date()
                else:
                    date_obj = screener_date
                (
                    session.query(SgOhlSignals)
                    .filter(
                        SgOhlSignals.screener_date == date_obj,
                        SgOhlSignals.screener == screener,
                        SgOhlSignals.stock_name == stock_name,
                        )
                    .update({"weekly_trend": weekly_trend})
                )
                session.commit()
            except Exception as e:
                session.rollback()
                print("Error updating weekly_trend:", e)

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

        with self._get_session() as session:
            try:
                deleted_count = (
                    session
                    .query(SgOhlSignals)
                    .filter(
                        SgOhlSignals.screener_date == screener_date,
                        SgOhlSignals.screener_type == screener_type
                    )
                    .delete(synchronize_session='fetch')
                )
                session.commit()
                print(f"🗑️ Deleted {deleted_count} records for date={screener_date} and type={screener_type}")
                return deleted_count

            except Exception as e:
                print("❌ Error deleting records:", e)
                session.rollback()
                return 0


if __name__ == "__main__":
    Base.metadata.create_all(engine)

    # Example usage:
    repo = SgOhlSignalsRepository()

    # Create a dummy entry
    dummy_data = [
        "dummy_run_id",
        today_ist(),
        "dummy_screener_type",
        "dummy_screener",
        "DUMMY_STOCK",
        "BUY",
        1,
        100.0,
        1.0,
        1.0,
        10.0,
        99.0,
        "R1",
        "10%",
        "OHL",
        "dummy",
        "UP",
        "dummy_sector",
        "dummy_bull_tag",
        "dummy_bear_tag",
    ]
    print("Inserting dummy data...")
    repo.insert(dummy_data)
    print("Dummy data inserted.")

    # Verify insertion
    retrieved_data = repo.get_by_screener_date_and_screener(screener_date=today_ist())
    print(f"Retrieved {len(retrieved_data)} records for today.")
    is_inserted = any(d[4] == "DUMMY_STOCK" for d in retrieved_data)
    print(f"Dummy entry inserted successfully: {is_inserted}")

    # Delete the dummy entry
    print("Deleting dummy data...")
    deleted_count = repo.delete_by_date_and_type(
        screener_date=today_ist(), screener_type="dummy_screener_type"
    )
    print(f"Deleted {deleted_count} record(s).")

    # Verify deletion
    retrieved_data_after_delete = repo.get_by_screener_date_and_screener(screener_date=today_ist())
    is_deleted = not any(d[4] == "DUMMY_STOCK" for d in retrieved_data_after_delete)
    print(f"Dummy entry deleted successfully: {is_deleted}")
