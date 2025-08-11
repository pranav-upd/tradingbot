from sqlalchemy import Column, String, Float, Integer, DateTime, PrimaryKeyConstraint
from sqlalchemy.orm import Session
from datetime import datetime
from typing import List, Optional, Dict
import logging
from dotenv import load_dotenv
import json
from sqlalchemy.sql import cast
from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import distinct
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import (
    get_db_session,
    Base,
    engine,
)

# ✅ Load environment variables
load_dotenv()

# ✅ Import database manager for session handling

# Logger setup
logger = logging.getLogger(__name__)

unique_stocks = []
# ✅ Define the TVSignals model
class TVSignals(Base):
    __tablename__ = "sg_tv_signals"

    row_id = Column(Integer, autoincrement=True)
    updated_time = Column(DateTime, primary_key=True, nullable=False)  # ✅ Changed to DateTime
    exchange = Column(String(50), nullable=False)
    ticker = Column(String(50), primary_key=True, nullable=False)  # ✅ Composite primary key consistency
    trade_type = Column(String(20), nullable=False)
    order_type = Column(String(20), nullable=False)
    quantity = Column(Integer, nullable=False)
    limit_price = Column(Float, nullable=False)
    signal_time = Column(DateTime, nullable=False)
    strategy = Column(String(50), nullable=False)
    candle_interval  = Column(String(10), nullable=False)
    alert_name = Column(String(100), nullable=False)
    open_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    response_message = Column(String(200), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('updated_time', 'ticker', name='updated_time_ticker_pk'),
        {"extend_existing": True},  # ✅ Fix for duplicate table issue
    )

# ✅ Repository class for handling trade signals
class TVSignalsRepository:
    def __init__(self, db_session):
        self.db_session: Session = next(get_db_session())
    # ✅ Ensure session is stored properly

    def insert_trade_signal(self, trade_data: List) -> Dict[str, str]:
        """Inserts a single trade signal into the database and returns the inserted row_id."""
        try:
            trade_entry = TVSignals(
                updated_time=datetime.strptime(trade_data[0], "%Y-%m-%d %H:%M:%S"),
                exchange=trade_data[1],
                ticker=trade_data[2],
                trade_type=trade_data[3],
                order_type=trade_data[4],
                quantity=int(trade_data[5]),
                limit_price=float(trade_data[6]),
                signal_time=datetime.strptime(trade_data[7], "%Y-%m-%d %H:%M:%S"),
                strategy=trade_data[8],
                candle_interval=trade_data[9],
                alert_name=trade_data[10],
                open_price=float(trade_data[11]),
                close_price=float(trade_data[12]),
                high_price=float(trade_data[13]),
                low_price=float(trade_data[14]),
                response_message=(trade_data[15])
            )

            self.db_session.add(trade_entry)
            self.db_session.commit()
            self.db_session.refresh(trade_entry)  # ✅ Get the generated row_id

            logger.info(f"Inserted trade signal for {trade_entry.ticker} at {trade_entry.signal_time} with ID {trade_entry.row_id}")
            return {"status": "success", "message": f"Inserted {trade_entry.ticker} at {trade_entry.signal_time}", "row_id": trade_entry.row_id}

        except Exception as error:
            self.db_session.rollback()
            logger.error(f"Error inserting trade signal: {error}", exc_info=True)
            return {"status": "error", "message": str(error)}

    def update_response_message(self, row_id: int, response_message: str) -> Dict[str, str]:
        """Updates the response_message for a given row_id."""
        try:
            # ✅ Ensure row_id is an integer
            if not isinstance(row_id, int):
                row_id = int(row_id)

            # ✅ Debugging (Check type)
            print(f"Type of TVSignals.row_id: {type(TVSignals.row_id)}")
            print(f"Type of row_id: {type(row_id)}")

            # ✅ Fixing Query
            trade_entry = self.db_session.query(TVSignals).filter(
                cast(TVSignals.row_id, Integer) == row_id  # Ensure correct type
            ).first()

            if not trade_entry:
                return {"status": "error", "message": f"No record found with row_id {row_id}"}

            # ✅ Update response_message
            trade_entry.response_message = response_message
            self.db_session.commit()

            logger.info(f"Updated response_message for row_id {row_id}")
            return {"status": "success", "message": f"Updated response_message for row_id {row_id}"}

        except Exception as error:
            self.db_session.rollback()
            logger.error(f"Error updating response_message: {error}", exc_info=True)
            return {"status": "error", "message": str(error)}

    def delete_tv_signal(self, ticker: str, timestamp: str):
        """Deletes a specific trade signal."""
        try:
            timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")  # ✅ Convert to DateTime

            deleted_count = (
                self.db_session.query(TVSignals)
                .filter(TVSignals.ticker == ticker, TVSignals.signal_time == timestamp_obj)
                .delete()
            )
            self.db_session.commit()
            return {"status": "success", "message": f"Deleted {deleted_count} record(s)"}

        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error deleting data: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    def get_tv_signals(self, date: Optional[str] = None):
        """Fetches trade signals from the database, optionally filtering by date."""
        try:
            query = self.db_session.query(TVSignals)
            if date:
                date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                query = query.filter(cast(TVSignals.signal_time, DateTime) == date_obj)

            result = query.all()
            return [
                {
                    "updated_time": row.updated_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "exchange": row.exchange,
                    "ticker": row.ticker,
                    "trade_type": row.trade_type,
                    "order_type": row.order_type,
                    "quantity": row.quantity,
                    "limit_price": row.limit_price,
                    "signal_time": row.signal_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "strategy": row.strategy,
                    "candle_interval": row.candle_interval,
                    "alert_name": row.alert_name,
                    "open_price": row.open_price,
                    "close_price": row.close_price,
                    "high_price": row.high_price,
                    "low_price": row.low_price,
                    "response_message": row.response_message  # ✅ This is now stored and retrieved as proper JSON
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Error retrieving data: {e}", exc_info=True)
            return []

    def exists_trade_signal(self, signal_time, ticker, trade_type):
        """Checks if a trade signal already exists in the database."""
        existing_entry = self.db_session.query(TVSignals).filter(
            and_(
                TVSignals.signal_time == signal_time,
                TVSignals.ticker == ticker,
                TVSignals.trade_type == trade_type
            )
        ).first()
        return existing_entry is not None  # ✅ Return True if exists, else False

    def check_stocks_by_date_and_screener(self, stocks:list, date: str) :
        """
        Retrieves unique stock tickers along with their strategies for a given date based on signal_time.
        :param date: The date in "YYYY-MM-DD" format.
        :return: A list of dictionaries containing unique stock tickers and their strategies.
        """
        try:
            for ticker in stocks:
                date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                query = (
                    self.db_session.query(distinct(TVSignals.ticker))
                    .filter(func.DATE(TVSignals.signal_time) == str(date_obj),
                            TVSignals.ticker == ticker
                            ).all()
                )
                if type(query).__name__ == "list" and len(query)!=0:
                    unique_stocks.append(query)
                else:
                    pass
            return unique_stocks

        except Exception as e:
            logger.error(f"Error retrieving unique stocks: {e}", exc_info=True)
            return []

    def get_tv_signals_by_criteria(self, signal_date: str, trade_type: str, strategy: str):
        """
        Fetches trade signals based on signal_date, trade_type, and strategy.
        :param signal_date: Date in "YYYY-MM-DD" format.
        :param trade_type: Trade type (e.g., BUY or SELL).
        :param strategy: Trading strategy name.
        :return: List of matching trade signals.
        """
        try:
            date_obj = datetime.strptime(signal_date, "%Y-%m-%d").date()
            query = self.db_session.query(TVSignals).filter(
                func.DATE(TVSignals.signal_time) == date_obj,
                TVSignals.trade_type == trade_type,
                TVSignals.strategy == strategy
            )

            result = query.all()
            return [
                {
                    "updated_time": row.updated_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "exchange": row.exchange,
                    "ticker": row.ticker,
                    "trade_type": row.trade_type,
                    "order_type": row.order_type,
                    "quantity": row.quantity,
                    "limit_price": row.limit_price,
                    "signal_time": row.signal_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "strategy": row.strategy,
                    "candle_interval": row.candle_interval,
                    "alert_name": row.alert_name,
                    "open_price": row.open_price,
                    "close_price": row.close_price,
                    "high_price": row.high_price,
                    "low_price": row.low_price,
                    "response_message": row.response_message
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Error retrieving trade signals by criteria: {e}", exc_info=True)
            return []

    def bulk_insert_trade_signals(self, trade_data_list):
        """
        Bulk inserts trade signals into the database while skipping existing records.

        :param trade_data_list: List of trade signal data lists.
        :return: Dictionary containing insert status.
        """
        try:
            new_records = []

            for trade_data in trade_data_list:
                # ✅ Convert timestamps correctly
                updated_time = datetime.strptime(trade_data[0], "%Y-%m-%d %H:%M:%S")
                signal_time = datetime.strptime(trade_data[7], "%Y-%m-%d %H:%M:%S")

                # ✅ Skip records that already exist
                if self.exists_trade_signal(signal_time, trade_data[2], trade_data[3]):
                    print(f"Skipping existing record for {trade_data[2]} at {signal_time}")
                    continue

                # ✅ Prepare new record for insertion
                new_records.append(
                    TVSignals(
                        updated_time=updated_time,
                        exchange=trade_data[1],
                        ticker={trade_data[2]},
                        trade_type=trade_data[3],
                        order_type=trade_data[4],
                        quantity=int(trade_data[5]),
                        limit_price=float(trade_data[6]),
                        signal_time=signal_time,
                        strategy=trade_data[8],
                        candle_interval=trade_data[9],
                        alert_name=trade_data[10],
                        open_price=float(trade_data[11]),
                        close_price=float(trade_data[12]),
                        high_price=float(trade_data[13]),
                        low_price=float(trade_data[14]),
                        response_message=(trade_data[15])
                    )
                )

            # ✅ Insert new records in bulk
            if new_records:
                self.db_session.bulk_save_objects(new_records)
                self.db_session.commit()
                print(f"Inserted {len(new_records)} new trade signals successfully.")
                return {"status": "success", "message": f"Inserted {len(new_records)} records."}
            else:
                print("No new records to insert.")
                return {"status": "success", "message": "No new records to insert."}

        except Exception as error:
            self.db_session.rollback()
            print(f"Error inserting trade signals: {error}")
            return {"status": "error", "message": str(error)}


if __name__ == "__main__":
    Base.metadata.create_all(engine)
