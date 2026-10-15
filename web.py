import logging
import os
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from twelvedata import TDClient


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class WebsocketPipeline:
    DB_TABLE = "crypto_ticks"
    DB_COLUMNS = ["time", "symbol", "price", "day_volume"]
    MAX_BATCH_SIZE = 10
    INITIAL_RECONNECT_DELAY = 1
    MAX_RECONNECT_DELAY = 60
    HEARTBEAT_INTERVAL = 10

    def __init__(self, conn):
        self.conn = conn
        self.current_batch = []
        self.insert_counter = 0

    def _insert_values(self, data):
        if self.conn is not None:
            cursor = self.conn.cursor()
            sql = f"""
            INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)})
            VALUES %s;"""
            execute_values(cursor, sql, data)
            self.conn.commit()

    def _on_event(self, event):
        if event.get("event") == "price":
            timestamp = datetime.utcfromtimestamp(event["timestamp"])
            data = (timestamp, event["symbol"], event["price"], event.get("day_volume"))

            self.current_batch.append(data)
            print(f"Current batch size: {len(self.current_batch)}")

            if len(self.current_batch) >= self.MAX_BATCH_SIZE:
                self._insert_values(self.current_batch)
                self.insert_counter += 1
                print(f"Batch insert #{self.insert_counter} successfully pushed to Tiger Cloud!")
                self.current_batch = []

    def start(self, symbols):
        reconnect_delay = self.INITIAL_RECONNECT_DELAY

        while True:
            ws = None
            try:
                td = TDClient(apikey=os.environ["TWELVE_DATA_API_KEY"])
                ws = td.websocket(on_event=self._on_event)
                ws.subscribe(symbols)
                logger.info("Connecting to Twelve Data websocket")
                ws.connect()

                while not ws.ready:
                    time.sleep(1)

                logger.info("Twelve Data websocket connected")
                reconnect_delay = self.INITIAL_RECONNECT_DELAY

                while ws.ready:
                    ws.heartbeat()
                    time.sleep(self.HEARTBEAT_INTERVAL)

                raise ConnectionError("Twelve Data websocket connection was lost")
            except KeyboardInterrupt:
                logger.info("Stopping Twelve Data websocket pipeline")
                raise
            except Exception:
                logger.exception(
                    "Twelve Data websocket failed; retrying in %s seconds",
                    reconnect_delay,
                )
            finally:
                if ws is not None:
                    try:
                        ws.disconnect()
                    except Exception:
                        logger.exception("Error while closing Twelve Data websocket")

            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, self.MAX_RECONNECT_DELAY)


conn = psycopg2.connect(
    database="tsdb",
    host="fir8pj94gm.duywqbv764.tsdb.cloud.timescale.com",
    user="tsdbadmin",
    password=os.environ["TSDB_PASSWORD"],
    port="39679",
    sslmode="require",
)

symbols = ["BTC/USD", "ETH/USD"]
websocket = WebsocketPipeline(conn)
websocket.start(symbols=symbols)
