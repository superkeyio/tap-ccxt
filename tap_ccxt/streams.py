"""Stream type classes for tap-ccxt."""

from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union, List, Iterable
import backoff
from pendulum import date
import requests

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_ccxt.client import ccxtStream
from ccxt.base.exchange import Exchange
import ccxt
from datetime import datetime
import math

# TODO: Delete this is if not using json files for schema definition
# SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
class OHLCVStream(ccxtStream):
    name = "ohlcv"

    primary_keys = ["exchange", "symbol", "timeframe", "timestamp"]
    replication_key = "timestamp"

    exchanges: Dict[str, Exchange] = {}
    symbols: List[str] = []
    timeframe: str = None
    start_dates: Dict[Tuple[str, str, str], datetime] = {}

    STATE_MSG_FREQUENCY = 100

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timeframe", th.StringType),
        th.Property(
            "open",
            th.NumberType,
            required=True,
        ),
        th.Property(
            "high",
            th.NumberType,
            required=True,
        ),
        th.Property(
            "low",
            th.NumberType,
            required=True,
        ),
        th.Property(
            "close",
            th.NumberType,
            required=True,
        ),
        th.Property(
            "volume",
            th.NumberType,
            required=True,
        ),
        th.Property("exchange", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        for exchange_config in self.config.get("exchanges"):
            exchange_id = exchange_config.get("id")
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class(
                {
                    "apiKey": exchange_config.get("api_key"),
                    "secret": exchange_config.get("secret"),
                }
            )
            self.exchanges[exchange_id] = exchange

    @property
    def partitions(self) -> List[Dict[str, int]]:
        partitions = []
        for exchange_config in self.config.get("exchanges"):
            for symbol_config in exchange_config.get("symbols"):
                partitions.append(
                    {
                        "symbol": symbol_config.get("symbol"),
                        "exchange": exchange_config.get("id"),
                        "timeframe": symbol_config.get("timeframe"),
                    }
                )
        return partitions

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        current_timestamp = math.floor(
            self.get_starting_timestamp(context).timestamp() * 1000
        )
        prev_timestamp = None
        end_timestamp = datetime.now().timestamp() * 1000
        symbol = context.get("symbol")
        exchange = self.exchanges[context.get("exchange")]
        timeframe = context.get("timeframe")
        while current_timestamp < end_timestamp:
            candles = exchange.fetchOHLCV(
                symbol, timeframe=timeframe, since=current_timestamp
            )
            for candle in candles:
                yield dict(
                    symbol=symbol,
                    exchange=exchange.id,
                    timestamp=datetime.fromtimestamp(candle[0] / 1000.0),
                    open=candle[1],
                    high=candle[2],
                    low=candle[3],
                    close=candle[4],
                    volume=candle[5],
                )
                current_timestamp = candle[0]
            if current_timestamp == prev_timestamp:
                break
            prev_timestamp = current_timestamp
