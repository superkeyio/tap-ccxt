"""Stream type classes for tap-ccxt."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_ccxt.client import ccxtStream
from ccxt.base.exchange import Exchange
from datetime import datetime
import math

# TODO: Delete this is if not using json files for schema definition
# SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class OHLCVStream(ccxtStream):
    name = "ohlcv"

    replication_key = "timestamp"

    exchange: Exchange = None
    symbols: List[str] = []
    timeframe: str = None

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
        th.Property("exchange_id", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        self.exchange = kwargs.pop("exchange")
        self.symbols = kwargs.pop("symbols")
        self.timeframe = kwargs.pop("timeframe")
        super().__init__(*args, **kwargs)
        self.STATE_MSG_FREQUENCY = 100

    @property
    def partitions(self) -> List[Dict[str, int]]:
        return [{"symbol": symbol} for symbol in self.symbols]

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        current_timestamp = math.floor(
            self.get_starting_timestamp(context).timestamp() * 1000
        )
        end_timestamp = datetime.now().timestamp() * 1000
        symbol = context.get("symbol")
        # segment based on exchange?
        while current_timestamp < end_timestamp:
            candles = self.exchange.fetchOHLCV(
                symbol, timeframe=self.timeframe, since=current_timestamp
            )
            for candle in candles:
                yield dict(
                    symbol=symbol,
                    exchange_id=self.exchange.id,
                    timestamp=datetime.fromtimestamp(candle[0] / 1000.0),
                    open=candle[1],
                    high=candle[2],
                    low=candle[3],
                    close=candle[4],
                    volume=candle[5],
                )
                current_timestamp = candle[0]
