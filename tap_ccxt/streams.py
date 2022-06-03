"""Stream type classes for tap-ccxt."""

from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union, List, Iterable
import backoff
from pendulum import date
import pendulum
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

    primary_keys = ["exchange", "base", "quote", "timeframe", "timestamp"]
    replication_key = "timestamp"

    exchanges: Dict[str, Exchange]
    timeframe: str = None
    start_dates: Dict[Tuple[str, str, str], datetime]

    STATE_MSG_FREQUENCY = 1000

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
        th.Property("base", th.StringType, required=True),
        th.Property("quote", th.StringType, required=True),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.exchanges = {}
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
            for pair_config in exchange_config.get("pairs"):
                partitions.append(
                    {
                        "base": pair_config.get("base"),
                        "quote": pair_config.get("quote"),
                        "exchange": exchange_config.get("id"),
                        "timeframe": pair_config.get("timeframe"),
                    }
                )
        return partitions

    def _get_pair_config(self, context: Optional[dict]):
        exchange_config = next(
            exchange_config
            for exchange_config in self.config.get("exchanges")
            if exchange_config.get("id") == context.get("exchange")
        )
        pair_config = next(
            pair_config
            for pair_config in exchange_config.get("pairs")
            if pair_config["base"] == context.get("base")
            and pair_config["quote"] == context.get("quote")
        )
        return pair_config

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        pair_config = self._get_pair_config(context)
        current_timestamp = math.floor(
            (
                self.get_starting_timestamp(context)
                or pendulum.parse(pair_config.get("start_date"))
            ).timestamp()
            * 1000
        )
        prev_timestamp = None
        end_timestamp = datetime.now().timestamp() * 1000  # milliseconds
        base, quote = context.get("base"), context.get("quote")
        symbol = f"{base}/{quote}"
        exchange = self.exchanges[context.get("exchange")]
        timeframe = context.get("timeframe")
        while current_timestamp < end_timestamp:
            candles = exchange.fetchOHLCV(
                symbol, timeframe=timeframe, since=current_timestamp
            )
            for candle in candles:
                yield dict(
                    base=base,
                    quote=quote,
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
                # Skip a day
                current_timestamp += 1000 * 60 * 60 * 24
            prev_timestamp = current_timestamp
