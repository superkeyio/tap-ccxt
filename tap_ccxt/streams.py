"""Stream type classes for tap-ccxt."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_ccxt.client import ccxtStream
from ccxt.base.exchange import Exchange

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.

class OHLCVStream(ccxtStream):
    name = 'ohlcv'
    
    replication_key = "timestamp"

    exchange: Exchange = None
    symbol: str = None
    timeframe: str = None

    schema = th.PropertiesList(
        th.Property(
            "timestamp",
            th.DateTimeType,
            required=True
        ),
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
        th.Property(
            "exchange_id",
            th.StringType,
            required=True
        ),
        th.Property(
            "symbol",
            th.StringType,
            required=True
        )
    ).to_dict()
