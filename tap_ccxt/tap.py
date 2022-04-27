"""ccxt tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_ccxt.streams import (
    OHLCVStream,
)
import ccxt


class Tapccxt(Tap):
    """ccxt tap class."""

    name = "tap-ccxt"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "exchanges",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType, required=True),
                    th.Property(
                        "timeframe",
                        th.StringType,
                        default="1m",
                    ),
                    th.Property(
                        "symbols",
                        th.ArrayType(th.StringType),
                        required=True,
                    ),
                    th.Property(
                        "api_key",
                        th.StringType,
                    ),
                    th.Property(
                        "secret",
                        th.StringType,
                    ),
                )
            ),
            required=True,
        ),
        th.Property("start_date", th.DateTimeType),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # status
        streams = []
        # maybe I am overthinking this
        # exchange stream?
        for exchange_config in self.config.get("exchanges"):
            exchange_class = getattr(ccxt, exchange_config.get("id"))
            exchange = exchange_class(
                {
                    "apiKey": exchange_config.get("api_key"),
                    "secret": exchange_config.get("secret"),
                }
            )
            ohlcv_stream = OHLCVStream(
                tap=self,
                exchange=exchange,
                timeframe=exchange_config.get("timeframe"),
                symbols=exchange_config.get("symbols"),
            )
            streams.append(ohlcv_stream)
        return streams
