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
                        "pairs",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property(
                                    "base",
                                    th.StringType,
                                    required=True,
                                ),
                                th.Property(
                                    "quote",
                                    th.StringType,
                                    required=True,
                                ),
                                th.Property(
                                    "timeframe",
                                    th.StringType,
                                    required=True,
                                ),
                                th.Property(
                                    "start_date",
                                    th.DateTimeType,
                                    required=True,
                                ),
                            )
                        ),
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
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [OHLCVStream(tap=self)]
