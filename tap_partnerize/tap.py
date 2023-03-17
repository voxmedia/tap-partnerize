"""Partnerize tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_partnerize import streams


class TapPartnerize(Tap):
    """Partnerize tap class."""

    name = "tap-partnerize"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The username to authenticate the Partnerize account",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The password to authenticate the Partnerize account",
        ),
        th.Property(
            "publisher_id",
            th.StringType,
            required=True,
            description="The partner id",
        ),
        th.Property(
            "start_date",
            th.DateType,
            required=True,
            description="The earliest record date to retrieve",
            default="2020-01-01",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.PartnerizeStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ConversionItemsStream(self),
        ]


if __name__ == "__main__":
    TapPartnerize.cli()
