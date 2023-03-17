"""REST client handling, including PartnerizeStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import requests
import datetime
import csv
import logging
import io
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseAPIPaginator
from requests import Response
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk._singerlib import Schema

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class DayChunkPaginator(BaseAPIPaginator):
    """A paginator that increments days in a date range."""

    def __init__(self, start_date, increment=1, *args: Any, **kwargs: Any) -> None:
        super().__init__(start_date)
        self._value = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        self._end = datetime.datetime.today()
        self._increment = increment

    @property
    def end_date(self):
        """Get the end pagination value.

        Returns:
            End date.
        """
        return self._end

    @property
    def increment(self):
        """Get the paginator increment.

        Returns:
            Increment.
        """
        return self._increment

    def get_next(self, response: Response):
        return self.current_value + datetime.timedelta(days=self.increment) if self.has_more(response) else None

    def has_more(self, response: Response) -> bool:
        """Checks if there are more days to process.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return self.current_value < self.end_date


def set_none_or_cast(value, expected_type):
    if value == '' or value is None:
        return None
    elif not isinstance(value, expected_type):
        return expected_type(value)
    else:
        return value


class PartnerizeStream(RESTStream):
    """Partnerize stream class."""

    url_base = "https://api.partnerize.com"

    def __init__(
        self,
        tap: TapBaseClass,
        name: str | None = None,
        schema: dict[str, Any] | Schema | None = None,
        path: str | None = None,
    ) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(name=name, schema=schema, tap=tap, path=path)
        self.next_page_token = self.config.get("start_date", "")

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("username", ""),
            password=self.config.get("password", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        return DayChunkPaginator(start_date=self.config.get("start_date"))

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        current_token = datetime.datetime.strptime(self.next_page_token, "%Y-%m-%d")
        if current_token <= datetime.datetime.today():
            new_token_datetime = current_token + datetime.timedelta(days=1)
            self.next_page_token = datetime.datetime.strftime(new_token_datetime, "%Y-%m-%d")
            return self.next_page_token
        return None

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {
            "publisher_id": self.config.get("publisher_id", ""),
            "ref_conversion_metric_id": "2"
        }

        next_page_date = datetime.datetime.strftime(next_page_token, "%Y-%m-%d")
        if next_page_date:
            params["start_date"] = next_page_date
            end_datetime = datetime.datetime.strptime(next_page_date, "%Y-%m-%d") + datetime.timedelta(days=1)
            params["end_date"] = datetime.datetime.strftime(end_datetime, "%Y-%m-%d")

        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """

        count = 0
        f = io.StringIO(response.text)
        for row in csv.DictReader(f):
            count += 1
            if row.get("meta_conversion_gross_value") == "undefined":
                row.pop("meta_conversion_gross_value")
            row["meta_conversion_gross_value"] = set_none_or_cast(row.get("meta_conversion_gross_value"), float)
            if row.get("meta_item_product_id") == "undefined":
                row.pop("meta_item_product_id")
            row["item_value"] = set_none_or_cast(row.get("item_value"), float)
            row["conversion_lag"] = set_none_or_cast(row.get("conversion_lag"), int)
            row["meta_conversion_delivery_cost"] = set_none_or_cast(row.get("meta_conversion_delivery_cost"), float)
            row["creative_type"] = set_none_or_cast(row.get("creative_type"), int)
            row["item_publisher_commission"] = set_none_or_cast(row.get("item_publisher_commission"), float)
            row["publisher_commission"] = set_none_or_cast(row.get("publisher_commission"), float)
            row["value"] = set_none_or_cast(row.get("value"), float)
            row["quantity"] = set_none_or_cast(row.get("quantity"), int)
            row["meta_conversion_container_version"] = set_none_or_cast(row.get("meta_conversion_container_version"), int)
            if row.get("job_id"):
                continue
            try:
                row.pop(None, None)
                row = {
                    k.replace('-', '_').lower(): v for k, v in
                       row.items()
                }  # BQ Schema doesn't recognize field names with the '-' character
            except:
                raise Exception('Check row')

            page_date = row.get("conversion_date")
            logging.info(f"Retrieved {count} records for data chunk {page_date}")
            yield row
