"""Tests for Partnerize response processing."""

import logging

import pytest

from tap_partnerize.client import PartnerizeStream


def processed_conversion_row(**overrides):
    row = {
        "meta_conversion_gross_value": "24.50",
        "item_value": "20.00",
        "conversion_lag": "1",
        "meta_conversion_delivery_cost": "4.95",
        "creative_type": "2",
        "item_publisher_commission": "1.25",
        "publisher_commission": "1.25",
        "value": "20.00",
        "quantity": "1",
        "meta_conversion_container_version": "3",
        "conversion_id": "conversion-123",
        "conversion_item_id": "item-456",
        "publisher_reference": "pub-ref",
        "advertiser_reference": "adv-ref",
    }
    row.update(overrides)

    stream = PartnerizeStream.__new__(PartnerizeStream)
    return stream.post_process(row)


def test_post_process_keeps_numeric_delivery_cost():
    row = processed_conversion_row(meta_conversion_delivery_cost="4.95")

    assert row["meta_conversion_delivery_cost"] == 4.95


def test_post_process_ignores_non_numeric_delivery_cost(caplog):
    caplog.set_level(logging.WARNING, logger="tap_partnerize.client")

    row = processed_conversion_row(meta_conversion_delivery_cost="Evening")

    assert row["meta_conversion_delivery_cost"] is None
    assert "meta_conversion_delivery_cost" in caplog.text
    assert "Evening" in caplog.text
    assert "conversion-123" in caplog.text
    assert "item-456" in caplog.text


def test_post_process_still_raises_for_other_non_numeric_fields():
    with pytest.raises(ValueError):
        processed_conversion_row(item_value="Standard")
