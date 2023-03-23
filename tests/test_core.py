"""Tests standard tap features using the built-in SDK tests library."""


from singer_sdk.testing import get_tap_test_class, get_standard_tap_tests

from tap_partnerize.tap import TapPartnerize
from singer_sdk.helpers._util import read_json_file

CONFIG_PATH = ".secrets/config.json"
SAMPLE_CONFIG = read_json_file(CONFIG_PATH)


# Run standard built-in tap tests from the SDK:
TestTapPartnerize = get_tap_test_class(
    tap_class=TapPartnerize,
    config=SAMPLE_CONFIG
)


def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapPartnerize, config=SAMPLE_CONFIG)
    for test in tests:
        test()
