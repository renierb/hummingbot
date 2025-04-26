import unittest
from decimal import Decimal

from pydantic import SecretStr

from hummingbot.connector.exchange.luno.luno_utils import (
    CENTRALIZED,
    DEFAULT_FEES,
    EXAMPLE_PAIR,
    LunoConfigMap,
    is_exchange_information_valid,
)


class LunoUtilsTests(unittest.TestCase):
    def test_constants(self):
        self.assertTrue(CENTRALIZED)
        self.assertEqual("XBT-ZAR", EXAMPLE_PAIR)
        self.assertEqual(Decimal("0.001"), DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.001"), DEFAULT_FEES.taker_percent_fee_decimal)
        self.assertTrue(DEFAULT_FEES.buy_percent_fee_deducted_from_returns)

    def test_is_exchange_information_valid_active(self):
        exchange_info = {
            "trading_status": "ACTIVE"
        }
        self.assertTrue(is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_inactive(self):
        exchange_info = {
            "trading_status": "INACTIVE"
        }
        self.assertFalse(is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_missing_status(self):
        exchange_info = {}
        self.assertFalse(is_exchange_information_valid(exchange_info))

    def test_luno_config_map(self):
        config_map = LunoConfigMap(
            luno_api_key=SecretStr("test_key"),
            luno_api_secret=SecretStr("test_secret")
        )

        # Test connector name
        self.assertEqual("luno", config_map.connector)

        # Test API key configuration
        self.assertTrue(hasattr(config_map, "luno_api_key"))
        api_key_json_schema = LunoConfigMap.model_fields["luno_api_key"].json_schema_extra
        self.assertTrue(api_key_json_schema["is_secure"])
        self.assertTrue(api_key_json_schema["is_connect_key"])
        self.assertTrue(api_key_json_schema["prompt_on_new"])

        # Test API secret configuration
        self.assertTrue(hasattr(config_map, "luno_api_secret"))
        api_secret_json_schema = LunoConfigMap.model_fields["luno_api_secret"].json_schema_extra
        self.assertTrue(api_secret_json_schema["is_secure"])
        self.assertTrue(api_secret_json_schema["is_connect_key"])
        self.assertTrue(api_secret_json_schema["prompt_on_new"])

        # Test model config
        self.assertEqual("luno", config_map.model_config["title"])
