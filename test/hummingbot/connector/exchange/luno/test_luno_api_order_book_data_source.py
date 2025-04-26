import asyncio
import logging
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Dict
from unittest.mock import AsyncMock, patch

from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.luno.luno_api_order_book_data_source import LunoAPIOrderBookDataSource
from hummingbot.connector.exchange.luno.luno_exchange import LunoExchange
from hummingbot.connector.exchange.luno.luno_order_book import LunoOrderBook
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class TestLunoAPIOrderBookDataSource(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "XBT"
        cls.quote_asset = "ZAR"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_tasks = []
        self.resume_test_event = asyncio.Event()
        self.mocking_assistant = NetworkMockingAssistant()

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = LunoExchange(
            client_config_map=client_config_map,
            luno_api_key="",
            luno_api_secret="",
            trading_pairs=[],
            trading_required=False
        )

        # Mock tracker with a single LunoOrderBook
        self.mock_tracker = AsyncMock()
        self.mock_tracker.order_books = {self.trading_pair: LunoOrderBook(trading_pair=self.trading_pair)}

        self.ob_data_source = LunoAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
        )
        self.ob_data_source._order_book_tracker = self.mock_tracker

        # Capture logs
        self.ob_data_source.logger().setLevel(logging.DEBUG)
        self.ob_data_source.logger().addHandler(self)

        # Map symbols
        self.connector._set_trading_pair_symbol_map(bidict({
            "XBTZAR": self.trading_pair,
            "ETHZAR": "ETH-ZAR"
        }))

    async def asyncTearDown(self) -> None:
        for task in self.listening_tasks:
            if not task.done():
                task.cancel()
        await super().asyncTearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, level: str, message: str) -> bool:
        return any(r.levelname == level and re.search(message, r.getMessage()) for r in self.log_records)

    @staticmethod
    def get_rest_snapshot_mock() -> Dict:
        return {
            "timestamp": 1630556205455,
            "bids": [{"price": "0.3003", "volume": "4146.5645"}],
            "asks": [{"price": "0.3004", "volume": "1553.6412"}]
        }

    @patch("hummingbot.connector.exchange_py_base.ExchangePyBase._api_get")
    async def test_get_new_order_book(self, mock_api_get):
        mock_api_get.return_value = self.get_rest_snapshot_mock()

        ob = await self.ob_data_source.get_new_order_book(self.trading_pair)
        self.assertIsInstance(ob, OrderBook)
        bids = list(ob.bid_entries())
        asks = list(ob.ask_entries())
        self.assertEqual(1, len(bids))
        self.assertEqual(0.3003, bids[0].price)
        self.assertEqual(4146.5645, bids[0].amount)
        self.assertEqual(1, len(asks))
        self.assertEqual(0.3004, asks[0].price)
        self.assertEqual(1553.6412, asks[0].amount)

    @patch("hummingbot.connector.exchange_py_base.ExchangePyBase._api_get")
    async def test_get_new_order_book_raises_exception(self, mock_api_get):
        mock_api_get.side_effect = ValueError("Test error")
        with self.assertRaises(ValueError):
            await self.ob_data_source.get_new_order_book(self.trading_pair)

    @patch("hummingbot.connector.exchange.luno.luno_api_order_book_data_source.LunoAPIOrderBookDataSource._listen_to_pair_stream", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_starts_listeners(self, mock_listen):
        self.ob_data_source._trading_pairs = ["XBT-ZAR", "ETH-ZAR"]
        self.ob_data_source._order_book_tracker.order_books = {
            "XBT-ZAR": LunoOrderBook(trading_pair="XBT-ZAR"),
            "ETH-ZAR": LunoOrderBook(trading_pair="ETH-ZAR")
        }
        mock_listen.side_effect = asyncio.CancelledError
        task = self.local_event_loop.create_task(self.ob_data_source.listen_for_subscriptions())
        self.listening_tasks.append(task)
        await asyncio.sleep(0.01)
        self.assertEqual(2, mock_listen.call_count)
        task.cancel()

    @patch("hummingbot.core.web_assistant.ws_assistant.WSAssistant.connect", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.luno.luno_api_order_book_data_source.LunoAPIOrderBookDataSource._sleep", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.luno.luno_exchange.LunoExchange.exchange_symbol_associated_to_pair")
    async def test_listen_to_pair_stream_reconnect_logic(self, exchange_symbol_mock, sleep_mock, connect_mock):
        exchange_symbol_mock.return_value = self.ex_trading_pair
        connect_mock.side_effect = [ConnectionRefusedError, ConnectionRefusedError, ConnectionRefusedError, None]
        sleep_mock.return_value = None

        # Create a mock for iter_messages that will yield one message and then raise CancelledError
        async def mock_iter_messages():
            # Allow the fourth connection attempt to happen before raising CancelledError
            if connect_mock.call_count < 4:
                # Wait a bit to allow the fourth connection attempt
                await asyncio.sleep(0.1)
            # Raise CancelledError after the fourth connection attempt
            raise asyncio.CancelledError()
            # This code is unreachable but included for clarity
            yield None

        with patch("hummingbot.core.web_assistant.ws_assistant.WSAssistant.iter_messages", return_value=mock_iter_messages()):
            task = self.local_event_loop.create_task(self.ob_data_source._listen_to_pair_stream(self.trading_pair))
            self.listening_tasks.append(task)
            try:
                await asyncio.wait_for(task, timeout=5)  # Add timeout to prevent test from hanging
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass  # Expected to be cancelled or timeout

            # Verify the expected behavior
            self.assertEqual(3, connect_mock.call_count)
            self.assertTrue(self._is_logged("INFO", r"Attempting reconnect 1/10 in .* seconds..."))

    async def test_parse_luno_trade(self):
        trade_data = {"base": "0.01", "counter": "100.0", "sequence": 102,
                      "maker_order_id": "m1", "taker_order_id": "t1"}
        timestamp = 1600000002.0
        msg = self.ob_data_source._parse_luno_trade(trade_data, timestamp, self.trading_pair)
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)
        self.assertEqual("0.01", msg.content["amount"])
        self.assertAlmostEqual(10000.0, float(msg.content["price"]))
        self.assertEqual(102, msg.content["update_id"])
        self.assertIn("_102_t1", msg.content["trade_id"])
        self.assertAlmostEqual(timestamp, msg.timestamp)
        self.assertEqual(TradeType.BUY.name.lower(), msg.content["trade_type"])

    # --- Additional tests for auxiliary methods ---

    async def test_get_last_traded_prices_empty(self):
        result = await self.ob_data_source.get_last_traded_prices([])
        self.assertEqual({}, result)

    async def test_get_last_traded_prices_batch(self):
        mc = AsyncMock()
        mc._get_last_traded_price.side_effect = [Decimal("1.23"), Decimal("4.56")]
        ds = LunoAPIOrderBookDataSource(trading_pairs=["A-B", "C-D"], connector=mc,
                                        api_factory=self.connector._web_assistants_factory)
        res = await ds.get_last_traded_prices(["A-B", "C-D"])
        self.assertEqual({"A-B": 1.23, "C-D": 4.56}, res)

    async def test_get_last_traded_prices_individual(self):
        mc = AsyncMock()
        mc._get_last_traded_price.return_value = Decimal("7.89")
        ds = LunoAPIOrderBookDataSource(trading_pairs=["X-Y"], connector=mc,
                                        api_factory=self.connector._web_assistants_factory)
        res = await ds.get_last_traded_prices(["X-Y"])
        self.assertEqual({"X-Y": 7.89}, res)
        mc._get_last_traded_price.assert_awaited_once_with(trading_pair="X-Y")

    async def test_get_last_traded_prices_exception(self):
        mc = AsyncMock()
        mc._get_last_traded_price.side_effect = Exception("fail")
        ds = LunoAPIOrderBookDataSource(trading_pairs=["A-B"], connector=mc,
                                        api_factory=self.connector._web_assistants_factory)
        ds.logger().addHandler(self)
        res = await ds.get_last_traded_prices(["A-B"])
        self.assertEqual({}, res)
        self.assertTrue(self._is_logged("ERROR", "Error fetching last traded price"))

    def test_parse_raw_ws_message_bytes_and_str(self):
        ds = self.ob_data_source
        self.assertEqual({"a": 1}, ds._parse_raw_ws_message(b'{"a":1}'))
        self.assertEqual({"b": 2}, ds._parse_raw_ws_message('{"b":2}'))

    def test_parse_raw_ws_message_empty_and_keepalive(self):
        ds = self.ob_data_source
        self.assertIsNone(ds._parse_raw_ws_message(''))
        self.assertIsNone(ds._parse_raw_ws_message('""'))

    def test_parse_raw_ws_message_wsresponse_and_unexpected(self):
        class Fake:
            data = b'{"k":3}'

        ds = self.ob_data_source
        ds.logger().setLevel(logging.WARNING)
        ds.logger().addHandler(self)
        self.assertEqual({"k": 3}, ds._parse_raw_ws_message(Fake()))
        self.assertIsNone(ds._parse_raw_ws_message(123))
        self.assertTrue(self._is_logged("WARNING", "Unexpected raw message type"))

    async def test_authenticate_ws_success_and_failure(self):
        ws = AsyncMock()
        await self.ob_data_source._authenticate_ws(ws)
        ws.send.assert_awaited()
        sent = ws.send.call_args[0][0]
        self.assertEqual(self.connector.api_key, sent.payload["api_key_id"])
        self.assertTrue(self._is_logged("INFO", "Sent authentication request"))

        # failure path
        ws2 = AsyncMock()
        ws2.send.side_effect = RuntimeError("error")
        self.ob_data_source.logger().addHandler(self)
        with self.assertRaises(RuntimeError):
            await self.ob_data_source._authenticate_ws(ws2)
        self.assertTrue(self._is_logged("ERROR", "Unexpected error during WebSocket authentication"))
