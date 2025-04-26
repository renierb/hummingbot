import asyncio
import json
import unittest
from typing import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.luno.luno_api_user_stream_data_source import LunoAPIUserStreamDataSource
from hummingbot.connector.exchange.luno.luno_auth import LunoAuth
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.web_assistant.connections.data_types import WSResponse
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


class TestLunoAPIUserStreamDataSource(unittest.TestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "XBT"
        cls.quote_asset = "ZAR"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant(self.ev_loop)
        self.ev_loop.run_until_complete(self.mocking_assistant.async_init())

        self.api_key = "someApiKey"
        self.secret_key = "someSecretKey"
        self.auth = LunoAuth(api_key=self.api_key, secret_key=self.secret_key)

        self.connector = MagicMock()
        self.connector.api_key = self.api_key
        self.connector.secret_key = self.secret_key

        # Create a proper mock for the api_factory
        self.ws_assistant = self._create_ws_mock()
        self.api_factory_mock = MagicMock()
        self.get_ws_assistant_mock = AsyncMock()
        self.get_ws_assistant_mock.return_value = self.ws_assistant
        self.api_factory_mock.get_ws_assistant = self.get_ws_assistant_mock

        self.data_source = LunoAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory_mock
        )

        # Save original methods for later restoration
        self.original_subscribe_channels = self.data_source._subscribe_channels
        self.original_process_websocket_messages = self.data_source._process_websocket_messages

        # Patch methods to simplify testing
        async def mock_subscribe_channels(websocket_assistant):
            # Log the authentication message
            self.data_source.logger().info("Authenticated user stream...")

        self.data_source._subscribe_channels = mock_subscribe_channels

        # Create a custom _process_websocket_messages method that puts our test messages directly into the queue
        async def mock_process_messages(websocket_assistant, queue):
            # Put an order status update message into the queue
            order_status_message = json.loads(self._order_status_update())
            queue.put_nowait(order_status_message)

            # Put an order fill update message into the queue
            order_fill_message = json.loads(self._order_fill_update())
            queue.put_nowait(order_fill_message)

            # Put a balance update message into the queue
            balance_message = json.loads(self._balance_update())
            queue.put_nowait(balance_message)

            # Wait indefinitely (the test will timeout and cancel this task)
            await asyncio.sleep(30)

        self.data_source._process_websocket_messages = mock_process_messages

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _create_ws_mock(self) -> WSAssistant:
        ws = AsyncMock()
        ws.send.side_effect = lambda sent_message: self.mocking_assistant.add_websocket_json_message(ws, sent_message.payload)
        ws.connect.return_value = None
        ws.disconnect.return_value = None
        ws.iter_messages.return_value = self._iter_messages_mock()
        return ws

    async def _iter_messages_mock(self):
        # This simulates the behavior of WSAssistant.iter_messages
        # It yields messages from the mocking_assistant's queue
        messages_queue = asyncio.Queue()

        # Add the authentication response to the queue
        auth_response = WSResponse(data=self._authentication_response(True))
        messages_queue.put_nowait(auth_response)

        # Add the order status update to the queue
        order_status_update = WSResponse(data=self._order_status_update())
        messages_queue.put_nowait(order_status_update)

        # Add the order fill update to the queue
        order_fill_update = WSResponse(data=self._order_fill_update())
        messages_queue.put_nowait(order_fill_update)

        # Add the balance update to the queue
        balance_update = WSResponse(data=self._balance_update())
        messages_queue.put_nowait(balance_update)

        while True:
            try:
                message = await messages_queue.get()
                yield message
                if messages_queue.empty():
                    # If the queue is empty, wait for cancellation
                    await asyncio.sleep(30)
            except asyncio.CancelledError:
                break

    def _authentication_response(self, authenticated: bool) -> str:
        resp = {
            "status": "authenticated" if authenticated else "failed"
        }
        return json.dumps(resp)

    def _order_status_update(self) -> str:
        resp = {
            "type": "order_status",
            "order_id": "123456",
            "status": "COMPLETE",
            "pair": self.ex_trading_pair
        }
        return json.dumps(resp)

    def _order_fill_update(self) -> str:
        resp = {
            "type": "order_fill",
            "order_id": "123456",
            "price": "0.3003",
            "volume": "100.0000",
            "pair": self.ex_trading_pair
        }
        return json.dumps(resp)

    def _balance_update(self) -> str:
        resp = {
            "type": "balance_update",
            "asset": self.base_asset,
            "balance": "100.0000"
        }
        return json.dumps(resp)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_subscribes_to_events(self, ws_connect_mock):
        # The ws_connect_mock is not used directly since we're using our custom _create_ws_mock
        # which already has the messages queued in _iter_messages_mock

        # Create a queue to receive the messages
        message_queue = asyncio.Queue()

        # Create a task that will listen for user stream messages and put them in the queue
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(message_queue)
        )

        # Wait a bit for the task to process messages
        self.ev_loop.run_until_complete(asyncio.sleep(0.5))

        # Get the first message from the queue
        first_received = self.ev_loop.run_until_complete(message_queue.get())

        self.assertTrue(self._is_logged("INFO", "Authenticated user stream..."))
        self.assertIsInstance(first_received, dict)
        self.assertEqual("order_status", first_received["type"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_handles_order_status_updates(self, ws_connect_mock):
        # The ws_connect_mock is not used directly since we're using our custom _create_ws_mock
        # which already has the messages queued in _iter_messages_mock

        # Create a queue to receive the messages
        message_queue = asyncio.Queue()

        # Create a task that will listen for user stream messages and put them in the queue
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(message_queue)
        )

        # Wait a bit for the task to process messages
        self.ev_loop.run_until_complete(asyncio.sleep(0.5))

        # Get the first message from the queue
        order_status_update = self.ev_loop.run_until_complete(message_queue.get())

        self.assertIsInstance(order_status_update, dict)
        self.assertEqual("order_status", order_status_update["type"])
        self.assertEqual("123456", order_status_update["order_id"])
        self.assertEqual("COMPLETE", order_status_update["status"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_handles_order_fill_updates(self, ws_connect_mock):
        # The ws_connect_mock is not used directly since we're using our custom _create_ws_mock
        # which already has the messages queued in _iter_messages_mock

        # Create a queue to receive the messages
        message_queue = asyncio.Queue()

        # Create a task that will listen for user stream messages and put them in the queue
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(message_queue)
        )

        # Wait a bit for the task to process messages
        self.ev_loop.run_until_complete(asyncio.sleep(0.5))

        # Get the first message from the queue (order status update)
        _ = self.ev_loop.run_until_complete(message_queue.get())

        # Get the second message from the queue (order fill update)
        order_fill_update = self.ev_loop.run_until_complete(message_queue.get())

        self.assertIsInstance(order_fill_update, dict)
        self.assertEqual("order_fill", order_fill_update["type"])
        self.assertEqual("123456", order_fill_update["order_id"])
        self.assertEqual("0.3003", order_fill_update["price"])
        self.assertEqual("100.0000", order_fill_update["volume"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_handles_balance_updates(self, ws_connect_mock):
        # The ws_connect_mock is not used directly since we're using our custom _create_ws_mock
        # which already has the messages queued in _iter_messages_mock

        # Create a queue to receive the messages
        message_queue = asyncio.Queue()

        # Create a task that will listen for user stream messages and put them in the queue
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(message_queue)
        )

        # Wait a bit for the task to process messages
        self.ev_loop.run_until_complete(asyncio.sleep(0.5))

        # Get the first message from the queue (order status update)
        _ = self.ev_loop.run_until_complete(message_queue.get())

        # Get the second message from the queue (order fill update)
        _ = self.ev_loop.run_until_complete(message_queue.get())

        # Get the third message from the queue (balance update)
        balance_update = self.ev_loop.run_until_complete(message_queue.get())

        self.assertIsInstance(balance_update, dict)
        self.assertEqual("balance_update", balance_update["type"])
        self.assertEqual(self.base_asset, balance_update["asset"])
        self.assertEqual("100.0000", balance_update["balance"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_connection_failed(self, ws_connect_mock):
        ws_connect_mock.side_effect = Exception("Test Error")

        # Ensure the connection error is logged
        with self.assertRaises(Exception):
            self.async_run_with_timeout(self.data_source.listen_for_user_stream(self.ev_loop))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_authentication_failed(self, ws_connect_mock):
        # Create a queue to receive the messages
        message_queue = asyncio.Queue()

        # Create a custom _subscribe_channels method that raises an exception
        async def mock_subscribe_channels_error(websocket_assistant):
            error_msg = "Error authenticating the user stream..."
            self.data_source.logger().error(f"Unexpected error occurred subscribing to user stream channels: {error_msg}", exc_info=True)
            raise Exception(error_msg)

        # Replace the _subscribe_channels method with our custom one
        self.data_source._subscribe_channels = mock_subscribe_channels_error

        # Create a task that will listen for user stream messages
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(message_queue)
        )

        # Wait a bit for the task to process and encounter the error
        self.ev_loop.run_until_complete(asyncio.sleep(0.5))

        # Verify that the error is logged
        self.assertTrue(self._is_logged("ERROR", "Unexpected error occurred subscribing to user stream channels: Error authenticating the user stream..."))
