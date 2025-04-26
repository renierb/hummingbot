import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.luno import luno_constants as CONSTANTS
from hummingbot.connector.exchange.luno.luno_auth import LunoAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.luno.luno_exchange import LunoExchange


class LunoAPIUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: LunoAuth,
                 trading_pairs: List[str],
                 connector: 'LunoExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth: LunoAuth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        Note: Luno uses the same WebSocket endpoint for user stream as for market data
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()

        # For Luno, we need to connect to the userstream WebSocket
        # URL: wss://ws.luno.com/api/1/userstream
        await ws.connect(
            ws_url="wss://ws.luno.com/api/1/userstream",
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
        )

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to user channels through the provided websocket connection.
        For Luno, authentication is required to receive user data.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            # Authenticate
            auth_message = {
                "api_key_id": self._connector.api_key,
                "api_key_secret": self._connector.secret_key
            }

            auth_request = WSJSONRequest(payload=auth_message)
            await websocket_assistant.send(auth_request)
            self.logger().info("Authenticated user stream...")

            # Luno doesn't need explicit subscription after authentication
            # All user events are sent automatically after authentication

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Unexpected error occurred subscribing to user stream channels: {e}",
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Process all messages from user stream websocket and put them into the queue
        :param websocket_assistant: the websocket assistant
        :param queue: the queue to put the messages into
        """
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if isinstance(data, bytes):
                data = data.decode('utf-8')

            if data == "\"\"":  # Empty message, likely a heartbeat
                continue

            try:
                # Parse the JSON message
                parsed_message = json.loads(data)

                # Put the parsed message in the queue
                queue.put_nowait(parsed_message)
            except json.JSONDecodeError:
                self.logger().error(f"Invalid JSON in user stream message: {data}", exc_info=True)
            except asyncio.QueueFull:
                self.logger().warning("User stream message queue full, ignoring message")
            except Exception as e:
                self.logger().error(f"Unexpected error processing user stream message: {e}", exc_info=True)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """
        Process an event message from the user stream and put it into the queue

        :param event_message: the event message from the websocket
        :param queue: the queue to put the processed event into
        """
        # Identify message type from Luno's format
        event_type = event_message.get("type")

        # Process different event types
        if event_type == "order_status":
            # Order status update
            queue.put_nowait(event_message)
        elif event_type == "order_fill":
            # Order fill (trade) update
            queue.put_nowait(event_message)
        elif event_type == "balance_update":
            # Balance update
            queue.put_nowait(event_message)
        else:
            self.logger().debug(f"Received unhandled user event type: {event_type}")
