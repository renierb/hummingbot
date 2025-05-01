import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from hummingbot.connector.exchange.luno import luno_constants as CONSTANTS, luno_web_utils as web_utils
from hummingbot.connector.exchange.luno.luno_order_book import LunoOrderBook, SequenceGapError  # noqa
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.luno.luno_exchange import LunoExchange


class LunoAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
            self,
            trading_pairs: List[str],
            connector: "LunoExchange",
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DEFAULT_DOMAIN
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

        # --- Internal State Management ---
        # DataSource manages its own LunoOrderBook instances for internal state
        self._luno_order_books: Dict[str, LunoOrderBook] = {}

        for pair in trading_pairs:
            self._luno_order_books[pair] = LunoOrderBook(trading_pair=pair)
            self.logger().info(f"Initialized internal LunoOrderBook for {pair}.")

        # Queue only for parsed TRADE messages
        self._trade_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._message_queue.setdefault(self._trade_queue_key, asyncio.Queue())

        self.order_book_create_function = lambda: LunoOrderBook(trading_pair="")  # trading_pair will be set later

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Creates a local instance of the exchange order book for a particular trading pair
        """
        order_book = self._luno_order_books[trading_pair]
        if order_book.sequence_uid > -1:
            snapshot_msg: OrderBookMessage = order_book.order_book_snapshot_safe(order_book.sequence_uid, order_book.timestamp)
        else:
            snapshot_msg = await self._order_book_snapshot(trading_pair=trading_pair)
        order_book_copy = cast(LunoOrderBook, self.order_book_create_function())
        order_book_copy.trading_pair = trading_pair
        order_book_copy.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book_copy

    async def get_last_traded_prices(
            self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        """Fetches last traded prices using the connector's method."""
        # Use the connector's method to fetch last traded prices
        if not trading_pairs:
            return {}
        try:
            return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)
        except Exception as e:
            self.logger().error(f"Error fetching last traded price: {e}", exc_info=True)
            return {}

    async def listen_for_subscriptions(self):
        """Starts and manages WebSocket listeners for all trading pairs."""
        self.logger().info(f"Starting WebSocket listeners for {len(self._trading_pairs)} trading pairs...")

        tasks = []
        try:
            for trading_pair in self._trading_pairs:
                task = asyncio.create_task(self._listen_to_pair_stream(trading_pair))
                tasks.append(task)
            if not tasks:
                self.logger().warning("No listener tasks started.")
                return
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            self.logger().info("WebSocket listeners task cancelled.")
        except Exception:
            self.logger().error("Unexpected error in listen_for_subscriptions.", exc_info=True)
        finally:  # Ensure cleanup
            if tasks:
                self.logger().info(f"Stopping {len(tasks)} WebSocket listener tasks...")
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.wait(tasks, timeout=5.0)
            self.logger().info("WebSocket listeners stopped.")

    async def _listen_to_pair_stream(self, trading_pair: str):
        """
        Establishes and maintains WS connection for a pair, processes messages,
        uses internal LunoOrderBook state to generate SNAPSHOT messages for the
        main output queue, and parses trades into the trade queue.
        """
        ws: Optional[WSAssistant] = None
        reconnect_attempts = 0
        max_attempts = CONSTANTS.MAX_RECONNECT_ATTEMPTS
        base_delay = CONSTANTS.BASE_RECONNECT_DELAY

        # Get the internally managed LunoOrderBook instance
        try:
            order_book: LunoOrderBook = self._luno_order_books[trading_pair]
        except KeyError:
            self.logger().error(f"[{trading_pair}] Internal LunoOrderBook instance missing. Stopping listener task.")
            return

        while True:
            try:
                # 1. Connect & Authenticate
                self.logger().info(f"[{trading_pair}] Connecting to WebSocket stream...")
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                ws_url = CONSTANTS.WSS_URL.format(exchange_symbol)
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                self.logger().info(f"[{trading_pair}] WebSocket connected.")
                await self._authenticate_ws(ws)
                reconnect_attempts = 0

                # 2. Message Processing Loop
                async for raw_msg_data in ws.iter_messages():
                    timestamp = self._time()
                    ob_message: Optional[OrderBookMessage] = None  # To hold generated message
                    try:
                        parsed_msg = self._parse_raw_ws_message(raw_msg_data)
                        if parsed_msg is None or not isinstance(parsed_msg, dict):
                            continue

                        # --- Let LunoOrderBook process raw message & GENERATE standard message ---
                        is_initial_book = ("asks" in parsed_msg and "bids" in parsed_msg and "sequence" in parsed_msg)

                        if is_initial_book:
                            # Process internally and get snapshot message back
                            ob_message = order_book.process_snapshot(parsed_msg)
                            self._message_queue[self._snapshot_messages_queue_key].put_nowait(ob_message)
                            self.logger().info(f"[{trading_pair}] Initial order book snapshot processed.")
                        elif "sequence" in parsed_msg:
                            # Process internally and get snapshot message back if state changed
                            ob_message = order_book.process_update(parsed_msg)
                            self._message_queue[self._snapshot_messages_queue_key].put_nowait(ob_message)
                            # --- Separate Trade Processing ---
                            if "trade_updates" in parsed_msg and isinstance(parsed_msg["trade_updates"], list):
                                sequence = parsed_msg.get("sequence")
                                for trade_data in parsed_msg["trade_updates"]:
                                    if "sequence" not in trade_data and sequence is not None:
                                        trade_data["sequence"] = sequence
                                    trade_msg = self._parse_luno_trade(trade_data, timestamp, trading_pair)
                                    if trade_msg:
                                        # Put TRADE messages onto teh dedicated trade queue
                                        self._message_queue[self._trade_queue_key].put_nowait(trade_msg)
                        else:
                            # Handle status or other messages
                            if "status_update" in parsed_msg:
                                status = parsed_msg.get("status_update", {}).get("status", "UNKNOWN")
                                self.logger().info(f"[{trading_pair}] Received status update: {status}")
                            else:
                                self.logger().debug(f"[{trading_pair}] Received unhandled message: {parsed_msg}")

                    except SequenceGapError:
                        self.logger().warning(f"[{trading_pair}] SequenceGapError detected. Resetting internal book and reconnecting...")
                        raise ConnectionError("Sequence gap detected")  # Trigger reconnect
                    except Exception as e:
                        self.logger().error(
                            f"[{trading_pair}] Error processing message: {raw_msg_data}. Error: {e}", exc_info=True)
                        # Optional: raise ConnectionError("Message processing error") to force reconnect

                # If loop exits cleanly (server disconnect?)
                self.logger().warning(f"[{trading_pair}] WebSocket stream unexpectedly closed by server.")
                raise ConnectionError("WebSocket stream closed")

            except asyncio.CancelledError:
                self.logger().info(f"[{trading_pair}] Listener task cancelled.")
                raise  # Propagate cancellation
            except ConnectionError as ce:
                self.logger().warning(f"[{trading_pair}] Connection error: {ce}. Attempting reconnect...")
            except Exception as e:
                self.logger().error(f"[{trading_pair}] Unexpected error in WebSocket loop: {e}", exc_info=True)
            finally:
                # Ensure disconnection before retry/exit
                await ws.disconnect()
                ws = None
                self.logger().debug(f"[{trading_pair}] WebSocket disconnected.")

            # --- Reconnect Logic ---
            reconnect_attempts += 1
            if reconnect_attempts > max_attempts:
                self.logger().error(f"[{trading_pair}] Max reconnect attempts. Stopping listener.")
                break  # Exit the loop for this pair
            delay = min(60.0, base_delay * (1.5 ** reconnect_attempts))
            delay *= (1 + (2 * 0.1 * (asyncio.get_event_loop().time() % 1) - 0.1))
            self.logger().info(f"[{trading_pair}] Reconnect attempt {reconnect_attempts}/{max_attempts} in {delay:.2f}s...")
            await self._sleep(delay)

    # --- Authentication and Parsing Helpers ---
    async def _authenticate_ws(self, ws: WSAssistant):
        """Sends authentication credentials to the Luno WebSocket."""
        try:
            auth_payload = {
                "api_key_id": self._connector.api_key,
                "api_key_secret": self._connector.secret_key
            }
            await ws.send(WSJSONRequest(payload=auth_payload, is_auth_required=False))
            self.logger().info("Sent authentication request.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error during WebSocket authentication", exc_info=True)
            raise  # Raise to trigger re-connect

    def _parse_raw_ws_message(self, raw_msg_data: Any) -> Optional[Dict[str, Any]]:
        """Parses raw WebSocket message data into a dictionary."""
        parsed_msg = None
        msg_str = ""
        try:
            if isinstance(raw_msg_data, bytes):
                msg_str = raw_msg_data.decode('utf8')
            elif isinstance(raw_msg_data, str):
                msg_str = raw_msg_data
            elif hasattr(raw_msg_data, "data"):
                if isinstance(raw_msg_data.data, bytes):
                    msg_str = raw_msg_data.data.decode('utf8')
                elif isinstance(raw_msg_data.data, str):
                    msg_str = raw_msg_data.data
                elif isinstance(raw_msg_data.data, dict):
                    return raw_msg_data.data
                else:
                    self.logger().warning(f"Unexpected WSResponse data type: {type(raw_msg_data.data)}")
                    return None
            else:
                self.logger().warning(f"Unexpected raw message type: {type(raw_msg_data)}")
                return None

            if msg_str == '""':
                return None  # Keep-alive

            if not msg_str:
                return None

            parsed_msg = json.loads(msg_str)
            if not isinstance(parsed_msg, dict):
                self.logger().warning(f"Parsed JSON is not a dictionary: {parsed_msg}")
                return None

            return parsed_msg
        except json.JSONDecodeError:
            self.logger().error(f"Failed to decode JSON message: '{msg_str[:100]}...'")
            return None
        except UnicodeDecodeError:
            self.logger().error(f"Failed to decode bytes message (UTF-8): {raw_msg_data[:100]}...")
            return None
        except Exception:
            self.logger().error(f"Error parsing raw WS message: {raw_msg_data}", exc_info=True)
            return None

    def _parse_luno_trade(self, trade_data: Dict[str, Any], timestamp: float, trading_pair: str) -> Optional[OrderBookMessage]:
        """Parses a single trade item from Luno's trade_updates list."""
        try:
            base_s = trade_data.get("base")
            counter_s = trade_data.get("counter")
            seq = int(trade_data.get("sequence"))

            if base_s is None or counter_s is None:
                return None

            base = Decimal(base_s)
            counter = Decimal(counter_s)
            if base <= 0:
                return None

            price = counter / base
            taker_order_id = trade_data.get("taker_order_id", "unknown")
            trade_id = f"{int(timestamp * 1e6)}_{seq}_{taker_order_id}"
            trade_type = TradeType.BUY  # Defaulting side
            content = {
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),  # Use float value for HB compatibility
                "trade_id": trade_id,
                "update_id": seq,
                "price": str(price),
                "amount": str(base)
            }
            return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=timestamp)
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            self.logger().error(f"[{trading_pair}] Error parsing Luno trade data: {e}. Data: {trade_data}", exc_info=False)
            return None

    # --- Methods Consumed by OrderBookTracker ---

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Listens for order book messages (SNAPSHOTS in this case) generated internally."""
        # This loop consumes messages from the single output queue managed by the base class
        await self._listen_for_messages(output=output, message_type=OrderBookMessageType.DIFF)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Listens for parsed trade messages from the internal trade queue."""
        # Consumes TRADE messages put by _parse_luno_trade via _listen_to_pair_stream
        queue = self._message_queue[self._trade_queue_key]
        while True:
            try:
                trade_msg: OrderBookMessage = await queue.get()
                output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in listen_for_trades loop.")
                await self._sleep(1.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Listens for order book SNAPSHOT messages generated internally."""
        # This loop consumes messages from the single output queue managed by the base class
        await self._listen_for_messages(output=output, message_type=OrderBookMessageType.SNAPSHOT)

    async def _listen_for_messages(self, output: asyncio.Queue, message_type: OrderBookMessageType):
        """Helper function to listen for specific message types from the main output queue."""
        # Access the main output queue (shared between diffs and snapshots)
        messages_queue = self._message_queue[self._diff_messages_queue_key if message_type == OrderBookMessageType.DIFF else self._snapshot_messages_queue_key]
        while True:
            try:
                message: OrderBookMessage = await messages_queue.get()
                if message is not None and message.type == message_type:
                    output.put_nowait(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(f"Unexpected error in _listen_for_messages loop for {message_type.name}.")
                await self._sleep(1.0)

    # --- Fallback/Test Snapshot Method ---
    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """Retrieves order book snapshot via REST API (primarily for tests/fallback)."""
        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {"pair": exchange_symbol}
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            data = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_TOP_URL, domain=self._domain),
                params=params, method=RESTMethod.GET, throttler_limit_id=CONSTANTS.ORDERBOOK_TOP_URL
            )
            if "sequence" not in data:
                data["sequence"] = str(int(time.time() * 1e6))
            data["timestamp"] = data.get("timestamp", int(time.time() * 1000))
            for order in data.get("bids", []):
                order.setdefault("id", f"bid_{order['price']}")
            for order in data.get("asks", []):
                order.setdefault("id", f"ask_{order['price']}")
            return data
        except Exception as e:
            raise IOError(f"Error fetching Luno REST snapshot for {trading_pair}: {e}") from e

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Fetches REST snapshot and formats as OrderBookMessage (for tests/fallback)."""
        try:
            snapshot = await self._request_order_book_snapshot(trading_pair)
            snapshot_timestamp = self._parse_luno_timestamp(snapshot["timestamp"])
            bids = [[bid["price"], bid["volume"]] for bid in snapshot.get("bids", [])]
            asks = [[ask["price"], ask["volume"]] for ask in snapshot.get("asks", [])]
            update_id = int(snapshot["sequence"])

            return OrderBookMessage(
                message_type=OrderBookMessageType.SNAPSHOT,
                content={"trading_pair": trading_pair, "update_id": update_id, "bids": bids, "asks": asks},
                timestamp=snapshot_timestamp
            )
        except Exception as e:
            self.logger().error(f"Unexpected error fetching order book snapshot for {trading_pair}: {e}", exc_info=True)
            raise

    @staticmethod
    def _parse_luno_timestamp(timestamp_ms: Any) -> float:
        """
        Safely convert a Luno timestamp (milliseconds since epoch) into
        a UNIX timestamp in seconds (float). If parsing fails, fall back
        to the current time.
        """
        try:
            # Luno timestamps come in as milliseconds since epoch
            ms = float(timestamp_ms)
            return ms / 1000.0
        except (ValueError, TypeError):
            # If for any reason we can't parse it, return now
            return time.time()

    async def _sleep(self, delay: float):
        """Async sleep helper."""
        await asyncio.sleep(delay)
