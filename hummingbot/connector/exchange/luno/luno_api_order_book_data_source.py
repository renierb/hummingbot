import asyncio
import json
import time
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from hummingbot.connector.exchange.luno import (  # Added web_utils
    luno_constants as CONSTANTS,
    luno_web_utils as web_utils,
)

# We rely on the LunoOrderBook from the tracker, so import its exception
from hummingbot.connector.exchange.luno.luno_order_book import LunoOrderBook, SequenceGapError  # noqa
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest  # Added RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.luno.luno_exchange import LunoExchange


class LunoAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

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

        # Set the order book create function to create LunoOrderBook instances
        self._order_book_create_function = lambda: LunoOrderBook(trading_pair="")

        # Define queue keys (only TRADE queue is actively used for output messages)
        self._trade_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        # DIFF queue is used minimally just to signal updates to the tracker
        self._diff_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        # Ensure the necessary queues exist in the inherited _message_queue
        self._message_queue.setdefault(self._trade_queue_key, asyncio.Queue())
        self._message_queue.setdefault(self._diff_queue_key, asyncio.Queue())

        # Remove unused snapshot queue
        # self._snapshot_queue_key = "order_book_snapshot"
        # self._message_queue.setdefault(self._snapshot_queue_key, asyncio.Queue())

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
        """
        Main entry point to start and manage WebSocket listeners for all trading pairs.
        """
        self.logger().info("Starting WebSocket listeners for all trading pairs...")
        tasks = []
        try:
            # Create and manage a listening task for each trading pair
            for trading_pair in self._trading_pairs:
                task = asyncio.create_task(self._listen_to_pair_stream(trading_pair))
                tasks.append(task)

            if not tasks:
                self.logger().warning("No listeners started. Ensure trading pairs are configured correctly.")
                return None  # Exit if no tasks were created

            # Wait for all tasks to complete (they run indefinitely until cancelled/error)
            return asyncio.gather(*tasks)

        except asyncio.CancelledError:
            self.logger().info("WebSocket listeners task cancelled.")
            return None  # Propagate cancellation
        except Exception:
            self.logger().error("Unexpected error in listen_for_subscriptions.", exc_info=True)
            return None

    async def _listen_to_pair_stream(self, trading_pair: str):
        """
        Establishes and maintains a WebSocket connection for a single trading pair,
        processes incoming messages, interacts with the LunoOrderBook instance,
        and handles reconnections.
        """
        ws: Optional[WSAssistant] = None
        reconnect_attempts = 0
        max_attempts = CONSTANTS.MAX_RECONNECT_ATTEMPTS
        base_delay = CONSTANTS.BASE_RECONNECT_DELAY

        # Ensure the order book instance is available
        try:
            order_book: LunoOrderBook = cast(LunoOrderBook, self._connector.order_book_tracker.order_books[trading_pair])
        except KeyError:
            self.logger().error(f"[{trading_pair}] Order book not found in tracker. Stopping listener task.")
            return

        while True:
            try:
                # 1. Establish Connection
                self.logger().info(f"[{trading_pair}] Connecting to WebSocket stream...")
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                ws_url = CONSTANTS.WSS_URL.format(exchange_symbol)
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                self.logger().info(f"[{trading_pair}] WebSocket connected.")

                # 2. Authenticate
                await self._authenticate_ws(ws)
                reconnect_attempts = 0  # Reset on successful connection and authentication

                # 3. Message Processing Loop
                async for raw_msg_data in ws.iter_messages():
                    timestamp = self._time()  # Use consistent timestamping
                    try:
                        parsed_msg = self._parse_raw_ws_message(raw_msg_data)
                        if parsed_msg is None or not isinstance(parsed_msg, dict):
                            continue  # Skip keep-alives or non-dict messages

                        # --- Message Routing & Processing ---

                        # Check if it's the initial snapshot (based on keys and book state)
                        is_initial_book = (
                            "asks" in parsed_msg and "bids" in parsed_msg and "sequence" in parsed_msg
                            and order_book._sequence == -1  # Book expects snapshot
                        )

                        if is_initial_book:
                            order_book.process_snapshot(parsed_msg)
                            # We don't need to put snapshot on a queue for tracker consumption
                            # because LunoOrderBook updates the base class internally.
                            # We *could* put a minimal signal on diff queue if tracker needs it.
                            await self._signal_tracker_update(trading_pair, parsed_msg, timestamp)

                        elif "sequence" in parsed_msg:  # Process as Update
                            # LunoOrderBook handles internal state and sequence checks
                            order_book.process_update(parsed_msg)
                            # If process_update didn't raise SequenceGapError, the update was valid (or old)
                            # We signal the tracker that an update happened
                            await self._signal_tracker_update(trading_pair, parsed_msg, timestamp)

                            # --- Separate Trade Processing ---
                            if "trade_updates" in parsed_msg and isinstance(parsed_msg["trade_updates"], list):
                                sequence = parsed_msg.get("sequence")
                                for trade_data in parsed_msg["trade_updates"]:
                                    # Add sequence from outer message if trade_data doesn't have it
                                    if "sequence" not in trade_data and sequence is not None:
                                        trade_data["sequence"] = sequence
                                    trade_msg = self._parse_luno_trade(trade_data, timestamp, trading_pair)
                                    if trade_msg:
                                        self._message_queue[self._trade_queue_key].put_nowait(trade_msg)
                        else:
                            # Handle other message types if necessary (e.g., status)
                            if "status_update" in parsed_msg:
                                # Log status but don't process further for book changes
                                status = parsed_msg.get("status_update", {}).get("status", "UNKNOWN")
                                self.logger().info(f"[{trading_pair}] Received status update: {status}")
                            else:
                                self.logger().debug(f"[{trading_pair}] Received unhandled structured message: {parsed_msg}")

                    except SequenceGapError:
                        self.logger().warning(f"[{trading_pair}] SequenceGapError detected by OrderBook. Triggering reconnect...")
                        order_book.reset_state()
                        order_book.apply_snapshot([], [], -1)  # Reset base class state too
                        raise ConnectionError("Sequence gap detected")  # Raise connection error to trigger reconnect logic
                    except Exception as e:
                        self.logger().error(
                            f"[{trading_pair}] Unexpected error processing message: {raw_msg_data}. Error: {e}",
                            exc_info=True
                        )
                        # Decide if non-sequence errors should trigger reconnect
                        # raise ConnectionError("Message processing error") # Optional: force reconnect

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
                self.logger().error(
                    f"[{trading_pair}] Maximum reconnect attempts ({max_attempts}) reached. Stopping listener."
                )
                break  # Exit the loop for this pair

            delay = min(60.0, base_delay * (1.5 ** reconnect_attempts))  # Cap delay
            delay *= (1 + (2 * 0.1 * (asyncio.get_event_loop().time() % 1) - 0.1))  # Add jitter +/- 10%
            self.logger().info(f"[{trading_pair}] Attempting reconnect {reconnect_attempts}/{max_attempts} in {delay:.2f} seconds...")
            await self._sleep(delay)

    async def _authenticate_ws(self, ws: WSAssistant):
        """Sends authentication credentials to the Luno WebSocket."""
        # (Keep implementation from previous refactoring)
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
        # (Keep implementation from previous refactoring)
        parsed_msg = None
        msg_str = ""
        try:
            if isinstance(raw_msg_data, bytes):
                msg_str = raw_msg_data.decode('utf8')
            elif isinstance(raw_msg_data, str):
                msg_str = raw_msg_data
            elif hasattr(raw_msg_data, "data"):  # Handle WSResponse like objects
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
            # Use sequence from outer message if trade item doesn't have it (it usually does in Luno)
            seq = int(trade_data.get("sequence"))

            if base_s is None or counter_s is None:
                return None

            base = Decimal(base_s)
            counter = Decimal(counter_s)

            if base <= 0:
                return None

            price = counter / base
            # Luno doesn't provide a unique trade ID here. Generate one.
            taker_order_id = trade_data.get("taker_order_id", "unknown")
            trade_id = f"{int(timestamp * 1e6)}_{seq}_{taker_order_id}"

            # --- Trade Side Inference (Still Ambiguous) ---
            # Defaulting to BUY as side is not provided reliably.
            # Add comment explaining this limitation.
            trade_type = TradeType.BUY
            # self.logger().debug(f"[{trading_pair}] Trade side inferred/defaulted to {trade_type.name} due to Luno API limitations.")

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

    async def _signal_tracker_update(self, trading_pair: str, raw_msg: Dict[str, Any], timestamp: float):
        """Puts a minimal message on the diff queue to signal the tracker."""
        # The tracker uses this queue to know when to potentially update strategies etc.
        # We don't need to send full diff data as LunoOrderBook handles the state.
        minimal_diff_msg = OrderBookMessage(
            OrderBookMessageType.DIFF,
            content={
                "trading_pair": trading_pair,
                "update_id": int(raw_msg["sequence"]),
                "bids": [],  # No data needed here
                "asks": []  # No data needed here
            },
            timestamp=timestamp
        )
        self._message_queue[self._diff_queue_key].put_nowait(minimal_diff_msg)

    # --- Methods Consumed by OrderBookTracker ---

    async def listen_for_order_book_diffs(
            self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue
    ):
        """Listens for messages signalling order book updates."""
        # This consumes the minimal messages put by _signal_tracker_update
        queue = self._message_queue[self._diff_queue_key]
        while True:
            update_signal_msg = await queue.get()
            await output.put(update_signal_msg)

    async def listen_for_trades(
            self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue
    ):
        """Listens for parsed trade messages."""
        # Consumes messages put by _parse_luno_trade via _listen_to_pair_stream
        queue = self._message_queue[self._trade_queue_key]
        while True:
            trade_msg = await queue.get()  # Already an OrderBookMessage
            await output.put(trade_msg)

    async def listen_for_order_book_snapshots(
            self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue
    ):
        """
        Listens for order book snapshots.
        NOTE: In this implementation, the initial snapshot is processed directly
        via WebSocket in `_listen_to_pair_stream`. This method remains for
        compatibility but won't receive snapshots unless explicitly put on a
        snapshot queue (which is currently removed). The REST fallback logic
        is removed to rely solely on the WebSocket flow.
        """
        self.logger().info("listen_for_order_book_snapshots started, but relies on WebSocket initial snapshot.")
        # Keep the loop structure for compatibility, but it won't do anything
        # unless snapshots are manually put onto a queue this method listens to.
        while True:
            # If a snapshot queue were used:
            # snapshot_msg = await self._message_queue[self._snapshot_queue_key].get()
            # await output.put(snapshot_msg)
            # For now, just sleep to prevent busy-looping if called unexpectedly
            await asyncio.sleep(3600)  # Sleep for a long time

    # --- Fallback/Test Snapshot Method ---
    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """Retrieves order book snapshot via REST API (primarily for tests/fallback)."""
        # (Keep implementation from previous refactoring)
        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {"pair": exchange_symbol}
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            data = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_TOP_URL, domain=self._domain),
                params=params, method=RESTMethod.GET, throttler_limit_id=CONSTANTS.ORDERBOOK_TOP_URL,
            )
            # Add Luno WS format fields if missing
            if "sequence" not in data:
                data["sequence"] = str(int(time.time() * 1e6))  # Fallback sequence
            data["timestamp"] = data.get("timestamp", int(time.time() * 1000))  # Ensure timestamp
            for order in data.get("bids", []):
                order.setdefault("id", f"bid_{order['price']}")
            for order in data.get("asks", []):
                order.setdefault("id", f"ask_{order['price']}")
            return data
        except Exception as e:
            raise IOError(f"Error fetching Luno REST snapshot for {trading_pair}: {e}") from e

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Fetches REST snapshot and formats as OrderBookMessage (for tests/fallback)."""
        # (Keep implementation from previous refactoring)
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
