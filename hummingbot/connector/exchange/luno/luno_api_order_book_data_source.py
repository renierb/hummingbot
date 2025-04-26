import asyncio
import json
import time
from decimal import Decimal, InvalidOperation  # Added for trade parsing
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.luno import luno_constants as CONSTANTS
from hummingbot.connector.exchange.luno.luno_order_book import SequenceGapError
from hummingbot.core.data_type.common import TradeType  # Added
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.luno.luno_exchange import LunoExchange


class LunoAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'LunoExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):  # Added domain
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain  # Store domain if needed for URLs

        # Define keys for internal message queues
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        # Note: DIFF queue is not actively used as LunoOrderBook updates base via snapshots
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        # Ensure the queues exist
        self._message_queue.setdefault(self._trade_messages_queue_key, asyncio.Queue())
        self._message_queue.setdefault(self._diff_messages_queue_key, asyncio.Queue())

        # Initialize order book tracker attribute
        # This is needed for compatibility with existing code and tests
        # In a typical design, the tracker would have a reference to the data source, not the other way around
        self._order_book_tracker = None

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """
        Fetches the last traded prices for the given trading pairs.
        (Keep this method as it's standard for DataSource)
        """
        # Use instance domain if not provided
        api_domain = domain or self._domain
        results = {}
        if trading_pairs:
            try:
                # Use individual calls to get last traded price for each trading pair
                for trading_pair in trading_pairs:
                    try:
                        price = await self._connector._get_last_traded_price(trading_pair=trading_pair)
                        results[trading_pair] = float(price)
                    except Exception:
                        self.logger().exception(f"Error fetching last traded price for {trading_pair} on {api_domain}.")
            except Exception:
                self.logger().exception(f"Error fetching last traded prices for {trading_pairs} on {api_domain}.")

        return results

    async def listen_for_subscriptions(self):
        """
        Connects to the Luno WebSocket streams for each trading pair and listens
        for messages. Routes messages to the appropriate LunoOrderBook instance
        and parses public trade messages.
        """
        self.logger().info("Starting WebSocket listeners for all trading pairs...")
        tasks = []
        try:
            # Create and manage a listening task for each trading pair
            for trading_pair in self._trading_pairs:
                task = asyncio.create_task(self._listen_to_pair_stream(trading_pair))
                tasks.append(task)
            # Keep the main task alive, handling potential task failures if needed
            # Or simply wait for all tasks to complete (which they shouldn't unless cancelled/error)
            if tasks:
                await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            self.logger().info("WebSocket listeners task cancelled.")
        except Exception:
            self.logger().error(
                "Unexpected error in listen_for_subscriptions. Sibling task monitoring might be needed.",
                exc_info=True
            )
        finally:
            # Ensure all child tasks are cancelled on exit
            for task in tasks:
                if not task.done():
                    task.cancel()
            self.logger().info("WebSocket listeners stopped.")

    def _create_order_book_message(self, msg: Dict[str, Any], timestamp: float,
                                   message_type: OrderBookMessageType, trading_pair: str) -> Optional[OrderBookMessage]:
        """
        Creates an OrderBookMessage from a WebSocket message.

        :param msg: The WebSocket message
        :param timestamp: The timestamp of the message in seconds
        :param message_type: The type of the message (SNAPSHOT or DIFF)
        :param trading_pair: The trading pair the message is for
        :return: An OrderBookMessage
        """
        # Calculate timestamp in milliseconds, preserving full precision
        timestamp_ms = int(timestamp * 1000)
        try:
            if message_type == OrderBookMessageType.SNAPSHOT:
                # Process snapshot message
                bids = []
                asks = []

                # Extract bids and asks
                for bid in msg.get("bids", []):
                    if isinstance(bid, list) and len(bid) >= 2:
                        bids.append([bid[0], bid[1]])
                    elif isinstance(bid, dict) and "price" in bid and "volume" in bid:
                        bids.append([bid["price"], bid["volume"]])

                for ask in msg.get("asks", []):
                    if isinstance(ask, list) and len(ask) >= 2:
                        asks.append([ask[0], ask[1]])
                    elif isinstance(ask, dict) and "price" in ask and "volume" in ask:
                        asks.append([ask["price"], ask["volume"]])

                # Create snapshot message
                return OrderBookMessage(
                    message_type=OrderBookMessageType.SNAPSHOT,
                    content={
                        "trading_pair": trading_pair,
                        "update_id": msg.get("sequence", timestamp_ms),
                        "bids": bids,
                        "asks": asks
                    },
                    timestamp=timestamp
                )
            elif message_type == OrderBookMessageType.DIFF:
                # Process diff message
                bids = []
                asks = []

                # Extract bids and asks updates
                for bid in msg.get("bids", []):
                    if isinstance(bid, list) and len(bid) >= 2:
                        bids.append([bid[0], bid[1]])
                    elif isinstance(bid, dict) and "price" in bid and "volume" in bid:
                        bids.append([bid["price"], bid["volume"]])

                for ask in msg.get("asks", []):
                    if isinstance(ask, list) and len(ask) >= 2:
                        asks.append([ask[0], ask[1]])
                    elif isinstance(ask, dict) and "price" in ask and "volume" in ask:
                        asks.append([ask["price"], ask["volume"]])

                # Create diff message
                return OrderBookMessage(
                    message_type=OrderBookMessageType.DIFF,
                    content={
                        "trading_pair": trading_pair,
                        "update_id": msg.get("sequence", timestamp_ms),
                        "bids": bids,
                        "asks": asks
                    },
                    timestamp=timestamp
                )

            return None
        except Exception as e:
            self.logger().error(f"Error creating order book message: {e}", exc_info=True)
            return None

    async def _listen_to_pair_stream(self, trading_pair: str):
        """
        Establishes and maintains a WebSocket connection for a single trading pair,
        processes incoming messages, creates OrderBookMessages and puts them into
        the appropriate message queues, and handles reconnections.
        """
        ws: Optional[WSAssistant] = None
        reconnect_attempts = 0
        max_reconnect_attempts = CONSTANTS.MAX_RECONNECT_ATTEMPTS  # Use a constant
        base_reconnect_delay = CONSTANTS.BASE_RECONNECT_DELAY  # Use a constant

        # Track sequence number for this trading pair
        last_sequence = -1

        # Check if we have access to an order book instance
        order_book = None
        if self._order_book_tracker is not None and hasattr(self._order_book_tracker, "order_books"):
            order_book = self._order_book_tracker.order_books.get(trading_pair)

        while True:
            try:
                # 1. Establish Connection
                self.logger().info(f"[{trading_pair}] Connecting to WebSocket stream...")
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                ws_url = CONSTANTS.WSS_URL.format(exchange_symbol)  # Use constant URL format
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                self.logger().info(f"[{trading_pair}] WebSocket connected.")

                # 2. Authenticate
                await self._authenticate_ws(ws)
                # Reset reconnect attempts on successful connection and authentication
                reconnect_attempts = 0

                # 3. Message Processing Loop
                async for raw_msg_data in ws.iter_messages():
                    try:
                        # Handle different message wrapper types (bytes, str, WSResponse)
                        parsed_msg = self._parse_raw_ws_message(raw_msg_data)
                        if parsed_msg is None:  # Handle keep-alive or parsing errors
                            continue

                        # --- Message Routing ---

                        # Process messages
                        if isinstance(parsed_msg, dict):
                            # Add trading pair for context
                            parsed_msg["trading_pair"] = trading_pair

                            # Get current timestamp
                            timestamp = time.time()

                            # Check if this is a snapshot message
                            is_snapshot = (
                                "asks" in parsed_msg and "bids" in parsed_msg and "sequence" in parsed_msg
                            )

                            # Check for sequence gaps
                            current_sequence = int(parsed_msg.get("sequence", -1)) if parsed_msg.get("sequence", -1) != -1 else -1
                            if last_sequence != -1 and current_sequence > last_sequence + 1:
                                self.logger().warning(
                                    f"[{trading_pair}] Sequence gap detected: {last_sequence} -> {current_sequence}. Reconnecting..."
                                )
                                # Force reconnection to get a fresh snapshot
                                raise SequenceGapError(f"Sequence gap: {last_sequence} -> {current_sequence}")

                            # Update last sequence if valid
                            if current_sequence != -1:
                                last_sequence = current_sequence

                            # Process message based on whether we have an order book instance
                            if order_book is not None:
                                # Direct order book update approach
                                # Determine if it's snapshot based on content and book state
                                is_initial_book = (
                                    is_snapshot and order_book._sequence == -1  # Check if book expects snapshot
                                )

                                if is_initial_book:
                                    order_book.process_luno_snapshot(parsed_msg)
                                elif "sequence" in parsed_msg:  # Must be an update if sequence exists
                                    order_book.process_luno_update(parsed_msg)
                                else:
                                    # Message doesn't fit snapshot or update format (e.g. simple ack?)
                                    self.logger().debug(f"[{trading_pair}] Received unhandled structured message: {parsed_msg}")
                            else:
                                # Message queue approach
                                # Process snapshot or update
                                if is_snapshot:
                                    # Create snapshot message
                                    snapshot_msg = self._create_order_book_message(
                                        parsed_msg,
                                        timestamp,
                                        OrderBookMessageType.SNAPSHOT,
                                        trading_pair
                                    )
                                    if snapshot_msg:
                                        self._message_queue[self._diff_messages_queue_key].put_nowait(snapshot_msg)
                                elif "sequence" in parsed_msg:  # Must be an update if sequence exists
                                    # Create diff message
                                    diff_msg = self._create_order_book_message(
                                        parsed_msg,
                                        timestamp,
                                        OrderBookMessageType.DIFF,
                                        trading_pair
                                    )
                                    if diff_msg:
                                        self._message_queue[self._diff_messages_queue_key].put_nowait(diff_msg)
                                else:
                                    # Message doesn't fit snapshot or update format (e.g. simple ack?)
                                    self.logger().debug(f"[{trading_pair}] Received unhandled structured message: {parsed_msg}")

                            # --- Separate Trade Processing ---
                            # Check if the update contained public trade information
                            if "trade_updates" in parsed_msg and isinstance(parsed_msg["trade_updates"], list):
                                sequence = parsed_msg.get("sequence")
                                for trade_data in parsed_msg["trade_updates"]:
                                    # Add sequence from outer message if trade_data doesn't have it
                                    if "sequence" not in trade_data and sequence is not None:
                                        trade_data["sequence"] = sequence
                                    trade_msg = self._parse_luno_trade(trade_data, timestamp, trading_pair)
                                    if trade_msg:
                                        # self.logger().debug(f"Parsed trade: {trade_msg}")
                                        self._message_queue[self._trade_messages_queue_key].put_nowait(trade_msg)

                        else:
                            # Should not happen if _parse_raw_ws_message works correctly
                            self.logger().warning(f"[{trading_pair}] Parsed message is not a dictionary: {parsed_msg}")

                    except SequenceGapError:
                        # Sequence gap detected, trigger reconnect
                        self.logger().warning(f"[{trading_pair}] SequenceGapError detected. Reconnecting...")
                        # Reset the sequence tracking
                        last_sequence = -1
                        # Reset the order book state if available
                        if order_book is not None:
                            order_book._reset_state_internal()
                            order_book.apply_snapshot([], [], -1)  # Reset base class state too
                        raise  # Re-raise to trigger the reconnect logic below
                    except Exception as e:
                        self.logger().error(
                            f"[{trading_pair}] Unexpected error processing message: {raw_msg_data}. Error: {e}",
                            exc_info=True
                        )
                        # Depending on severity, might trigger reconnect too
                        # raise # Uncomment to force reconnect on any processing error

            except asyncio.CancelledError:
                self.logger().info(f"[{trading_pair}] Listener task cancelled.")
                raise  # Propagate cancellation
            except SequenceGapError:  # Catch re-raised SequenceGapError
                # Already logged, now request a new snapshot before reconnecting
                self.logger().info(f"[{trading_pair}] Requesting new order book snapshot after sequence gap...")
                try:
                    # Request a new snapshot
                    snapshot_msg = await self._order_book_snapshot(trading_pair)

                    # Process the snapshot
                    if order_book is not None:
                        # If we have direct access to the order book, apply the snapshot directly
                        # Convert the snapshot message to the format expected by process_luno_snapshot
                        snapshot_data = {
                            "sequence": str(snapshot_msg.update_id),
                            "asks": [{"id": f"snapshot_{i}", "price": ask[0], "volume": ask[1]}
                                     for i, ask in enumerate(snapshot_msg.content["asks"])],
                            "bids": [{"id": f"snapshot_{i}", "price": bid[0], "volume": bid[1]}
                                     for i, bid in enumerate(snapshot_msg.content["bids"])],
                            "timestamp": int(snapshot_msg.timestamp * 1000)
                        }
                        order_book.process_luno_snapshot(snapshot_data)
                        self.logger().info(f"[{trading_pair}] Applied new snapshot to order book after sequence gap.")
                    else:
                        # If we don't have direct access, put the snapshot in the message queue
                        self._message_queue[self._snapshot_messages_queue_key].put_nowait(snapshot_msg)
                        self.logger().info(f"[{trading_pair}] Queued new snapshot after sequence gap.")
                except Exception as e:
                    self.logger().error(f"[{trading_pair}] Failed to request/process new snapshot after sequence gap: {e}", exc_info=True)
            except Exception as e:
                self.logger().error(f"[{trading_pair}] Unexpected error in WebSocket loop: {e}", exc_info=True)
            finally:
                # Ensure disconnection before retry/exit
                if ws is not None and ws._connection.connected:
                    await ws.disconnect()
                ws = None
                self.logger().debug(f"[{trading_pair}] WebSocket disconnected.")

            # --- Reconnect Logic ---
            reconnect_attempts += 1
            if reconnect_attempts > max_reconnect_attempts:
                self.logger().error(
                    f"[{trading_pair}] Maximum reconnect attempts ({max_reconnect_attempts}) reached. Stopping listener."
                )
                break  # Exit the loop for this pair

            # Exponential backoff with jitter
            delay = min(60.0, base_reconnect_delay * (1.5 ** reconnect_attempts))  # Cap delay
            delay *= (1 + (2 * 0.1 * (asyncio.get_event_loop().time() % 1) - 0.1))  # Add jitter +/- 10%
            self.logger().info(f"[{trading_pair}] Attempting reconnect {reconnect_attempts}/{max_reconnect_attempts} in {delay:.2f} seconds...")
            await asyncio.sleep(delay)

    def _parse_raw_ws_message(self, raw_msg_data: Any) -> Optional[Dict[str, Any]]:
        """Parses raw WebSocket message data (bytes, str, WSResponse) into a dictionary."""
        parsed_msg = None
        msg_str = ""

        try:
            if isinstance(raw_msg_data, bytes):
                msg_str = raw_msg_data.decode('utf8')
            elif isinstance(raw_msg_data, str):
                msg_str = raw_msg_data
            elif hasattr(raw_msg_data, "data") and isinstance(raw_msg_data.data, (str, dict, bytes)):  # Handle WSResponse like objects
                if isinstance(raw_msg_data.data, bytes):
                    msg_str = raw_msg_data.data.decode('utf8')
                elif isinstance(raw_msg_data.data, str):
                    msg_str = raw_msg_data.data
                elif isinstance(raw_msg_data.data, dict):
                    # Already a dict, use directly
                    return raw_msg_data.data
                else:
                    self.logger().warning(f"Unexpected WSResponse data type: {type(raw_msg_data.data)}")
                    return None
            else:
                self.logger().warning(f"Unexpected raw message type: {type(raw_msg_data)}")
                return None

            # Handle Luno's empty string keep-alive ""
            if msg_str == '""':
                # self.logger().debug("Received Luno keep-alive.")
                return None

            if not msg_str:  # Handle truly empty messages if they occur
                return None

            parsed_msg = json.loads(msg_str)
            if not isinstance(parsed_msg, dict):
                self.logger().warning(f"Parsed JSON is not a dictionary: {parsed_msg}")
                return None  # We expect dictionary messages

            return parsed_msg

        except json.JSONDecodeError:
            self.logger().error(f"Failed to decode JSON message: '{msg_str[:100]}...'")
            return None
        except UnicodeDecodeError:
            self.logger().error(f"Failed to decode bytes message (UTF-8): {raw_msg_data[:100]}...")
            return None
        except Exception as e:
            self.logger().error(f"Error parsing raw WS message: {e}", exc_info=True)
            return None

    async def _authenticate_ws(self, ws: WSAssistant):
        """Sends authentication credentials to the Luno WebSocket."""
        try:
            auth_payload = {
                "api_key_id": self._connector.api_key,
                "api_key_secret": self._connector.secret_key,
            }
            self.logger().info(f"API Key: {self._connector.api_key}.")
            auth_request = WSJSONRequest(payload=auth_payload, is_auth_required=False)  # Auth is the payload itself
            await ws.send(auth_request)
            self.logger().info("Sent authentication request for WebSocket connection.")
            # Note: Luno doesn't send an explicit auth confirmation back immediately.
            # Successful connection + subsequent data implies success.
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error during WebSocket authentication.", exc_info=True)
            raise

    def _parse_luno_trade(self, trade_data: Dict[str, Any], timestamp: float, trading_pair: str) -> Optional[OrderBookMessage]:
        """
        Parses a single trade item from Luno's trade_updates list into an OrderBookMessage.

        Luno trade_updates structure:
        {
          "sequence": 24509303, # Sequence of the *update message* containing the trade
          "base": "0.1",        # Volume traded
          "counter": "5232.00", # Value of the trade
          "maker_order_id": "BXMC2CJ7HNB88U4",
          "taker_order_id": "BXMC2CJ7HNB88U5",
          # Luno doesn't explicitly provide price or side in trade_updates.
          # Price needs calculation: counter / base
          # Side needs inference or might be unavailable reliably.
        }

        :param trade_data: A dictionary representing a single trade.
        :param timestamp: The timestamp of the message containing the trade (in seconds).
        :param trading_pair: The trading pair associated with this trade.
        :return: An OrderBookMessage of type TRADE, or None if parsing fails.
        """
        try:
            base_volume_str = trade_data.get("base")
            counter_volume_str = trade_data.get("counter")
            sequence = int(trade_data.get("sequence"))  # Use sequence from trade if available, else outer msg

            if base_volume_str is None or counter_volume_str is None:
                self.logger().warning(f"[{trading_pair}] Trade update missing base or counter volume: {trade_data}")
                return None

            base_volume = Decimal(base_volume_str)
            counter_volume = Decimal(counter_volume_str)

            if base_volume <= 0:
                self.logger().debug(f"[{trading_pair}] Ignoring trade update with zero or negative base volume: {trade_data}")
                return None

            price = counter_volume / base_volume

            # --- Inferring Trade Side (Difficult and potentially unreliable with Luno) ---
            # Luno's stream doesn't explicitly give the taker's side ('is_buy').
            # We *could* try to look up the maker_order_id in our *current* order book state,
            # but that's prone to race conditions (the book might have updated since the trade).
            # Relying on this is fragile.
            # For now, we will mark trades with an UNDEFINED side, as Hummingbot allows this.
            # Some strategies might not require the trade side from public trades.

            # Using TradeType.BUY explicitly as a placeholder for UNDEFINED
            # Being explicit that the side is unknown rather than silently guessing
            trade_type = TradeType.BUY  # UNDEFINED - Actual side is unknown from Luno API

            # If strategies NEED the side, more complex logic or assumptions are required.
            # Example (FRAGILE): Look up maker order
            # order_book = self._order_book_tracker.order_books.get(trading_pair)
            # if order_book and hasattr(order_book, "_order_id_map"):
            #     maker_info = order_book._order_id_map.get(trade_data.get("maker_order_id"))
            #     if maker_info:
            #         _, maker_side = maker_info
            #         # If maker was selling (ASK), taker was buying
            #         # If maker was buying (BID), taker was selling
            #         trade_type = TradeType.BUY if maker_side == TradeType.SELL else TradeType.SELL
            #     else:
            #         self.logger().debug(f"[{trading_pair}] Maker order ID not found for trade side inference: {trade_data.get('maker_order_id')}")
            # else:
            #     self.logger().warning(f"[{trading_pair}] Cannot access order book map for trade side inference.")
            # --- End Fragile Inference ---

            # Luno doesn't provide a unique trade ID in the stream 'trade_updates' item.
            # We need to generate one. Using timestamp + sequence + taker_id might work.
            taker_order_id = trade_data.get("taker_order_id", "unknown")
            trade_id = f"{int(timestamp * 1e6)}_{sequence}_{taker_order_id}"  # Microsecond timestamp + sequence + taker

            content = {
                "trading_pair": trading_pair,
                "trade_type": trade_type.name.lower(),  # Using string name instead of float value
                "trade_id": trade_id,
                "update_id": sequence,  # Use sequence as update_id
                "price": str(price),
                "amount": str(base_volume)
            }
            return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=timestamp)

        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            self.logger().error(f"[{trading_pair}] Error parsing Luno trade data: {e}. Data: {trade_data}", exc_info=True)
            return None

    # --- Required Implementation Methods ---

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Fetches the order book snapshot for a specific trading pair from the Luno API.

        :param trading_pair: The trading pair for which to fetch the order book snapshot
        :return: An OrderBookMessage containing the snapshot data
        """
        try:
            exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

            # Prepare the request parameters
            params = {
                "pair": exchange_symbol
            }

            # Make the API request to get the order book snapshot
            snapshot_response = await self._connector._api_get(
                path_url=CONSTANTS.ORDERBOOK_TOP_URL,
                params=params
            )

            # Extract timestamp from the response or use current time
            timestamp_ms = int(snapshot_response.get("timestamp", time.time() * 1000))
            timestamp = timestamp_ms / 1000.0

            # Process the bids and asks
            bids = []
            asks = []

            # Process bids
            for bid in snapshot_response.get("bids", []):
                price = bid.get("price")
                volume = bid.get("volume")
                if price is not None and volume is not None:
                    bids.append([price, volume])

            # Process asks
            for ask in snapshot_response.get("asks", []):
                price = ask.get("price")
                volume = ask.get("volume")
                if price is not None and volume is not None:
                    asks.append([price, volume])

            # Create the order book message
            return OrderBookMessage(
                message_type=OrderBookMessageType.SNAPSHOT,
                content={
                    "trading_pair": trading_pair,
                    "update_id": timestamp_ms,  # Use original timestamp in ms as update_id
                    "bids": bids,
                    "asks": asks
                },
                timestamp=timestamp
            )
        except Exception as e:
            self.logger().error(f"Error fetching order book snapshot for {trading_pair}: {e}", exc_info=True)
            raise

    # --- Helper Methods ---

    async def _sleep(self, delay: float):
        """
        Simple wrapper around asyncio.sleep to facilitate patching in tests.
        """
        await asyncio.sleep(delay)

    # --- Methods Not Used/Needed by this Implementation ---

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Not directly used. Connections are managed per-pair in _listen_to_pair_stream."""
        raise NotImplementedError

    async def _subscribe_channels(self, ws: WSAssistant):
        """Authentication is handled by _authenticate_ws. Luno connects per-pair, no separate subscription needed."""
        pass  # Authentication happens separately

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Handled by listen_for_subscriptions."""
        pass  # Logic is consolidated in listen_for_subscriptions

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Handled by listen_for_subscriptions."""
        pass  # Logic is consolidated in listen_for_subscriptions

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Initial snapshot comes via WebSocket. Periodic REST snapshots are not used."""
        pass  # Not used in this WebSocket-centric implementation
