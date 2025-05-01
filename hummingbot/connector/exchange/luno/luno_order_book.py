import logging
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from sortedcontainers import SortedDict

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook, OrderBookRow
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class SequenceGapError(Exception):
    """Exception raised when a sequence gap is detected in Luno stream."""

    def __init__(self, trading_pair: str, expected: int, received: int):
        super().__init__(f"Sequence gap for {trading_pair}: expected {expected}, received {received}")
        self.trading_pair = trading_pair
        self.expected = expected
        self.received = received


class LunoOrderBook(OrderBook):
    """
    OrderBook implementation for Luno exchange.

    Manages a detailed internal state tracking individual orders by ID based on
    Luno's WebSocket stream. It keeps the internal book trimmed to a defined depth.
    Instead of directly updating the base OrderBook state, it generates
    OrderBookMessage objects (SNAPSHOT type) representing the current aggregated
    state after processing snapshots or updates. These messages should be passed
    to the OrderBookTracker via the DataSource.
    """
    _logger = None

    _DEPTH_LIMIT: int = 100
    _DECIMAL_ZERO: Decimal = Decimal("0")
    _HB_DUST_THRESHOLD: Decimal = Decimal("1e-9")

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str):
        """Initializes the LunoOrderBook."""
        # Initialize base class, but we won't call its apply methods directly
        super().__init__(dex=False)
        self.trading_pair = trading_pair
        # Internal detailed state
        self._bids: SortedDict[Decimal, Decimal] = SortedDict()  # price -> cumulative volume
        self._asks: SortedDict[Decimal, Decimal] = SortedDict()  # price -> cumulative volume
        self._order_map: Dict[str, Tuple[Decimal, TradeType, Decimal]] = {}  # order_id -> (price, side, volume)
        self.sequence_uid: int = -1
        self.timestamp: Optional[float] = None
        self._lock = threading.Lock()

    # --- Luno Message Processors ---

    def process_snapshot(self, snapshot: Dict[str, Any]) -> Optional[OrderBookMessage]:
        """
        Processes the initial snapshot message from Luno WebSocket stream.
        Updates internal state, trims, and generates a SNAPSHOT OrderBookMessage.

        :param snapshot: The snapshot data dictionary from Luno.
        :return: An OrderBookMessage of type SNAPSHOT, or None on error.
        """
        message: Optional[OrderBookMessage] = None
        with self._lock:
            self.logger().info(f"[{self.trading_pair}] Processing snapshot...")
            try:
                seq = int(snapshot["sequence"])
                if seq < 0:
                    raise ValueError("Invalid sequence number.")

                self.sequence_uid = seq  # Set internal sequence

                timestamp = snapshot.get("timestamp", int(time.time() * 1e3))
                self.timestamp = timestamp  # Set internal timestamp

                # Populate internal state
                self._order_map.clear()  # Clear existing orders before loading a new snapshot
                self._load_side(self._bids, snapshot.get("bids", []), TradeType.BUY)
                self._load_side(self._asks, snapshot.get("asks", []), TradeType.SELL)

                self.logger().info(f"[{self.trading_pair}] Internal snapshot processed & trimmed. Seq: {seq}")

                # Generate the SNAPSHOT message for the tracker
                message = self._create_snapshot_message(self.sequence_uid, timestamp)

            except (KeyError, ValueError) as e:
                self.reset_state()
                self.logger().error(f"[{self.trading_pair}] Failed to process snapshot: {e}. State reset.", exc_info=True)
            except Exception:
                self.reset_state()
                self.logger().exception(f"[{self.trading_pair}] Unexpected error processing snapshot. State reset.")

            return message  # Return generated message or None

    def process_update(self, update: Dict[str, Any]) -> Optional[OrderBookMessage]:
        """
        Processes an incremental update message from Luno WebSocket stream.
        Updates internal state, trims (if changed), and generates a SNAPSHOT
        OrderBookMessage representing the new state if changes occurred.

        :param update: The update data dictionary from Luno.
        :raises SequenceGapError: If a sequence number gap is detected.
        :return: An OrderBookMessage of type SNAPSHOT if state changed, None otherwise or on error.
        """
        message: Optional[OrderBookMessage] = None
        state_changed = False
        with self._lock:
            try:
                # --- Sequence Check ---
                seq = int(update["sequence"])
                if self.sequence_uid == -1:
                    self.logger().warning(f"[{self.trading_pair}] Update received before snapshot. Ignoring seq {seq}.")
                    return None

                if seq <= self.sequence_uid:
                    # self.logger().debug(f"[{self._trading_pair}] Ignoring old sequence {seq} (current {self.sequence_uid}).")
                    return None
                if seq > self.sequence_uid + 1:
                    expected = self.sequence_uid + 1
                    self.logger().error(f"[{self.trading_pair}] Sequence gap detected! Expected: {expected}, Received: {seq}.")
                    raise SequenceGapError(self.trading_pair, expected, seq)

                # --- Update Sequence ---
                self.sequence_uid = seq

                timestamp = update.get("timestamp", int(time.time() * 1e3))
                self.timestamp = timestamp  # Set internal timestamp

                # --- Apply Updates to internal state ---
                if cu := update.get("create_update"):
                    state_changed |= self._apply_create(cu)
                if du := update.get("delete_update"):
                    state_changed |= self._apply_delete(du)
                if trade_updates := update.get("trade_updates"):
                    for tu in trade_updates:
                        state_changed |= self._apply_trade_update(tu)

                # --- Trim and Generate SNAPSHOT Message if Changed ---
                if state_changed:
                    message = self._create_snapshot_message(self.sequence_uid, timestamp)

            except SequenceGapError:
                raise  # Propagate gap error for DataSource to handle
            except (KeyError, ValueError, InvalidOperation) as e:
                self.logger().warning(f"[{self.trading_pair}] Error applying update seq {update.get('sequence', 'N/A')}: {e}", exc_info=False)
            except Exception:
                self.logger().exception(f"[{self.trading_pair}] Unexpected error processing update seq {update.get('sequence', 'N/A')}.")

            return message  # Return generated message (if any) or None

    # --- Public State Management ---
    def reset_state(self) -> None:
        """Clears internal detailed state and resets sequence."""
        self._bids.clear()
        self._asks.clear()
        self._order_map.clear()
        self.sequence_uid = -1
        self.timestamp = None
        self.logger().info(f"[{self.trading_pair}] Internal state reset.")

    # --- Message Generation ---

    def _create_snapshot_message(self, update_id: int, timestamp: float) -> OrderBookMessage:
        """
        Generates a standard Hummingbot SNAPSHOT message from the current
        (trimmed) internal aggregated state. Assumes lock is held.
        """
        # Bids (price_str, volume_str) - sorted high to low
        bids_list: List[Tuple[str, str]] = []
        for price, volume in reversed(self._bids.items()):
            if len(bids_list) >= self._DEPTH_LIMIT:
                break
            bids_list.append((str(price), str(volume)))

        # Asks (price_str, volume_str) - sorted low to high
        asks_list: List[Tuple[str, str]] = []
        for price, volume in self._asks.items():
            if len(asks_list) >= self._DEPTH_LIMIT:
                break
            asks_list.append((str(price), str(volume)))

        content = {
            "trading_pair": self.trading_pair,
            "update_id": update_id,
            "bids": bids_list,
            "asks": asks_list,
        }
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=content,
            timestamp=timestamp
        )

    def order_book_snapshot_safe(self, update_id: int, timestamp: float) -> OrderBookMessage:
        """
        Generates a thread-safe snapshot of the current internal order book state
        as an OrderBookMessage. Useful for calculations.

        :return: An OrderBookMessage of type SNAPSHOT.
        """
        with self._lock:
            return self._create_snapshot_message(update_id, timestamp)

    # --- Internal State Modification Helpers (Assume Lock Held) ---
    def _load_side(
            self,
            internal_book: SortedDict[Decimal, Decimal],
            orders: List[Dict[str, str]],
            side: TradeType
    ) -> None:
        """Helper to populate internal bids or asks from Luno snapshot data."""
        # Clear the side first before rebuilding it
        internal_book.clear()

        # Process all orders and store them in _order_map
        for order_data in orders:
            try:
                order_id = order_data["id"]
                price = Decimal(order_data["price"])
                amount = Decimal(order_data["volume"])
                internal_book[price] = internal_book.setdefault(price, Decimal("0")) + amount
                self._order_map[order_id] = (price, side, amount)
            except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                self.logger().warning(f"[{self.trading_pair}] Skipping invalid order during snapshot load: {order_data}. Error: {e}", exc_info=False)

    def _apply_create(self, create_data: Dict[str, str]) -> bool:
        """Applies a create_update message."""
        try:
            order_id = create_data["order_id"]
            price = Decimal(create_data["price"])
            amount = Decimal(create_data["volume"])
            order_type_str = create_data["type"]  # "BID" or "ASK"

            if order_id in self._order_map:
                self.logger().warning(f"[{self.trading_pair}] Create update for existing order_id {order_id}. Ignoring create.")
                return False

            if amount <= self._HB_DUST_THRESHOLD:
                return False  # Ignore dust orders

            if order_type_str == "BID":
                side = TradeType.BUY
                book = self._bids
            elif order_type_str == "ASK":
                side = TradeType.SELL
                book = self._asks
            else:
                self.logger().warning(f"[{self.trading_pair}] Unknown order type in create_update: {order_type_str}")
                return False

            # Store order in _order_map
            self._order_map[order_id] = (price, side, amount)

            # Update cumulative volume at the price level
            current_volume = book.get(price, Decimal("0"))
            book[price] = current_volume + amount

            return True
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            self.logger().warning(f"[{self.trading_pair}] Failed to process create_update: {e}. Data: {create_data}", exc_info=False)
            return False

    def _apply_delete(self, delete_data: Dict[str, Any]) -> bool:
        """Applies a delete_update message."""
        try:
            order_id = delete_data["order_id"]
            order_info = self._order_map.pop(order_id, None)
            if order_info:
                price, side, volume = order_info
                book = self._bids if side is TradeType.BUY else self._asks

                # Update cumulative volume at the price level
                if price in book:
                    new_volume = book[price] - volume
                    if new_volume <= self._HB_DUST_THRESHOLD:
                        # Remove price level if volume becomes dust
                        del book[price]
                    else:
                        # Update with a new volume
                        book[price] = new_volume
                    return True
                else:
                    self.logger().error(f"[{self.trading_pair}] Inconsistency: Delete order {order_id} in map but price {price} not in book.")
                    return False
            else:
                return False  # Order not in map
        except (KeyError, ValueError, TypeError) as e:
            self.logger().warning(f"[{self.trading_pair}] Failed to process delete_update: {e}. Data: {delete_data}", exc_info=False)
            return False

    def _apply_trade_update(self, trade_data: Dict[str, Any]) -> bool:
        """Applies a trade_updates item."""
        try:
            maker_order_id = trade_data["maker_order_id"]
            traded_volume = Decimal(trade_data["base"])

            order_info = self._order_map.get(maker_order_id)
            if not order_info:
                return False

            price, side, order_volume_cur = order_info
            book = self._bids if side is TradeType.BUY else self._asks

            if price not in book:
                self.logger().error(f"[{self.trading_pair}] Inconsistency: Trade maker order {maker_order_id} in map but price {price} not in book.")
                self._order_map.pop(maker_order_id, None)
                return False

            # --- Apply volume reduction to the current order ---
            order_volume_new = order_volume_cur - traded_volume

            if order_volume_new <= self._HB_DUST_THRESHOLD:
                # Fully filled or dust remaining - remove order
                self._order_map.pop(maker_order_id, None)
            else:
                # Update order volume in order map (partially filled)
                self._order_map[maker_order_id] = (price, side, order_volume_new)

            # Update the book's cumulative volume at this price level
            price_volume_new = book[price] - traded_volume

            # If price level volume is now dust, remove the price level
            if price_volume_new <= self._HB_DUST_THRESHOLD:
                del book[price]
            else:
                book[price] = price_volume_new

            return True
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            self.logger().warning(f"[{self.trading_pair}] Failed to process trade_update: {e}. Data: {trade_data}", exc_info=False)
            return False

    # --- Base Class Methods Not Used Directly by this Implementation ---
    # Override apply_diffs to prevent misuse
    def apply_diffs(self, bids: List[OrderBookRow], asks: List[OrderBookRow], update_id: int):
        self.sequence_uid = update_id
        super().apply_diffs(bids, asks, update_id)

    # Override apply_snapshot to prevent misuse
    def apply_snapshot(self, bids: List[OrderBookRow], asks: List[OrderBookRow], update_id: int):
        self.sequence_uid = update_id
        super().apply_snapshot(bids, asks, update_id)
