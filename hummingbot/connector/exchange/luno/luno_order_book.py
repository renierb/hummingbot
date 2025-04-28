import logging
import threading
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from sortedcontainers import SortedDict

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook, OrderBookRow


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

    Maintains a detailed internal state tracking individual orders by ID based on
    Luno's WebSocket stream. It keeps the internal book trimmed to a defined depth
    and updates the base Hummingbot OrderBook state using aggregated snapshots
    derived from this trimmed internal state.
    """
    _logger = None  # Class logger instance

    # --- Constants ---
    _DEPTH_LIMIT: int = 100  # Keep only top N price levels per side
    _DECIMAL_ZERO: Decimal = Decimal("0")
    _DUST_THRESHOLD: Decimal = Decimal("1e-18")  # Threshold for treating internal volume as zero
    _HB_DUST_THRESHOLD: Decimal = Decimal("1e-9")  # Hummingbot typical threshold for snapshots

    @classmethod
    def logger(cls) -> logging.Logger:
        """Gets the logger for this class."""
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str):
        """Initializes the LunoOrderBook."""
        super().__init__(dex=False)
        self._trading_pair = trading_pair
        # Internal detailed state {price: {order_id: volume}}
        self._bids: SortedDict[Decimal, Dict[str, Decimal]] = SortedDict()
        self._asks: SortedDict[Decimal, Dict[str, Decimal]] = SortedDict()
        # Map order_id -> (price, TradeType) for fast lookups
        self._order_map: Dict[str, Tuple[Decimal, TradeType]] = {}
        # Luno sequence number
        self._sequence: int = -1
        # Thread lock for state modification
        self._lock = threading.Lock()

    def process_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """
        Processes the initial snapshot message from the Luno WebSocket stream.

        :param snapshot: The snapshot data dictionary.
        """
        with self._lock:
            self.logger().info(f"[{self._trading_pair}] Processing snapshot...")
            try:
                self.reset_state()
                seq = int(snapshot["sequence"])

                # --- Set sequence BEFORE potentially failing population ---
                self._sequence = seq
                self.logger().debug(f"[{self._trading_pair}] Snapshot sequence set to {seq}.")

                # --- Populate internal state ---
                self._load_side(self._bids, snapshot.get("bids", []), TradeType.BUY)
                self._load_side(self._asks, snapshot.get("asks", []), TradeType.SELL)

                # --- Trim to depth limit ---
                self._trim_book()

                self.logger().info(f"[{self._trading_pair}] Internal snapshot loaded & trimmed. "
                                   f"Bids: {len(self._bids)}, Asks: {len(self._asks)}, Orders: {len(self._order_map)}")

                # --- Apply to base class ---
                self._apply_snapshot_to_base_book(seq)

            except (KeyError, ValueError) as e:
                self.reset_state()  # Reset on critical failure
                self.logger().error(f"[{self._trading_pair}] Failed to process snapshot: {e}. State reset.", exc_info=True)
            except Exception:
                self.reset_state()  # Reset on critical failure
                self.logger().exception(f"[{self._trading_pair}] Unexpected error processing snapshot. State reset.")

    def process_update(self, update: Dict[str, Any]) -> bool:
        """
        Processes an incremental update message from the Luno WebSocket stream.

        :param update: The update data dictionary.
        :raises SequenceGapError: If a sequence number gap is detected.
        :return: True if the update changed the order book state, False otherwise.
        """
        state_changed = False  # Default return value
        with self._lock:
            try:
                seq = int(update["sequence"])

                # --- Sequence Check ---
                if self._sequence == -1:
                    self.logger().warning(f"[{self._trading_pair}] Update received before snapshot. Ignoring seq {seq}.")
                    return False

                if seq <= self._sequence:
                    # self.logger().debug(f"[{self._trading_pair}] Ignoring old sequence {seq} (current {self._sequence}).")
                    return False

                if seq > self._sequence + 1:
                    expected = self._sequence + 1
                    self.logger().error(f"[{self._trading_pair}] Sequence gap detected! Expected: {expected}, Received: {seq}.")
                    raise SequenceGapError(self._trading_pair, expected, seq)

                # --- Apply Updates ---
                if cu := update.get("create_update"):
                    state_changed |= self._apply_create(cu)
                if du := update.get("delete_update"):
                    state_changed |= self._apply_delete(du)
                if trade_updates := update.get("trade_updates"):
                    for tu in trade_updates:
                        state_changed |= self._apply_trade_update(tu)

                # --- Update Sequence ---
                self._sequence = seq  # Update only after successful processing

                # --- Trim and Apply to Base if Changed ---
                if state_changed:
                    # self.logger().debug(f"[{self._trading_pair}] State changed by seq {seq}. Trimming and applying snapshot.")
                    self._trim_book()
                    self._apply_snapshot_to_base_book(seq)

                return state_changed

            except SequenceGapError:
                raise  # Propagate gap error
            except (KeyError, ValueError, InvalidOperation) as e:
                self.logger().warning(f"[{self._trading_pair}] Error applying update seq {update.get('sequence', 'N/A')}: {e}", exc_info=False)
                return False  # Indicate error without stopping caller
            except Exception:
                self.logger().exception(f"[{self._trading_pair}] Unexpected error processing update seq {update.get('sequence', 'N/A')}.")
                return False

    # --- Public State Management ---
    def reset_state(self) -> None:
        """Clears internal detailed state and resets sequence."""
        self._bids.clear()
        self._asks.clear()
        self._order_map.clear()
        self._sequence = -1
        # Reset base class snapshot UID tracking as well
        self._snapshot_uid = -1
        self.logger().info(f"[{self._trading_pair}] Internal state reset.")

    # --- Internal State Modification Helpers (Assume Lock Held) ---

    def _load_side(
            self,
            internal_book_side: SortedDict[Decimal, Dict[str, Decimal]],
            orders: List[Dict[str, str]],
            side: TradeType
    ) -> None:
        """Helper to populate internal bids or asks from Luno snapshot data."""
        orders_loaded = 0
        for order_data in orders:
            try:
                order_id = order_data["id"]
                price = Decimal(order_data["price"])
                amount = Decimal(order_data["volume"])

                if amount > self._DUST_THRESHOLD:
                    # Handle potential duplicate order IDs gracefully (overwrite)
                    if order_id in self._order_map:
                        self.logger().warning(f"[{self._trading_pair}] Duplicate order ID '{order_id}' found during snapshot load. Overwriting.")
                        old_price, old_side = self._order_map[order_id]
                        # Clean up old entry if it was in a different place (shouldn't happen in clean snapshot)
                        if old_price != price or old_side != side:
                            old_book = self._bids if old_side == TradeType.BUY else self._asks
                            if old_price in old_book and order_id in old_book[old_price]:
                                del old_book[old_price][order_id]
                                if not old_book[old_price]:
                                    del old_book[old_price]

                    # Ensure price level exists and add/update order
                    internal_book_side.setdefault(price, {})[order_id] = amount
                    # Update map
                    self._order_map[order_id] = (price, side)
                    orders_loaded += 1
            except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                self.logger().warning(
                    f"[{self._trading_pair}] Skipping invalid order during snapshot load: {order_data}. Error: {e}",
                    exc_info=False
                )
        # self.logger().debug(f"[{self._trading_pair}] Loaded {orders_loaded} orders for {side.name} side from snapshot.")

    def _apply_create(self, create_data: Dict[str, str]) -> bool:
        """Applies a create_update message."""
        try:
            order_id = create_data["order_id"]
            price = Decimal(create_data["price"])
            amount = Decimal(create_data["volume"])
            order_type_str = create_data["type"]  # "BID" or "ASK"

            if order_id in self._order_map:
                self.logger().warning(f"[{self._trading_pair}] Create update for existing order_id {order_id}. Ignoring create.")
                return False

            if amount <= self._DUST_THRESHOLD:
                return False  # Ignore dust orders

            if order_type_str == "BID":
                side = TradeType.BUY
                book = self._bids
            elif order_type_str == "ASK":
                side = TradeType.SELL
                book = self._asks
            else:
                self.logger().warning(f"[{self._trading_pair}] Unknown order type in create_update: {order_type_str}")
                return False

            book.setdefault(price, {})[order_id] = amount
            self._order_map[order_id] = (price, side)
            return True
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            self.logger().warning(f"[{self._trading_pair}] Failed to process create_update: {e}. Data: {create_data}", exc_info=False)
            return False

    def _apply_delete(self, delete_data: Dict[str, Any]) -> bool:
        """Applies a delete_update message."""
        try:
            order_id = delete_data["order_id"]
            order_info = self._order_map.pop(order_id, None)  # Use pop atomically

            if order_info:
                price, side = order_info
                book = self._bids if side is TradeType.BUY else self._asks
                level = book.get(price)

                if level and order_id in level:
                    del level[order_id]
                    if not level:  # Is price level now empty?
                        del book[price]
                    return True
                else:
                    self.logger().error(f"[{self._trading_pair}] Inconsistency: Delete order {order_id} in map but not in book.")
                    return False
            else:
                return False  # Order not in map
        except (KeyError, ValueError, TypeError) as e:
            self.logger().warning(f"[{self._trading_pair}] Failed to process delete_update: {e}. Data: {delete_data}", exc_info=False)
            return False

    def _apply_trade_update(self, trade_data: Dict[str, Any]) -> bool:
        """Applies a trade_updates item."""
        try:
            maker_order_id = trade_data["maker_order_id"]
            base_volume_traded = Decimal(trade_data["base"])

            if base_volume_traded <= self._DUST_THRESHOLD:
                return False

            order_info = self._order_map.get(maker_order_id)
            if not order_info:
                return False

            price, side = order_info
            book = self._bids if side is TradeType.BUY else self._asks
            level = book.get(price)

            if not level or maker_order_id not in level:
                self.logger().error(f"[{self._trading_pair}] Inconsistency: Trade maker order {maker_order_id} in map but not in book level {price}.")
                self._order_map.pop(maker_order_id, None)
                return False

            # --- Apply volume reduction ---
            current_volume = level[maker_order_id]
            new_volume = current_volume - base_volume_traded

            if new_volume <= self._DUST_THRESHOLD:
                # Fully filled or dust remaining - remove order
                del level[maker_order_id]
                self._order_map.pop(maker_order_id, None)
                if not level:
                    del book[price]
            else:
                level[maker_order_id] = new_volume

            return True
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            self.logger().warning(f"[{self._trading_pair}] Failed to process trade_update: {e}. Data: {trade_data}", exc_info=False)
            return False

    def _trim_book(self) -> None:
        """Removes price levels beyond the _DEPTH_LIMIT from internal state and _order_map."""
        # Assumes lock is held
        removed_orders_count = 0
        # Trim bids (remove lowest prices)
        while len(self._bids) > self._DEPTH_LIMIT:
            price_to_remove, orders_at_level = self._bids.popitem(0)
            for order_id in list(orders_at_level.keys()):  # Iterate keys for safe deletion
                self._order_map.pop(order_id, None)
                removed_orders_count += 1
        # Trim asks (remove highest prices)
        while len(self._asks) > self._DEPTH_LIMIT:
            price_to_remove, orders_at_level = self._asks.popitem(-1)
            for order_id in list(orders_at_level.keys()):  # Iterate keys for safe deletion
                self._order_map.pop(order_id, None)
                removed_orders_count += 1

        if removed_orders_count > 0:
            self.logger().debug(f"[{self._trading_pair}] Trimmed {removed_orders_count} orders from internal book/map.")

    # --- Base Class Update ---

    def _apply_snapshot_to_base_book(self, update_id: int) -> None:
        """
        Aggregates the (trimmed) internal detailed state and applies it as a snapshot
        to the base Hummingbot OrderBook using the standard List[OrderBookRow] format.
        Assumes lock is held.
        """
        bids_list, asks_list = self.get_aggregated_snapshot(update_id) or ([], [])

        # Call the base class snapshot method
        try:
            # Use apply_snapshot to ensure snapshot_uid is updated
            super().apply_snapshot(bids_list, asks_list, update_id)
        except Exception as e:
            # Catch potential errors during base class update
            self.logger().exception(f"[{self._trading_pair}] Error applying snapshot to base OrderBook class: {e}")

    # --- Public Accessor ---

    def get_aggregated_snapshot(self, update_id: int) -> Optional[Tuple[List[OrderBookRow], List[OrderBookRow]]]:
        """
        Returns an aggregated snapshot of the current internal order book state.

        :return: A tuple of two lists: (bids, asks), where each list contains tuples of (price_str, volume_str).
        """
        if self._sequence == -1:
            return None

        # Aggregate bids (price_str, volume_str) - sorted high to low for base class
        bids_list: List[OrderBookRow] = []
        for price, level_orders in reversed(self._bids.items()):  # Iterate high to low
            total_volume = sum(level_orders.values())
            if total_volume > self._HB_DUST_THRESHOLD:  # Use HB threshold for snapshot
                bids_list.append(OrderBookRow(float(price), float(total_volume), update_id))

        # Aggregate asks (price_str, volume_str) - sorted low to high for base class
        asks_list: List[OrderBookRow] = []
        for price, level_orders in self._asks.items():  # Iterate low to high
            total_volume = sum(level_orders.values())
            if total_volume > self._HB_DUST_THRESHOLD:
                asks_list.append(OrderBookRow(float(price), float(total_volume), update_id))

        return bids_list, asks_list
