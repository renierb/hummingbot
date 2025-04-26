import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from sortedcontainers import SortedDict

# Use Hummingbot's specific types where available
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook  # Base class
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_row import OrderBookRow  # Used by base class iterators


# Define a custom exception for sequence gaps (can be kept outside the class)
class SequenceGapError(Exception):
    """Custom exception for handling sequence gaps in Luno stream."""

    def __init__(self, trading_pair: str, expected: int, received: int):
        self.trading_pair = trading_pair
        self.expected = expected
        self.received = received
        super().__init__(
            f"Sequence gap detected for {trading_pair}: Expected {expected}, received {received}"
        )


class LunoOrderBook(OrderBook):
    """
    Custom OrderBook implementation for Luno exchange.

    Manages a detailed, order-ID-based internal state based on Luno's
    WebSocket stream, which provides individual order data. It updates the
    base Hummingbot OrderBook state using snapshots derived from this
    detailed internal state.

    Rationale for Snapshot Updates:
    Luno's stream provides individual order creation, deletion (by ID), and
    partial fills (trades affecting maker orders). Generating correct Hummingbot
    'diff' messages (which require the new total aggregated volume at affected
    price levels) directly from these granular updates is complex and error-prone,
    especially for deletions and trades where remaining volume isn't explicitly sent.
    Therefore, this implementation prioritizes internal state accuracy and updates
    the base Hummingbot OrderBook via full snapshots whenever the internal state changes,
    ensuring consistency at the cost of potential performance overhead compared to
    true diff processing.
    """

    # Logger instance should be set by the DataSource/Connector using this class
    _logger = None

    # Constants
    _DUST_THRESHOLD_INTERNAL = Decimal("1e-18")  # Tolerance for internal volume checks
    _DUST_THRESHOLD_HUMMINGBOT = Decimal("1e-9")  # Tolerance for Hummingbot snapshot data

    def __init__(self, trading_pair: str):
        """
        Initializes the LunoOrderBook.
        :param trading_pair: The trading pair this order book represents (e.g., "BTC-ZAR").
        """
        super().__init__(dex=False)
        self._trading_pair = trading_pair
        self._bids_internal: SortedDict[Decimal, Dict[str, Decimal]] = SortedDict()
        self._asks_internal: SortedDict[Decimal, Dict[str, Decimal]] = SortedDict()
        self._order_id_map: Dict[str, Tuple[Decimal, TradeType]] = {}
        self._sequence: int = -1
        self._last_update_timestamp: float = -1.0
        self._lock = threading.Lock()
        self._snapshot_uid = -1  # Mirror base class property if needed for checks
        self._last_diff_uid = -1

    # --- Core Luno Stream Processing Logic ---

    def process_luno_snapshot(self, snapshot_msg: Dict[str, Any]):
        """
        Processes the initial order book snapshot message from Luno's WebSocket.
        Resets state, populates internal detailed state, and applies a snapshot
        to the base Hummingbot OrderBook.

        Expected snapshot_msg format:
        {
          "sequence": "24352",
          "asks": [ {"id": "...", "price": "...", "volume": "..."}, ... ],
          "bids": [ {"id": "...", "price": "...", "volume": "..."}, ... ],
          "status": "ACTIVE",
          "timestamp": 1528884331021
        }

        :param snapshot_msg: The initial book message from Luno stream.
        """
        with self._lock:
            try:
                self._reset_state_internal()  # Reset detailed state

                # --- Validate and extract critical data FIRST ---
                sequence = int(snapshot_msg['sequence'])
                timestamp_ms = snapshot_msg['timestamp']  # Luno uses ms
                asks_data = snapshot_msg.get('asks', [])
                bids_data = snapshot_msg.get('bids', [])

                if sequence < 0:
                    raise ValueError("Invalid initial sequence number.")

                # --- Set sequence and timestamp BEFORE population ---
                # If population fails partially, sequence is still set correctly
                self._sequence = sequence
                self._last_update_timestamp = self._parse_luno_timestamp(timestamp_ms)
                self.logger().debug(f"[{self._trading_pair}] Processing snapshot seq {self._sequence}...")

                # --- Populate internal detailed book ---
                # Errors during population of individual orders are logged but shouldn't stop the whole process
                self._populate_side_from_luno(self._asks_internal, asks_data, TradeType.SELL)
                self._populate_side_from_luno(self._bids_internal, bids_data, TradeType.BUY)

                self.logger().info(f"[{self._trading_pair}] Initial internal book processed. Seq: {self._sequence}, "
                                   f"Internal Bids: {len(self._bids_internal)} levels ({sum(len(v) for v in self._bids_internal.values())} orders), "
                                   f"Internal Asks: {len(self._asks_internal)} levels ({sum(len(v) for v in self._asks_internal.values())} orders).")

                # --- Apply snapshot to the base Hummingbot OrderBook ---
                self._apply_snapshot_to_base_book()

            except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                # Catch errors in extracting sequence/timestamp or critical failures
                self.logger().error(
                    f"[{self._trading_pair}] CRITICAL Error processing Luno snapshot: {e}. Resetting state. Message: {snapshot_msg}",
                    exc_info=True
                )
                self._reset_state_internal()  # Ensure clean state on critical error
                self.apply_snapshot([], [], -1)  # Also reset base class state
            except Exception as e:
                # Catch any other unexpected critical errors
                self.logger().error(
                    f"[{self._trading_pair}] Unexpected CRITICAL error processing Luno snapshot: {e}. Resetting state. Message: {snapshot_msg}",
                    exc_info=True
                )
                self._reset_state_internal()
                self.apply_snapshot([], [], -1)

    def process_luno_update(self, update_msg: Dict[str, Any]) -> bool:
        """
        Processes a subsequent update message (create, delete, trade, status) from Luno's WebSocket.
        Handles sequence checks and updates the internal detailed state.
        If the internal state changes, applies a new snapshot to the base OrderBook.

        Expected update_msg format:
        {
          "sequence": "24353",
          "trade_updates": null or [ { "sequence": ..., "base": ..., ... } ],
          "create_update": null or { "order_id": ..., "type": ..., ... },
          "delete_update": null or { "order_id": ... },
          "status_update": null or { "status": ... },
          "timestamp": 1469031991
        }

        :param update_msg: The update message from Luno stream.
        :return: True if the internal book state was changed, False otherwise.
        :raises: SequenceGapError if a gap is detected.
        """
        internal_state_changed = False  # Default return value
        with self._lock:
            try:
                # Ensure initial snapshot was processed
                if self._sequence == -1:
                    self.logger().warning(f"[{self._trading_pair}] Ignoring update, initial snapshot not yet processed: {update_msg}")
                    return False

                update_sequence = int(update_msg['sequence'])

                # --- Sequence Check ---
                if update_sequence <= self._sequence:
                    # self.logger().debug(f"[{self._trading_pair}] Ignoring old update seq {update_sequence} (current: {self._sequence})")
                    return False

                if update_sequence > self._sequence + 1:
                    expected = self._sequence + 1
                    self.logger().error(f"[{self._trading_pair}] Sequence gap detected! Expected: {expected}, Received: {update_sequence}.")
                    raise SequenceGapError(self._trading_pair, expected, update_sequence)

                # --- Apply Update to Internal State ---
                # This handles create_update, delete_update, trade_updates
                internal_state_changed = self._apply_luno_update_internal(update_msg)

                # Process status_update separately (usually doesn't change book structure)
                self._process_luno_status_update(update_msg)
                self._sequence = update_sequence  # Update sequence *after* successful application

                timestamp_ms = update_msg.get('timestamp')
                if timestamp_ms:
                    self._last_update_timestamp = self._parse_luno_timestamp(timestamp_ms)

                # --- Update Base Hummingbot OrderBook (if changed) ---
                if internal_state_changed:
                    self._apply_snapshot_to_base_book()
                    return True
                else:
                    return False

            except SequenceGapError:
                # Re-raise sequence gap errors to be handled by the caller (DataSource)
                raise
            except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                # Log errors during update processing but don't crash the loop
                self.logger().error(
                    f"[{self._trading_pair}] Error processing Luno update: {e}. Message: {update_msg}",
                    exc_info=True
                )
                return False  # Indicate no change / error occurred
            except Exception as e:
                # Catch unexpected errors
                self.logger().error(
                    f"[{self._trading_pair}] Unexpected error processing Luno update: {e}. Message: {update_msg}",
                    exc_info=True
                )
                return False

    # --- Base Class Method Implementations / Interactions ---

    def apply_trade(self, trade: OrderBookMessage):
        """Applies a trade message to the base OrderBook state."""
        super().apply_trade(trade)

    def apply_snapshot(self, bids: List[List[str]], asks: List[List[str]], update_id: int):
        """
        Applies a snapshot represented by bids and asks lists to the base OrderBook state.
        CALLED INTERNALLY - Direct external calls are DISCOURAGED.
        """
        # Assumes lock is held
        super().apply_snapshot(bids, asks, update_id)
        # Ensure the base class snapshot_uid is also updated
        # self._snapshot_uid is the attribute name in the C implementation
        try:
            self._snapshot_uid = update_id
        except AttributeError:
            # Fallback if the attribute name differs or isn't directly settable
            self.logger().warning("Could not directly set _snapshot_uid on base OrderBook class.")

    def apply_diffs(self, bids: List[List[str]], asks: List[List[str]], update_id: int):
        """
        Applies differential updates to the base OrderBook state.

        NOTE: This LunoOrderBook implementation uses snapshots for base class updates.
        Calling this method directly will lead to inconsistencies.
        """
        self.logger().warning(
            f"[{self._trading_pair}] apply_diffs called on LunoOrderBook. "
            f"This implementation uses snapshot updates for the base class state. "
            f"Direct diff application is not supported and may cause inconsistencies."
        )
        # Do not call super().apply_diffs()

    # --- Methods Operating on Base Class State (Inherited) ---
    # (No changes needed here, methods are inherited)

    # --- Custom Methods for Luno Detailed State ---

    def get_internal_detailed_book(self) -> Optional[Tuple[SortedDict, SortedDict, int]]:
        """
        Returns thread-safe copies of the INTERNAL DETAILED order book state.
        Useful for logic needing order-level detail (e.g., advanced execution).

        :return: (bids_copy, asks_copy, sequence) where bids/asks are
                 SortedDict[Decimal, Dict[str, Decimal]], or None if not initialized.
        """
        with self._lock:
            if self._sequence == -1:
                return None
            bids_copy = self._bids_internal.copy()
            asks_copy = self._asks_internal.copy()
            return bids_copy, asks_copy, self._sequence

    def get_aggregated_snapshot(self) -> Tuple[SortedDict, SortedDict, int]:
        """
        Returns a thread-safe, AGGREGATED copy of the current order book state
        in a format convenient for calculations (SortedDict[Decimal, Decimal]).
        Derived from the internal detailed state.

        :return: (aggregated_bids, aggregated_asks, sequence) - empty SortedDicts if not initialized.
        """
        with self._lock:
            if self._sequence == -1:
                return SortedDict(), SortedDict(), self._sequence

            # Aggregate Bids
            aggregated_bids = SortedDict()
            for price, orders_at_price in self._bids_internal.items():
                total_volume = sum(orders_at_price.values())
                if total_volume > self._DUST_THRESHOLD_HUMMINGBOT:
                    aggregated_bids[price] = total_volume

            # Aggregate Asks
            aggregated_asks = SortedDict()
            for price, orders_at_price in self._asks_internal.items():
                total_volume = sum(orders_at_price.values())
                if total_volume > self._DUST_THRESHOLD_HUMMINGBOT:
                    aggregated_asks[price] = total_volume

            return aggregated_bids, aggregated_asks, self._sequence

    # --- Internal Helper Methods ---

    def _reset_state_internal(self):
        """Clears internal detailed book data and resets sequence/flags."""
        # Assumes lock is held
        self.logger().debug(f"Resetting internal detailed state for {self._trading_pair}.")
        self._bids_internal.clear()
        self._asks_internal.clear()
        self._order_id_map.clear()
        self._sequence = -1
        self._last_update_timestamp = -1.0
        # Reset base class snapshot UID tracking as well
        self._snapshot_uid = -1
        self._last_diff_uid = -1

    @staticmethod
    def _parse_luno_timestamp(timestamp_ms: Any) -> float:
        """Safely parse Luno timestamp (ms) to float seconds."""
        try:
            ts = float(timestamp_ms)
            # Luno uses ms since epoch
            return ts / 1000.0
        except (ValueError, TypeError):
            # Fallback to current time if parsing fails
            return time.time()

    def _populate_side_from_luno(self,
                                 internal_book_side: SortedDict[Decimal, Dict[str, Decimal]],
                                 luno_orders: List[Dict[str, str]],
                                 side: TradeType):
        """Helper to populate internal bids or asks from Luno snapshot data."""
        # Assumes lock is held
        for order in luno_orders:
            try:
                order_id = order['id']
                price = Decimal(order['price'])
                volume = Decimal(order['volume'])

                if volume > self._DUST_THRESHOLD_INTERNAL:
                    if price not in internal_book_side:
                        internal_book_side[price] = {}

                    if order_id in self._order_id_map:
                        self.logger().warning(f"[{self._trading_pair}] Duplicate order ID '{order_id}' in snapshot. Overwriting: {order}")
                        # Clean up old entry if necessary (should ideally not happen in clean snapshot)
                        old_price, old_side = self._order_id_map[order_id]
                        if old_price != price or old_side != side:
                            old_book = self._bids_internal if old_side == TradeType.BUY else self._asks_internal
                            if old_price in old_book and order_id in old_book[old_price]:
                                del old_book[old_price][order_id]
                                if not old_book[old_price]:
                                    del old_book[old_price]

                    internal_book_side[price][order_id] = volume
                    self._order_id_map[order_id] = (price, side)
            except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                # Log error for the specific order but continue processing others
                self.logger().warning(f"[{self._trading_pair}] Error processing single snapshot order: {e}. Order: {order}", exc_info=False)

    def _apply_luno_update_internal(self, update: Dict[str, Any]) -> bool:
        """
        Applies 'create', 'delete', or 'trade' updates from a Luno stream message
        to the internal detailed order book state.
        Assumes lock is held.

        :param update: The Luno update message part containing create/delete/trade info.
        :return: True if the internal book state actually changed, False otherwise.
        """
        state_changed = False

        # --- Process Create Update ---
        create_data = update.get("create_update")
        if create_data and isinstance(create_data, dict):
            try:
                order_id = create_data["order_id"]
                price = Decimal(create_data["price"])
                volume = Decimal(create_data["volume"])
                order_type_str = create_data["type"]  # "BID" or "ASK"

                if order_id in self._order_id_map:
                    self.logger().warning(f"[{self._trading_pair}] Create update for existing order_id {order_id}. Ignoring create.")
                elif order_type_str == "BID":
                    side = TradeType.BUY
                    book = self._bids_internal
                    if price not in book:
                        book[price] = {}
                    book[price][order_id] = volume
                    self._order_id_map[order_id] = (price, side)
                    state_changed = True  # State definitely changed
                elif order_type_str == "ASK":
                    side = TradeType.SELL
                    book = self._asks_internal
                    if price not in book:
                        book[price] = {}
                    book[price][order_id] = volume
                    self._order_id_map[order_id] = (price, side)
                    state_changed = True  # State definitely changed
                else:
                    self.logger().warning(f"[{self._trading_pair}] Create update with unknown type: {order_type_str}. Data: {create_data}")

            except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                self.logger().warning(f"[{self._trading_pair}] Error processing create_update {create_data}: {e}", exc_info=False)

        # --- Process Delete Update ---
        delete_data = update.get("delete_update")
        if delete_data and isinstance(delete_data, dict):
            try:
                order_id = delete_data["order_id"]
                order_info = self._order_id_map.pop(order_id, None)  # Use pop to remove and get info

                if order_info:
                    price, side = order_info
                    book = self._bids_internal if side == TradeType.BUY else self._asks_internal
                    if price in book and order_id in book[price]:
                        del book[price][order_id]
                        if not book[price]:
                            del book[price]
                        state_changed = True  # State changed
                    else:
                        self.logger().error(f"[{self._trading_pair}] Inconsistency: Delete order {order_id} in map but not in book structure ({side.name} @ {price}).")
            except (KeyError, ValueError, TypeError) as e:  # KeyError should be less likely now
                self.logger().warning(f"[{self._trading_pair}] Error processing delete_update {delete_data}: {e}", exc_info=False)

        # --- Process Trade Updates ---
        trade_list = update.get("trade_updates")
        if trade_list and isinstance(trade_list, list):
            for trade in trade_list:
                try:
                    # Ensure necessary keys exist before processing
                    if "maker_order_id" not in trade or "base" not in trade:
                        self.logger().warning(f"[{self._trading_pair}] Skipping trade update missing maker_order_id or base: {trade}")
                        continue

                    maker_order_id = trade["maker_order_id"]
                    base_volume_traded = Decimal(trade["base"])

                    if base_volume_traded <= self._DUST_THRESHOLD_INTERNAL:
                        continue

                    order_info = self._order_id_map.get(maker_order_id)

                    if order_info:
                        price, side = order_info
                        book = self._bids_internal if side == TradeType.BUY else self._asks_internal

                        if price in book and maker_order_id in book[price]:
                            current_volume = book[price][maker_order_id]
                            new_volume = current_volume - base_volume_traded

                            if new_volume <= self._DUST_THRESHOLD_INTERNAL:
                                del book[price][maker_order_id]
                                if not book[price]:
                                    del book[price]
                                del self._order_id_map[maker_order_id]  # Remove from map
                            else:
                                book[price][maker_order_id] = new_volume

                            state_changed = True  # Trade always changes state if processed
                        else:
                            self.logger().error(
                                f"[{self._trading_pair}] Inconsistency: Trade maker order {maker_order_id} in map but not in book structure ({side.name} @ {price}).")
                            self._order_id_map.pop(maker_order_id, None)  # Clean up map

                except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                    self.logger().warning(f"[{self._trading_pair}] Error processing trade_update {trade}: {e}", exc_info=False)

        return state_changed

    def _process_luno_status_update(self, update: Dict[str, Any]):
        """Processes status updates from Luno. Assumes lock is held."""
        status_data = update.get("status_update")
        if status_data and isinstance(status_data, dict):
            status = status_data.get("status")
            self.logger().info(f"[{self._trading_pair}] Received status update: {status}")
            # Add logic here if specific statuses require action (e.g., market suspension)

    def _aggregate_internal_book(self) -> Tuple[List[OrderBookRow], List[OrderBookRow]]:
        """
        Aggregates the internal detailed order book into the list format
        required by Hummingbot's apply_snapshot. Assumes lock is held.

        :return: Tuple containing (formatted_bids, formatted_asks)
                 Each list contains OrderBookRow objects.
        """
        hb_bids = []
        # Iterate bids (descending price) - base class apply_snapshot handles sorting
        for price, orders in self._bids_internal.items():
            total_volume = sum(orders.values())
            if total_volume > self._DUST_THRESHOLD_HUMMINGBOT:
                hb_bids.append(OrderBookRow(Decimal(price), Decimal(total_volume), self._sequence))

        hb_asks = []
        # Iterate asks (ascending price)
        for price, orders in self._asks_internal.items():
            total_volume = sum(orders.values())
            if total_volume > self._DUST_THRESHOLD_HUMMINGBOT:
                hb_asks.append(OrderBookRow(Decimal(price), Decimal(total_volume), self._sequence))

        return hb_bids, hb_asks

    def _apply_snapshot_to_base_book(self):
        """
        Aggregates the internal detailed state and applies it as a snapshot
        to the base Hummingbot OrderBook. Assumes lock is held.
        """
        if self._sequence == -1:
            self.logger().warning(f"[{self._trading_pair}] Attempted to apply snapshot to base book before initialization.")
            return

        hb_bids, hb_asks = self._aggregate_internal_book()
        # Call the base class method using super()
        super().apply_snapshot(
            bids=hb_bids,
            asks=hb_asks,
            update_id=self._sequence  # Use Luno sequence as update_id
        )
        # Ensure the snapshot_uid property is updated
        try:
            # Try setting the protected attribute directly if necessary
            self._snapshot_uid = self._sequence
            if (self._sequence % 10) == 0:
                self.logger().info(f"[{self._trading_pair}] Applied snapshot to base OrderBook. Seq/UpdateID: {self._sequence}")
        except AttributeError:
            self.logger().debug(f"[{self._trading_pair}] Applied snapshot to base OrderBook. Seq/UpdateID: {self._sequence}. (_snapshot_uid not directly settable)")
