import logging
import time
import unittest
from decimal import Decimal

from hummingbot.connector.exchange.luno.luno_order_book import LunoOrderBook, SequenceGapError
from hummingbot.core.data_type.common import TradeType

# OrderBookMessage might still be useful for testing apply_trade if needed, but not for snapshot/diff parsing tests
# from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType

# Silence logging during tests if desired
logging.basicConfig(level=logging.ERROR)


class LunoOrderBookTests(unittest.TestCase):
    level = logging.DEBUG  # so our handler sees DEBUG+ messages

    def handle(self, record):
        """logging.Handler interface — stash records for assertions."""
        if not hasattr(self, "log_records"):
            self.log_records = []
        self.log_records.append(record)

    def setUp(self) -> None:
        super().setUp()
        # 1) Prepare log capture
        self.log_records = []

        # 2) Create the book
        self.trading_pair = "XBT-ZAR"
        self.book = LunoOrderBook(trading_pair=self.trading_pair)

        # 3) Attach this test as a log handler
        logger = self.book.logger()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self)

    # No tearDown needed anymore as we don't use class variables

    def test_initial_state(self):
        self.assertEqual(-1, self.book._sequence)
        self.assertEqual(-1.0, self.book._last_update_timestamp)
        self.assertEqual(0, self.book.snapshot_uid)  # Check base class property
        self.assertFalse(list(self.book.bid_entries()))  # Check base class state
        self.assertFalse(list(self.book.ask_entries()))  # Check base class state

    def test_process_luno_snapshot(self):
        # Arrange
        snapshot_msg = {
            "sequence": "24352",
            "asks": [
                {"id": "ask_id_1", "price": "1235.00", "volume": "0.93"},
                {"id": "ask_id_2", "price": "1236.00", "volume": "1.50"}
            ],
            "bids": [
                {"id": "bid_id_1", "price": "1234.00", "volume": "1.22"},
                {"id": "bid_id_2", "price": "1233.00", "volume": "2.00"}
            ],
            "status": "ACTIVE",
            "timestamp": 1528884331021  # ms
        }

        # Act
        self.book.process_luno_snapshot(snapshot_msg)

        # Assert Internal State
        self.assertEqual(24352, self.book._sequence)
        self.assertAlmostEqual(1528884331.021, self.book._last_update_timestamp)
        self.assertEqual(2, len(self.book._bids_internal))
        self.assertEqual(2, len(self.book._asks_internal))
        self.assertIn("bid_id_1", self.book._order_id_map)
        self.assertIn("ask_id_1", self.book._order_id_map)
        self.assertEqual((Decimal("1234.00"), TradeType.BUY), self.book._order_id_map["bid_id_1"])
        self.assertEqual((Decimal("1235.00"), TradeType.SELL), self.book._order_id_map["ask_id_1"])

        # Assert Base Class State (via inherited methods/properties)
        self.assertEqual(24352, self.book.snapshot_uid)
        bids = list(self.book.bid_entries())  # bids() returns an iterator
        asks = list(self.book.ask_entries())  # asks() returns an iterator
        self.assertEqual(2, len(bids))
        self.assertEqual(2, len(asks))
        # Base class stores price/amount as float internally
        self.assertAlmostEqual(1234.00, bids[0].price)
        self.assertAlmostEqual(1.22, bids[0].amount)
        self.assertAlmostEqual(1233.00, bids[1].price)
        self.assertAlmostEqual(2.00, bids[1].amount)
        self.assertAlmostEqual(1235.00, asks[0].price)
        self.assertAlmostEqual(0.93, asks[0].amount)
        self.assertAlmostEqual(1236.00, asks[1].price)
        self.assertAlmostEqual(1.50, asks[1].amount)

    def test_process_luno_update_create(self):
        # Arrange: Start with a snapshot
        snapshot_msg = {
            "sequence": "100", "timestamp": 1600000000000,
            "asks": [{"id": "a1", "price": "101", "volume": "1"}],
            "bids": [{"id": "b1", "price": "99", "volume": "1"}]
        }
        self.book.process_luno_snapshot(snapshot_msg)

        create_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": {"order_id": "b2", "type": "BID", "price": "99.5", "volume": "0.5"},
            "delete_update": None, "trade_updates": None, "status_update": None
        }

        # Act
        changed = self.book.process_luno_update(create_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        self.assertAlmostEqual(1600000001.000, self.book._last_update_timestamp)
        # Internal check
        self.assertIn(Decimal("99.5"), self.book._bids_internal)
        self.assertEqual(Decimal("0.5"), self.book._bids_internal[Decimal("99.5")]["b2"])
        self.assertIn("b2", self.book._order_id_map)
        # Base class check
        self.assertEqual(101, self.book.snapshot_uid)  # Snapshot UID updated
        bids = list(self.book.bid_entries())
        self.assertEqual(2, len(bids))  # Original bid plus new bid
        self.assertAlmostEqual(99.5, bids[0].price)  # Highest bid first
        self.assertAlmostEqual(0.5, bids[0].amount)

    def test_process_luno_update_delete(self):
        # Arrange: Start with a snapshot containing the order to delete
        snapshot_msg = {
            "sequence": "100", "timestamp": 1600000000000,
            "asks": [{"id": "a1", "price": "101", "volume": "1"}, {"id": "a2", "price": "102", "volume": "2"}],
            "bids": [{"id": "b1", "price": "99", "volume": "1"}]
        }
        self.book.process_luno_snapshot(snapshot_msg)
        self.assertEqual(2, len(list(self.book.ask_entries())))
        self.assertIn("a1", self.book._order_id_map)

        delete_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": None,
            "delete_update": {"order_id": "a1"},  # Delete the first ask
            "trade_updates": None, "status_update": None
        }

        # Act
        changed = self.book.process_luno_update(delete_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        # Internal check
        self.assertNotIn(Decimal("101"), self.book._asks_internal)  # Price level removed
        self.assertNotIn("a1", self.book._order_id_map)
        self.assertIn(Decimal("102"), self.book._asks_internal)  # Other asks still exist
        # Base class check
        self.assertEqual(101, self.book.snapshot_uid)
        asks = list(self.book.ask_entries())
        self.assertEqual(1, len(asks))
        self.assertAlmostEqual(102.0, asks[0].price)
        self.assertAlmostEqual(2.0, asks[0].amount)

    def test_process_luno_update_trade_partial_fill(self):
        # Arrange: Start with a snapshot containing the order to trade against
        snapshot_msg = {
            "sequence": "100", "timestamp": 1600000000000,
            "asks": [{"id": "a1", "price": "101", "volume": "5.0"}],  # Maker order
            "bids": []
        }
        self.book.process_luno_snapshot(snapshot_msg)
        self.assertEqual(Decimal("5.0"), self.book._asks_internal[Decimal("101")]["a1"])

        trade_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": None, "delete_update": None,
            "trade_updates": [{"base": "2.0", "counter": "202.0", "maker_order_id": "a1", "taker_order_id": "t1", "sequence": 101}],
            "status_update": None
        }

        # Act
        changed = self.book.process_luno_update(trade_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        # Internal check
        self.assertIn(Decimal("101"), self.book._asks_internal)
        self.assertIn("a1", self.book._order_id_map)  # Order still exists
        self.assertEqual(Decimal("3.0"), self.book._asks_internal[Decimal("101")]["a1"])  # 5.0 - 2.0
        # Base class check
        self.assertEqual(101, self.book.snapshot_uid)
        asks = list(self.book.ask_entries())
        self.assertEqual(1, len(asks))
        self.assertAlmostEqual(101.0, asks[0].price)
        self.assertAlmostEqual(3.0, asks[0].amount)

    def test_process_luno_update_trade_full_fill(self):
        # Arrange: Start with a snapshot containing the order to trade against
        snapshot_msg = {
            "sequence": "100", "timestamp": 1600000000000,
            "asks": [{"id": "a1", "price": "101", "volume": "2.0"}],  # Maker order
            "bids": []
        }
        self.book.process_luno_snapshot(snapshot_msg)

        trade_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": None, "delete_update": None,
            "trade_updates": [{"base": "2.0", "counter": "202.0", "maker_order_id": "a1", "taker_order_id": "t1", "sequence": 101}],
            "status_update": None
        }

        # Act
        changed = self.book.process_luno_update(trade_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        # Internal check
        self.assertNotIn(Decimal("101"), self.book._asks_internal)  # Price level removed
        self.assertNotIn("a1", self.book._order_id_map)  # Order removed from the map
        # Base class check
        self.assertEqual(101, self.book.snapshot_uid)
        asks = list(self.book.ask_entries())
        self.assertEqual(0, len(asks))  # Ask side empty

    def test_process_luno_update_old_sequence(self):
        # Arrange: Start with a snapshot
        snapshot_msg = {"sequence": "100", "timestamp": 1600000000000, "asks": [], "bids": []}
        self.book.process_luno_snapshot(snapshot_msg)

        old_update_msg = {
            "sequence": "99", "timestamp": 1600000001000,  # Sequence older than current
            "create_update": {"order_id": "b2", "type": "BID", "price": "99.5", "volume": "0.5"}
        }

        # Act
        changed = self.book.process_luno_update(old_update_msg)

        # Assert
        self.assertFalse(changed)
        self.assertEqual(100, self.book._sequence)  # Sequence unchanged

    def test_process_luno_update_sequence_gap(self):
        # Arrange: Start with a snapshot
        snapshot_msg = {"sequence": "100", "timestamp": 1600000000000, "asks": [], "bids": []}
        self.book.process_luno_snapshot(snapshot_msg)

        gap_update_msg = {
            "sequence": "102", "timestamp": 1600000001000,  # Expected 101
            "create_update": {"order_id": "b2", "type": "BID", "price": "99.5", "volume": "0.5"}
        }

        # Act & Assert
        with self.assertRaises(SequenceGapError) as cm:
            self.book.process_luno_update(gap_update_msg)

        self.assertEqual(101, cm.exception.expected)
        self.assertEqual(102, cm.exception.received)
        self.assertEqual(self.trading_pair, cm.exception.trading_pair)
        # Sequence should remain unchanged after gap detection
        self.assertEqual(100, self.book._sequence)

    # --- Test custom accessors ---
    def test_get_aggregated_snapshot(self):
        # Arrange
        snapshot_msg = {
            "sequence": "24352",
            "asks": [
                {"id": "ask_id_1", "price": "1235.00", "volume": "0.93"},
                {"id": "ask_id_2", "price": "1236.00", "volume": "1.50"},
                {"id": "ask_id_3", "price": "1235.00", "volume": "0.07"}  # Same price level
            ],
            "bids": [
                {"id": "bid_id_1", "price": "1234.00", "volume": "1.22"},
                {"id": "bid_id_2", "price": "1233.00", "volume": "2.00"}
            ],
            "status": "ACTIVE", "timestamp": 1528884331021
        }
        self.book.process_luno_snapshot(snapshot_msg)

        # Act
        agg_bids, agg_asks, seq = self.book.get_aggregated_snapshot()

        # Assert
        self.assertEqual(24352, seq)
        self.assertEqual(2, len(agg_bids))
        self.assertEqual(Decimal("1.22"), agg_bids[Decimal("1234.00")])
        self.assertEqual(Decimal("2.00"), agg_bids[Decimal("1233.00")])

        self.assertEqual(2, len(agg_asks))
        self.assertEqual(Decimal("1.00"), agg_asks[Decimal("1235.00")])  # 0.93 + 0.07
        self.assertEqual(Decimal("1.50"), agg_asks[Decimal("1236.00")])

    def test_parse_luno_timestamp_valid(self):
        # 1600000000000 ms → 1600000000.0 s
        ts = self.book._parse_luno_timestamp(1600000000000)
        self.assertAlmostEqual(1600000000.0, ts)

    def test_parse_luno_timestamp_invalid(self):
        # Should catch ValueError and return something close to now
        before = time.time()
        ts = self.book._parse_luno_timestamp("not_a_number")
        after = time.time()
        self.assertTrue(before <= ts <= after)

    def test_populate_side_skips_dust_and_includes_valid(self):
        orders = [
            {"id": "low", "price": "1.0", "volume": "1e-19"},  # below 1e-18
            {"id": "high", "price": "1.0", "volume": "1e-17"},  # above 1e-18
        ]
        self.book._populate_side_from_luno(self.book._bids_internal, orders, TradeType.BUY)
        self.assertNotIn("low", self.book._order_id_map)
        self.assertIn("high", self.book._order_id_map)

    def test_populate_side_handles_duplicate_order_id(self):
        # First snapshot
        orders1 = [{"id": "x1", "price": "1.0", "volume": "1.0"}]
        self.book._populate_side_from_luno(self.book._bids_internal, orders1, TradeType.BUY)

        # Then a duplicate arrives at a new price
        self.log_records.clear()
        self.book.logger().setLevel(logging.WARNING)
        self.book._populate_side_from_luno(
            self.book._bids_internal,
            [{"id": "x1", "price": "2.0", "volume": "2.0"}],
            TradeType.BUY
        )

        # Old price removed, new one present
        self.assertNotIn(Decimal("1.0"), self.book._bids_internal)
        self.assertIn(Decimal("2.0"), self.book._bids_internal)
        # Warning was logged
        self.assertTrue(any("Duplicate order ID 'x1'" in r.getMessage()
                            for r in self.log_records))

    def test_apply_snapshot_to_base_book_before_init_logs_warning(self):
        self.log_records.clear()
        self.book._apply_snapshot_to_base_book()
        self.assertTrue(any("Attempted to apply snapshot to base book before initialization"
                            in r.getMessage()
                            for r in self.log_records))

    def test_apply_diffs_logs_warning(self):
        self.log_records.clear()
        self.book.apply_diffs([["1", "1"]], [["2", "2"]], update_id=5)
        self.assertTrue(any(
            "Direct diff application is not supported" in rec.getMessage()
            for rec in self.log_records
        ))

    def test_get_internal_detailed_book_before_and_after_snapshot(self):
        # Before any snapshot
        self.assertIsNone(self.book.get_internal_detailed_book())

        # After snapshot
        snapshot = {"sequence": "1", "timestamp": 1000, "asks": [], "bids": []}
        self.book.process_luno_snapshot(snapshot)

        bids_copy, asks_copy, seq = self.book.get_internal_detailed_book()
        self.assertEqual(seq, 1)
        # Mutate the copy and verify original stays intact
        bids_copy[Decimal("9.9")] = {"foo": Decimal("0.1")}
        orig_bids, _, _ = self.book.get_internal_detailed_book()
        self.assertNotIn(Decimal("9.9"), orig_bids)

    def test_create_update_for_existing_order_logs_and_no_change(self):
        # Set up one bid
        snapshot = {"sequence": "1", "timestamp": 1000, "asks": [], "bids": [{"id": "b1", "price": "1.0", "volume": "1.0"}]}
        self.book.process_luno_snapshot(snapshot)

        self.log_records.clear()
        create_msg = {"order_id": "b1", "type": "BID", "price": "1.0", "volume": "1.0"}
        changed = self.book._apply_luno_update_internal({"create_update": create_msg})
        self.assertFalse(changed)
        self.assertTrue(any("Create update for existing order_id b1. Ignoring create."
                            in r.getMessage() for r in self.log_records))

    def test_trade_update_with_unknown_maker_no_log(self):
        snapshot = {"sequence": "1", "timestamp": 1000, "asks": [], "bids": []}
        self.book.process_luno_snapshot(snapshot)

        self.log_records.clear()
        update = {
            "trade_updates": [
                {"base": "1", "counter": "10", "maker_order_id": "nope", "taker_order_id": "t1"}
            ]
        }
        changed = self.book._apply_luno_update_internal(update)

        # Unknown maker should be silently skipped
        self.assertFalse(changed)
        self.assertEqual([], self.log_records)

    def test_status_update_logs_info(self):
        self.log_records.clear()
        self.book.logger().setLevel(logging.INFO)
        self.book._process_luno_status_update({"status_update": {"status": "PAUSED"}})
        self.assertTrue(any("Received status update: PAUSED" in r.getMessage()
                            for r in self.log_records))
