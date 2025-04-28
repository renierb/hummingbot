import logging
import unittest
from decimal import Decimal

from hummingbot.connector.exchange.luno.luno_order_book import LunoOrderBook, SequenceGapError
from hummingbot.core.data_type.common import TradeType

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
        self.book.process_snapshot(snapshot_msg)

        # Assert Internal State
        self.assertEqual(24352, self.book._sequence)
        self.assertEqual(2, len(self.book._bids))
        self.assertEqual(2, len(self.book._asks))
        self.assertIn("bid_id_1", self.book._order_map)
        self.assertIn("ask_id_1", self.book._order_map)
        self.assertEqual((Decimal("1234.00"), TradeType.BUY), self.book._order_map["bid_id_1"])
        self.assertEqual((Decimal("1235.00"), TradeType.SELL), self.book._order_map["ask_id_1"])

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
        self.book.process_snapshot(snapshot_msg)

        create_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": {"order_id": "b2", "type": "BID", "price": "99.5", "volume": "0.5"},
            "delete_update": None, "trade_updates": None, "status_update": None
        }

        # Act
        changed = self.book.process_update(create_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        # Internal check
        self.assertIn(Decimal("99.5"), self.book._bids)
        self.assertEqual(Decimal("0.5"), self.book._bids[Decimal("99.5")]["b2"])
        self.assertIn("b2", self.book._order_map)
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
        self.book.process_snapshot(snapshot_msg)
        self.assertEqual(2, len(list(self.book.ask_entries())))
        self.assertIn("a1", self.book._order_map)

        delete_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": None,
            "delete_update": {"order_id": "a1"},  # Delete the first ask
            "trade_updates": None, "status_update": None
        }

        # Act
        changed = self.book.process_update(delete_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        # Internal check
        self.assertNotIn(Decimal("101"), self.book._asks)  # Price level removed
        self.assertNotIn("a1", self.book._order_map)
        self.assertIn(Decimal("102"), self.book._asks)  # Other asks still exist
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
        self.book.process_snapshot(snapshot_msg)
        self.assertEqual(Decimal("5.0"), self.book._asks[Decimal("101")]["a1"])

        trade_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": None, "delete_update": None,
            "trade_updates": [{"base": "2.0", "counter": "202.0", "maker_order_id": "a1", "taker_order_id": "t1", "sequence": 101}],
            "status_update": None
        }

        # Act
        changed = self.book.process_update(trade_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        # Internal check
        self.assertIn(Decimal("101"), self.book._asks)
        self.assertIn("a1", self.book._order_map)  # Order still exists
        self.assertEqual(Decimal("3.0"), self.book._asks[Decimal("101")]["a1"])  # 5.0 - 2.0
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
        self.book.process_snapshot(snapshot_msg)

        trade_msg = {
            "sequence": "101", "timestamp": 1600000001000,
            "create_update": None, "delete_update": None,
            "trade_updates": [{"base": "2.0", "counter": "202.0", "maker_order_id": "a1", "taker_order_id": "t1", "sequence": 101}],
            "status_update": None
        }

        # Act
        changed = self.book.process_update(trade_msg)

        # Assert
        self.assertTrue(changed)
        self.assertEqual(101, self.book._sequence)
        # Internal check
        self.assertNotIn(Decimal("101"), self.book._asks)  # Price level removed
        self.assertNotIn("a1", self.book._order_map)  # Order removed from the map
        # Base class check
        self.assertEqual(101, self.book.snapshot_uid)
        asks = list(self.book.ask_entries())
        self.assertEqual(0, len(asks))  # Ask side empty

    def test_process_luno_update_old_sequence(self):
        # Arrange: Start with a snapshot
        snapshot_msg = {"sequence": "100", "timestamp": 1600000000000, "asks": [], "bids": []}
        self.book.process_snapshot(snapshot_msg)

        old_update_msg = {
            "sequence": "99", "timestamp": 1600000001000,  # Sequence older than current
            "create_update": {"order_id": "b2", "type": "BID", "price": "99.5", "volume": "0.5"}
        }

        # Act
        changed = self.book.process_update(old_update_msg)

        # Assert
        self.assertFalse(changed)
        self.assertEqual(100, self.book._sequence)  # Sequence unchanged

    def test_process_luno_update_sequence_gap(self):
        # Arrange: Start with a snapshot
        snapshot_msg = {"sequence": "100", "timestamp": 1600000000000, "asks": [], "bids": []}
        self.book.process_snapshot(snapshot_msg)

        gap_update_msg = {
            "sequence": "102", "timestamp": 1600000001000,  # Expected 101
            "create_update": {"order_id": "b2", "type": "BID", "price": "99.5", "volume": "0.5"}
        }

        # Act & Assert
        with self.assertRaises(SequenceGapError) as cm:
            self.book.process_update(gap_update_msg)

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
        self.book.process_snapshot(snapshot_msg)

        # Act
        agg_bids, agg_asks = self.book.get_aggregated_snapshot(24352)

        # Assert
        self.assertEqual(2, len(agg_bids))
        self.assertEqual(1.22, agg_bids[0][1])
        self.assertEqual(2.00, agg_bids[1][1])

        self.assertEqual(2, len(agg_asks))
        self.assertEqual(1.00, agg_asks[0][1])  # 0.93 + 0.07
        self.assertEqual(1.50, agg_asks[1][1])

    def test_populate_side_skips_dust_and_includes_valid(self):
        orders = [
            {"id": "low", "price": "1.0", "volume": "1e-19"},  # below 1e-18
            {"id": "high", "price": "1.0", "volume": "1e-17"},  # above 1e-18
        ]
        self.book._load_side(self.book._bids, orders, TradeType.BUY)
        self.assertNotIn("low", self.book._order_map)
        self.assertIn("high", self.book._order_map)

    def test_populate_side_handles_duplicate_order_id(self):
        # First snapshot
        orders1 = [{"id": "x1", "price": "1.0", "volume": "1.0"}]
        self.book._load_side(self.book._bids, orders1, TradeType.BUY)

        # Then a duplicate arrives at a new price
        self.log_records.clear()
        self.book.logger().setLevel(logging.WARNING)
        self.book._load_side(
            self.book._bids,
            [{"id": "x1", "price": "2.0", "volume": "2.0"}],
            TradeType.BUY
        )

        # Old price removed, new one present
        self.assertNotIn(Decimal("1.0"), self.book._bids)
        self.assertIn(Decimal("2.0"), self.book._bids)
        # Warning was logged
        self.assertTrue(any("Duplicate order ID 'x1'" in r.getMessage()
                            for r in self.log_records))

    def test_create_update_for_existing_order_logs_and_no_change(self):
        # Set up one bid
        snapshot = {"sequence": "1", "timestamp": 1000, "asks": [], "bids": [{"id": "b1", "price": "1.0", "volume": "1.0"}]}
        self.book.process_snapshot(snapshot)

        self.log_records.clear()
        create_msg = {"order_id": "b1", "type": "BID", "price": "1.0", "volume": "1.0"}
        changed = self.book._apply_create(create_msg)
        self.assertFalse(changed)
        self.assertTrue(any("Create update for existing order_id b1. Ignoring create."
                            in r.getMessage() for r in self.log_records))

    def test_trade_update_with_unknown_maker_no_log(self):
        snapshot = {"sequence": "1", "timestamp": 1000, "asks": [], "bids": []}
        self.book.process_snapshot(snapshot)

        self.log_records.clear()
        update = {"base": "1", "counter": "10", "maker_order_id": "nope", "taker_order_id": "t1"}
        changed = self.book._apply_trade_update(update)

        # Unknown maker should be silently skipped
        self.assertFalse(changed)
        self.assertEqual([], self.log_records)
