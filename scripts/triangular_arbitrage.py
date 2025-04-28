import logging
from decimal import Decimal, getcontext
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple  # Added Tuple

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType  # Added OrderType
from hummingbot.core.data_type.order_book import OrderBook  # For type hinting
from hummingbot.core.event.events import OrderFilledEvent  # Can be useful for more detail
from hummingbot.core.event.events import BuyOrderCompletedEvent, MarketOrderFailureEvent, SellOrderCompletedEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

# Increase Decimal precision for multi-step calc
getcontext().prec = 30  # Increased precision slightly more


class Status(Enum):
    INIT = auto()
    ACTIVE = auto()
    ARBITRAGE = auto()
    STOPPED = auto()


class TriangularArbitrage(ScriptStrategyBase):
    """
    (Docstring updated below)
    Executes sequential triangular arbitrage trades using market orders on a single exchange.

    - Monitors three specified trading pairs.
    - Calculates profitability based on current order book state, accounting for estimated taker fees.
    - If profitable, executes three market orders sequentially:
        - Starts by selling/buying a configured 'holding_asset'.
        - Second leg uses the proceeds of the first.
        - Third leg uses the proceeds of the second, aiming to end back in the 'holding_asset'.
    - Tracks overall profit/loss and stops if a 'kill_switch_rate' is breached.
    """
    # --- Configurable Parameters ---
    connector_name: str = "luno"
    # Define the three legs of the triangle
    first_pair: str = "USDC-USDT"
    second_pair: str = "XBT-USDC"
    third_pair: str = "XBT-USDT"
    # The asset the bot should hold idle and start/end trades with
    holding_asset: str = "USDT"
    # Order size in terms of the holding_asset
    order_amount_in_holding_asset: Decimal = Decimal("20")
    # Minimum required NET profitability (percentage) after estimated fees for all 3 legs
    min_profitability: Decimal = Decimal("1.0")  # Example: 0.3% = 3 * 0.1% fee
    # If total PnL% (relative to order_amount_in_holding_asset * num_trades) falls below this, stop.
    kill_switch_rate: Decimal = Decimal("-2.0")

    # --- ADD THIS CLASS ATTRIBUTE ---
    markets = {connector_name: {first_pair, second_pair, third_pair}}
    # ----------------------------------

    # --- Constants ---
    NUM_LEGS = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ——— Internal State ———
        self.status: Status = Status.INIT
        # Stores the ordered pairs for direct/reverse paths, e.g., {"direct": (p1, p2, p3)}
        self.trading_pairs: Dict[str, Tuple[str, str, str]] = {}
        # Stores the TradeType sequence for direct/reverse, e.g., {"direct": (BUY, SELL, SELL)}
        self.order_sides: Dict[str, Tuple[TradeType, TradeType, TradeType]] = {}
        # Stores calculated net profit % per direction for the last tick
        self.last_profit_pct: Dict[str, Decimal] = {"direct": Decimal("0"), "reverse": Decimal("0")}
        # Stores calculated order amounts for each leg (based on last tick's snapshot)
        self.last_calculated_amounts: Dict[str, List[Decimal]] = {}
        # State during an active arbitrage round
        self.current_leg: int = 0
        self.profitable_direction: str = ""  # "direct" or "reverse"
        self.initial_committed_amount: Decimal = Decimal("0")  # Amount of holding_asset committed
        # Running total PnL in holding_asset terms
        self.total_profit_holding_asset: Decimal = Decimal("0")
        self.total_trades_executed: int = 0  # Count completed rounds for PnL% calculation

    @property
    def connector(self):
        """Provides easy access to the configured connector."""
        # Accessing the initialized connectors dictionary from the base class
        if self.connector_name not in self.connectors:
            # This might happen if initialization failed or connectors haven't been set up yet
            self.logger().error(f"Connector '{self.connector_name}' not found in self.connectors.")
            # Raise an exception or handle gracefully depending on when this is called
            raise ValueError(f"Connector '{self.connector_name}' is not available.")
        return self.connectors[self.connector_name]

    def on_tick(self):
        """Main strategy loop executed periodically."""
        # 1) Initialize on first tick
        if self.status is Status.INIT:
            if not self._initialize():  # Initialization validates pairs
                self.stop()  # Stop strategy if initialization fails
            return

        # 2) Always calculate profitability, but only execute when ACTIVE
        execute_arbitrage = self.status is Status.ACTIVE

        # 3) Calculate potential profitability for both directions
        try:
            fee_pct = self.connector.get_fee(
                base_currency="",  # Base/Quote not relevant for percentage fee estimation
                quote_currency="",
                order_type=OrderType.MARKET,  # Use market order type
                order_side=TradeType.BUY,  # Side doesn't matter for percentage fee
                amount=Decimal("0"),  # Amount doesn't matter for percentage fee
                price=Decimal("0")
            ).percent  # Get taker fee percentage
        except Exception:
            self.logger().warning("Could not estimate connector fee. Assuming 0.45%.", exc_info=True)
            fee_pct = Decimal("0.0045")  # Default to 0.45% if estimation fails

        net_profit_after_fees = {}
        order_amounts = {}

        for direction in ("direct", "reverse"):
            gross_pct, amounts = self._calculate_potential_profit(direction)
            if amounts is None:  # Calculation failed (e.g., insufficient depth)
                net_profit_after_fees[direction] = Decimal("-Infinity")  # Mark as non-viable
                continue

            # Calculate net profit after estimated fees for all legs
            net_pct = gross_pct - (fee_pct * self.NUM_LEGS * Decimal("100"))
            net_profit_after_fees[direction] = net_pct
            order_amounts[direction] = amounts
            self.last_profit_pct[direction] = net_pct  # Store for status
            self.last_calculated_amounts[direction] = amounts  # Store for execution start

        # 4) Log potential profitability
        self.log_with_clock(
            logging.INFO,
            f"Potential Net Profitability: "
            f"Direct={net_profit_after_fees.get('direct', -999):.3f}%, "
            f"Reverse={net_profit_after_fees.get('reverse', -999):.3f}% "
            f"(Min Req: {self.min_profitability:.3f}%)"
        )

        # 5) Find the best profitable direction
        # Filter out directions where calculation failed
        valid_profits = {d: p for d, p in net_profit_after_fees.items() if p > Decimal("-Infinity")}
        if not valid_profits:
            return  # Neither direction calculable

        best_direction = max(valid_profits, key=valid_profits.get)

        # 6) Check if best direction meets minimum profitability threshold
        if valid_profits[best_direction] < self.min_profitability:
            return  # Not profitable enough

        # 7) Only proceed with execution if we're in ACTIVE state
        if execute_arbitrage:
            # Check available balance
            if not self._has_sufficient_balance():
                self.logger().warning(f"Insufficient {self.holding_asset} balance to start arbitrage.")
                return

            # 8) Start the arbitrage execution
            self.profitable_direction = best_direction
            self._start_arbitrage(best_direction)

    def _initialize(self) -> bool:
        """
        Validates configuration and sets up trading pairs and sides.
        Returns True if successful, False otherwise.
        """
        try:
            # Validate that exactly 3 unique assets are involved
            all_pairs = (self.first_pair, self.second_pair, self.third_pair)
            assets = set()
            for p in all_pairs:
                base, quote = split_hb_trading_pair(p)
                assets.add(base)
                assets.add(quote)

            if len(assets) != 3:
                self.logger().error(f"Configuration requires exactly 3 unique assets, found {len(assets)}: {assets}. Stopping.")
                self.status = Status.STOPPED
                return False
            if self.holding_asset not in assets:
                self.logger().error(f"Holding asset '{self.holding_asset}' must be one of the 3 assets involved: {assets}. Stopping.")
                self.status = Status.STOPPED
                return False

            # Reorder pairs to ensure the path starts and ends with the holding_asset
            # The goal is: Leg 1 converts Holding -> Asset B, Leg 2 converts Asset B -> Asset C, Leg 3 converts Asset C -> Holding
            pairs = list(all_pairs)
            # Find the pair that converts *from* holding asset (either buy base with holding, or sell holding for quote)
            start_pair_index = -1
            for i, pair in enumerate(pairs):
                base, quote = split_hb_trading_pair(pair)
                if base == self.holding_asset or quote == self.holding_asset:
                    start_pair_index = i
                    break
            if start_pair_index != 0:  # Rotate list if needed
                pairs = pairs[start_pair_index:] + pairs[:start_pair_index]

            # Find the pair that converts *to* holding asset
            end_pair_index = -1
            for i, pair in enumerate(pairs):
                base, quote = split_hb_trading_pair(pair)
                if base == self.holding_asset or quote == self.holding_asset:
                    # Ensure it's not the same as the start pair if possible (in a 3-pair setup)
                    if i != 0:
                        end_pair_index = i
                        break
            # If the end pair is index 1, swap index 1 and 2 to make it the last leg
            if end_pair_index == 1:
                pairs[1], pairs[2] = pairs[2], pairs[1]
            elif end_pair_index == -1:
                # This shouldn't happen if validation passed, but handle defensively
                self.logger().error("Could not determine pair order for arbitrage path. Check pairs and holding asset.")
                self.status = Status.STOPPED
                return False

            ordered_pairs = tuple(pairs)
            self.trading_pairs["direct"] = ordered_pairs
            self.trading_pairs["reverse"] = ordered_pairs[::-1]  # Reverse path uses pairs in reverse order

            # --- Determine Order Sides ---
            # Direct Path: A->B, B->C, C->A (where A is holding_asset)
            # Leg 1: Convert Holding (A) to Asset B. Side depends on pair structure.
            # Leg 2: Convert Asset B to Asset C.
            # Leg 3: Convert Asset C back to Holding (A).

            def get_required_sides(path_pairs: Tuple[str, str, str]) -> Tuple[TradeType, TradeType, TradeType]:
                sides = []
                current_asset = self.holding_asset
                for i, pair in enumerate(path_pairs):
                    base, quote = split_hb_trading_pair(pair)
                    if current_asset == quote:  # Need to buy base using current quote asset
                        side = TradeType.BUY
                        next_asset = base
                    elif current_asset == base:  # Need to sell base for quote asset
                        side = TradeType.SELL
                        next_asset = quote
                    else:
                        # Should not happen with correct pair ordering
                        raise ValueError(f"Logical error: Cannot trade pair {pair} with current asset {current_asset}")
                    sides.append(side)
                    current_asset = next_asset
                    # Final check: ensure last leg brings us back to holding asset
                    if i == 2 and current_asset != self.holding_asset:
                        raise ValueError(f"Logical error: Path does not end in holding asset. Ended with {current_asset}")
                return tuple(sides)

            self.order_sides["direct"] = get_required_sides(self.trading_pairs["direct"])
            # Reverse path: A->C, C->B, B->A (still starts/ends with A)
            # Sides are determined by the reversed pair order, not just flipping direct sides
            self.order_sides["reverse"] = get_required_sides(self.trading_pairs["reverse"])

            self.status = Status.ACTIVE
            self.log_with_clock(logging.INFO, f"TriangularArbitrage initialized. Holding: {self.holding_asset}.")
            self.log_with_clock(logging.INFO, f"Direct Path: {self.trading_pairs['direct']} | Sides: {[s.name for s in self.order_sides['direct']]}")
            self.log_with_clock(logging.INFO, f"Reverse Path: {self.trading_pairs['reverse']} | Sides: {[s.name for s in self.order_sides['reverse']]}")
            return True

        except Exception as e:
            self.logger().error(f"Error during initialization: {e}", exc_info=True)
            self.status = Status.STOPPED
            return False

    def _calculate_potential_profit(self, direction: str) -> Tuple[Optional[Decimal], Optional[List[Decimal]]]:
        """
        Calculates the estimated gross profit percentage and leg amounts for a given direction,
        simulating trades against the current order book.

        Returns: (gross_profit_pct, leg_amounts) or (None, None) if calculation fails.
        """
        pairs = self.trading_pairs[direction]
        sides = self.order_sides[direction]
        # Start simulation with the configured amount of holding asset
        available_amount: Decimal = self.order_amount_in_holding_asset
        current_asset: str = self.holding_asset
        simulated_leg_amounts: List[Decimal] = []  # Store base amount traded for each leg

        try:
            for leg_index, (pair, side) in enumerate(zip(pairs, sides)):
                base, quote = split_hb_trading_pair(pair)
                order_book = self.connector.get_order_book(pair)

                if side == TradeType.BUY:  # Buying base with quote
                    if current_asset != quote:
                        self.logger().error(f"Logic error in profit calc ({direction}, leg {leg_index + 1}): Trying to buy {pair} but holding {current_asset} instead of {quote}")
                        return None, None
                    quote_to_spend = available_amount
                    # Simulate buying base using quote_to_spend, considering slippage
                    base_received, quote_actually_spent = self._simulate_buy_base_with_quote(order_book, quote_to_spend)
                    if base_received is None:
                        self.logger().warning(f"Profit calc ({direction}, leg {leg_index + 1}): Not enough liquidity on {pair} asks to buy with {quote_to_spend:.4f} {quote}.")
                        return None, None
                    simulated_leg_amounts.append(base_received)
                    available_amount = base_received  # Now holding base
                    current_asset = base
                    # self.logger().debug(f"Sim BUY {base_received:.6f} {base} on {pair} spent {quote_actually_spent:.6f} {quote}")

                else:  # TradeType.SELL - Selling base for quote
                    if current_asset != base:
                        self.logger().error(f"Logic error in profit calc ({direction}, leg {leg_index + 1}): Trying to sell {pair} but holding {current_asset} instead of {base}")
                        return None, None
                    base_to_sell = available_amount
                    # Simulate selling base_to_sell, considering slippage
                    quote_received = self._simulate_sell_base_for_quote(order_book, base_to_sell)
                    if quote_received is None:
                        self.logger().warning(f"Profit calc ({direction}, leg {leg_index + 1}): Not enough liquidity on {pair} bids to sell {base_to_sell:.4f} {base}.")
                        return None, None
                    simulated_leg_amounts.append(base_to_sell)  # Selling this amount of base
                    available_amount = quote_received  # Now holding quote
                    current_asset = quote
                    # self.logger().debug(f"Sim SELL {base_to_sell:.6f} {base} on {pair} received {quote_received:.6f} {quote}")

            # After 3 legs, available_amount should be back in holding_asset
            if current_asset != self.holding_asset:
                self.logger().error(f"Profit calc ({direction}): Simulation did not end in {self.holding_asset}, ended in {current_asset}.")
                return None, None

            final_amount = Decimal(available_amount)
            gross_profit = final_amount - self.order_amount_in_holding_asset
            gross_pct = (gross_profit / self.order_amount_in_holding_asset) * Decimal("100") if self.order_amount_in_holding_asset > 0 else Decimal("0")

            # self.logger().debug(f"Profit calc ({direction}): Start {self.order_amount_in_holding_asset}, End {final_amount}, Gross {gross_profit}, Gross% {gross_pct:.4f}")
            return gross_pct, simulated_leg_amounts

        except Exception as e:
            self.logger().error(f"Error during profit calculation for {direction}: {e}", exc_info=True)
            return None, None

    def _simulate_buy_base_with_quote(self, order_book: OrderBook, quote_amount_to_spend: Decimal) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Simulates buying base asset by spending a target quote amount."""
        cumulative_base = Decimal("0")
        cumulative_quote_spent = Decimal("0")
        for entry in order_book.ask_entries():  # Iterate asks (lowest price first)
            price = Decimal(str(entry.price))
            available_volume_base = Decimal(str(entry.amount))
            available_volume_quote = price * available_volume_base

            quote_needed = quote_amount_to_spend - cumulative_quote_spent
            quote_to_use_from_level = min(quote_needed, available_volume_quote)

            if price <= 0:
                continue  # Skip invalid prices

            base_bought_on_level = quote_to_use_from_level / price
            cumulative_base += base_bought_on_level
            cumulative_quote_spent += quote_to_use_from_level

            if cumulative_quote_spent >= quote_amount_to_spend:
                # Ensure we didn't spend slightly more due to precision
                cumulative_quote_spent = quote_amount_to_spend
                break  # Target quote spent

        if cumulative_quote_spent < quote_amount_to_spend * Decimal("0.999"):  # Allow tiny tolerance
            return None, None  # Insufficient liquidity to spend the quote amount
        return cumulative_base, cumulative_quote_spent

    def _simulate_sell_base_for_quote(self, order_book: OrderBook, base_amount_to_sell: Decimal) -> Optional[Decimal]:
        """Simulates selling a target base amount for quote."""
        # Leverage Hummingbot's built-in simulation which handles walking the bids
        result = order_book.get_quote_volume_for_base_amount(is_buy=False, base_amount=base_amount_to_sell)
        # Check if the simulation could fulfill the entire amount
        if result.query_volume < Decimal(str(base_amount_to_sell)) * Decimal("0.999"):  # Allow tiny tolerance
            return None  # Insufficient liquidity
        return result.result_volume  # This is the quote amount received

    def _start_arbitrage(self, direction: str):
        """Initiates the first leg of the arbitrage."""
        if self.status is Status.ARBITRAGE:
            self.logger().warning("Arbitrage already in progress. Ignoring new start request.")
            return

        self.status = Status.ARBITRAGE
        self.current_leg = 0
        # Record the amount of holding asset we are committing to this round trip
        self.initial_committed_amount = self.order_amount_in_holding_asset

        pair = self.trading_pairs[direction][0]
        side = self.order_sides[direction][0]
        # Use the pre-calculated amount for the first leg based on the snapshot
        amt = self.last_calculated_amounts[direction][0]

        # --- Check Minimum Order Size ---
        min_size = self.connector.get_order_size_quantum(trading_pair=pair, order_side=side)  # Check base amount
        min_notional = self.connector.get_minimum_notional_size(trading_pair=pair)

        if amt < min_size:
            self.logger().warning(f"Calculated amount {amt} for leg 1 ({pair}) is below minimum size {min_size}. Aborting.")
            self.status = Status.ACTIVE
            return

        # Approximate notional check (can be refined)
        approx_price = self.connector.get_price_by_type(pair, side.get_order_price_type())
        notional_value = amt * approx_price if side == TradeType.BUY else self._simulate_sell_base_for_quote(self.connector.get_order_book(pair), amt) or Decimal("0")
        if notional_value < min_notional:
            self.logger().warning(f"Estimated notional value {notional_value} for leg 1 ({pair}) is below minimum {min_notional}. Aborting.")
            self.status = Status.ACTIVE
            return
        # --- End Check ---

        self.log_with_clock(logging.INFO,
                            f"Starting arbitrage ({direction.upper()}), Leg 1: {side.name} {amt:.8f} on {pair}. "
                            f"Profit forecast: {self.last_profit_pct[direction]:.3f}%"
                            )
        # Place market order for the first leg
        self.market_order(self.connector_name, pair, side, amt)

    # --- Order Event Handlers ---

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        # Note: Use base_asset_amount for buys
        self._on_leg_complete(event, event.base_asset_amount)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        # Note: Use quote_asset_amount for sells
        self._on_leg_complete(event, event.quote_asset_amount)

    def _on_leg_complete(self, event: OrderFilledEvent, received_amount: Decimal):
        """Handles completion of one leg and places the next, or finalizes."""
        # Skip if not our current round or if base_asset/quote_asset are None
        if self.status is not Status.ARBITRAGE or event.base_asset is None or event.quote_asset is None:
            return

        pair = f"{event.base_asset}-{event.quote_asset}"
        direction = self.profitable_direction
        leg_just_completed = self.current_leg

        # Verify the completed order corresponds to the expected leg
        expected_pair = self.trading_pairs[direction][leg_just_completed]
        if pair != expected_pair:
            self.logger().warning(f"Received completion event for unexpected pair {pair} (expected {expected_pair}). Ignoring.")
            return

        self.log_with_clock(logging.INFO,
                            f"Leg {leg_just_completed + 1} completed: {event.order_type.name} {event.order_side.name} "
                            f"{event.amount:.8f} {event.base_asset} @ avg {event.average_price:.6f} {event.quote_asset}. "
                            f"Received: {received_amount:.8f} "
                            f"{event.base_asset if event.order_side == TradeType.BUY else event.quote_asset}"
                            )

        self.current_leg += 1
        if self.current_leg < self.NUM_LEGS:
            # --- Place next leg ---
            next_pair = self.trading_pairs[direction][self.current_leg]
            next_side = self.order_sides[direction][self.current_leg]
            # The amount for the next leg IS the amount received from the previous leg
            amount_for_next_leg = received_amount

            # --- Check Minimum Order Size ---
            min_size = self.connector.get_order_size_quantum(trading_pair=next_pair, order_side=next_side)
            min_notional = self.connector.get_minimum_notional_size(trading_pair=next_pair)

            base_next, quote_next = split_hb_trading_pair(next_pair)
            # Determine base amount equivalent for checks
            if next_side == TradeType.BUY:  # Amount is in quote, need to convert to base for size check
                order_book = self.connector.get_order_book(next_pair)
                # Simulate buy to see how much base we'd get (approx)
                base_equiv, _ = self._simulate_buy_base_with_quote(order_book, amount_for_next_leg)
                base_equiv = base_equiv or Decimal("0")  # Handle None case
                notional_value = amount_for_next_leg  # Amount is quote
            else:  # Amount is in base
                base_equiv = amount_for_next_leg
                order_book = self.connector.get_order_book(next_pair)
                notional_value = self._simulate_sell_base_for_quote(order_book, amount_for_next_leg) or Decimal("0")  # Approx quote value

            if base_equiv < min_size:
                self.logger().error(f"Calculated amount {base_equiv:.8f} for leg {self.current_leg + 1} ({next_pair}) is below minimum size {min_size}. ABORTING ARBITRAGE.")
                # Strategy is now stuck holding intermediate asset - manual intervention might be needed!
                self.status = Status.STOPPED  # Stop to prevent further issues
                self.notify_hb_app_with_timestamp(f"ERROR: Arbitrage aborted mid-way due to minimum size constraint on leg {self.current_leg + 1}.")
                return
            if notional_value < min_notional:
                self.logger().error(
                    f"Estimated notional value {notional_value:.4f} for leg {self.current_leg + 1} ({next_pair}) is below minimum {min_notional}. ABORTING ARBITRAGE.")
                self.status = Status.STOPPED
                self.notify_hb_app_with_timestamp(f"ERROR: Arbitrage aborted mid-way due to minimum notional constraint on leg {self.current_leg + 1}.")
                return
            # --- End Check ---

            self.log_with_clock(logging.INFO, f"Placing Leg {self.current_leg + 1}: {next_side.name} {amount_for_next_leg:.8f} on {next_pair}")
            self.market_order(self.connector_name, next_pair, next_side, amount_for_next_leg)
        else:
            # --- All three legs completed -> Finalize ---
            self._finalize(received_amount)

    def did_fail_order(self, event: MarketOrderFailureEvent):
        """Handles failure of any market order during the arbitrage sequence."""
        if self.status is Status.ARBITRAGE:
            self.log_with_clock(logging.ERROR,
                                f"Order failure during arbitrage round (Leg {self.current_leg + 1}): "
                                f"Order ID {event.order_id}. ABORTING round."
                                )
            # Reset status to ACTIVE to allow trying again on the next tick
            self.status = Status.ACTIVE
            self.notify_hb_app_with_timestamp(f"WARNING: Arbitrage round aborted due to order failure on leg {self.current_leg + 1}.")

    def _finalize(self, final_received_amount: Decimal):
        """Calculates PnL for the completed round and updates overall status."""
        if self.status is not Status.ARBITRAGE:
            return  # Avoid finalizing multiple times

        # final_received_amount should be in holding_asset terms due to path design
        profit_absolute = final_received_amount - self.initial_committed_amount
        profit_pct = (profit_absolute / self.initial_committed_amount) * Decimal("100") if self.initial_committed_amount > 0 else Decimal("0")

        self.total_profit_holding_asset += profit_absolute
        self.total_trades_executed += 1  # Count completed rounds

        msg = (f"Arbitrage round ({self.profitable_direction.upper()}) complete. "
               f"PnL: {profit_absolute:.8f} {self.holding_asset} ({profit_pct:.3f}%). "
               f"Total PnL: {self.total_profit_holding_asset:.8f} {self.holding_asset}.")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

        # --- Check Kill Switch ---
        # Calculate overall PnL percentage based on total profit vs. total capital committed over time
        # A simple approximation: Total Profit / (Amount Per Trade * Number of Trades)
        if self.total_trades_executed > 0:
            total_committed = self.order_amount_in_holding_asset * self.total_trades_executed
            overall_profit_pct = (self.total_profit_holding_asset / total_committed) * Decimal("100") if total_committed > 0 else Decimal("0")

            self.log_with_clock(logging.INFO, f"Total Trades: {self.total_trades_executed}, Approx Overall PnL %: {overall_profit_pct:.3f}%")

            if overall_profit_pct < self.kill_switch_rate:
                self.status = Status.STOPPED
                self.logger().error(
                    f"Kill switch triggered! Overall PnL% ({overall_profit_pct:.3f}%) "
                    f"is below threshold ({self.kill_switch_rate:.3f}%). Stopping strategy."
                )
                self.notify_hb_app_with_timestamp("KILL SWITCH TRIGGERED - Strategy stopped.")
                return  # Stop here

        # Reset for the next opportunity
        self.status = Status.ACTIVE
        self.profitable_direction = ""
        self.current_leg = 0
        self.initial_committed_amount = Decimal("0")

    def _has_sufficient_balance(self) -> bool:
        """Checks if there is enough holding_asset balance for the configured order amount."""
        balance = self.connector.get_available_balance(self.holding_asset)
        return balance >= self.order_amount_in_holding_asset

    def format_status(self) -> str:
        """Returns status of the current strategy for display."""
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        lines.append(f"Strategy Status: {self.status.name}")

        # Calculate overall PnL % more reliably here
        overall_profit_pct = Decimal("0")
        if self.total_trades_executed > 0:
            total_committed = self.order_amount_in_holding_asset * self.total_trades_executed
            if total_committed > 0:
                overall_profit_pct = (self.total_profit_holding_asset / total_committed) * Decimal("100")

        lines.append(f"Total PnL: {self.total_profit_holding_asset:.8f} {self.holding_asset} "
                     f"(Trades: {self.total_trades_executed}, Approx Overall PnL: {overall_profit_pct:.3f}%)")
        lines.append(f"Kill Switch Threshold: {self.kill_switch_rate:.3f}%")

        # Show potential profitability from last tick
        lines.append("Last Calculated Net Profitability:")
        for direction in ("direct", "reverse"):
            profit_str = f"{self.last_profit_pct[direction]:.3f}%" if direction in self.last_profit_pct else "N/A"
            if direction in self.trading_pairs and direction in self.order_sides:  # Check if initialized
                pairs_str = [f"{side.name} {pair}" for side, pair in zip(self.order_sides[direction], self.trading_pairs[direction])]
                pairs_str = " -> ".join(pairs_str)
                lines.append(f"  {direction.capitalize()}: {profit_str} | Path: {pairs_str}")
            else:
                lines.append(f"  {direction.capitalize()}: {profit_str} | Path: (Not Initialized)")

        # Display balances
        balance_df = self.get_balance_df()
        lines.extend(["", "Balances:"] + ["  " + line for line in balance_df.to_string(index=False).split("\n")])

        # Display active orders (if any)
        try:
            df = self.active_orders_df()
            if not df.empty:
                lines.extend(["", "Active Orders:"] + ["  " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "No active orders."])  # Handle case where connector might raise error

        # Balance warning
        if not self._has_sufficient_balance():
            warning_lines.append(
                f"*** Holding asset ({self.holding_asset}) balance is BELOW configured order amount "
                f"({self.order_amount_in_holding_asset})! ***"
            )

        if warning_lines:
            lines.extend(["", "*** WARNINGS ***"] + ["  " + line for line in warning_lines])

        return "\n".join(lines)
