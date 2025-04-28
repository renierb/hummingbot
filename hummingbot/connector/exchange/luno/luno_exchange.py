import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

from bidict import bidict

from hummingbot.connector.budget_checker import BudgetChecker
from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.luno import luno_constants as CONSTANTS, luno_web_utils as web_utils
from hummingbot.connector.exchange.luno.luno_api_order_book_data_source import LunoAPIOrderBookDataSource
from hummingbot.connector.exchange.luno.luno_api_user_stream_data_source import LunoAPIUserStreamDataSource
from hummingbot.connector.exchange.luno.luno_auth import LunoAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import (
    AddedToCostTradeFee,
    DeductedFromReturnsTradeFee,
    TokenAmount,
    TradeFeeBase,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class LunoExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 luno_api_key: str,
                 luno_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 ):
        self.api_key = luno_api_key
        self.secret_key = luno_api_secret
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_luno_timestamp = 1.0
        super().__init__(client_config_map)

    @staticmethod
    def luno_order_type(order_type: OrderType) -> str:
        if order_type == OrderType.LIMIT:
            return CONSTANTS.ORDER_TYPE_LIMIT
        elif order_type == OrderType.MARKET:
            return CONSTANTS.ORDER_TYPE_MARKET
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

    @staticmethod
    def to_hb_order_type(luno_type: str) -> OrderType:
        if luno_type == CONSTANTS.ORDER_TYPE_LIMIT:
            return OrderType.LIMIT
        elif luno_type == CONSTANTS.ORDER_TYPE_MARKET:
            return OrderType.MARKET
        else:
            raise ValueError(f"Unsupported Luno order type: {luno_type}")

    @property
    def authenticator(self) -> LunoAuth:
        return LunoAuth(
            api_key=self.api_key,
            secret_key=self.secret_key)

    @property
    def name(self) -> str:
        return "luno"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return "com"  # Luno only has one domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.MARKETS_INFO_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKETS_INFO_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.TICKERS_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """Get ticker prices for all trading pairs"""
        tickers = await self._api_get(path_url=CONSTANTS.TICKERS_URL)
        return tickers.get("tickers", [])

    async def get_trading_rules(self) -> Dict[str, TradingRule]:
        """
        Retrieves the trading rules for the exchange.
        Returns a dictionary of trading rules.
        In case of an exception, returns an empty dictionary.
        """
        try:
            await self._update_trading_rules()
            return self._trading_rules
        except Exception:
            self.logger().warning("Error getting trading rules")
            return {}

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        # Luno doesn't require time synchronization like Binance,
        # so always return False
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        error_description = str(status_update_exception)
        return CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE in error_description and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in error_description

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        error_description = str(cancelation_exception)
        return CONSTANTS.UNKNOWN_ORDER_ERROR_CODE in error_description and CONSTANTS.UNKNOWN_ORDER_MESSAGE in error_description

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
            domain=self.domain)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return LunoAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return LunoAPIUserStreamDataSource(
            auth=cast(LunoAuth, self._auth),
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        Synchronous method to get the fee, used by scripts that don't support async calls.
        This method estimates the fee without making API calls.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _get_fee(self,
                       base_currency: str,
                       quote_currency: str,
                       order_type: OrderType,
                       order_side: TradeType,
                       amount: Decimal,
                       price: Decimal = s_decimal_NaN,
                       is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        Retrieves the fee for a trading pair using the Luno API fee_info endpoint.
        :param base_currency: The base currency of the trading pair
        :param quote_currency: The quote currency of the trading pair
        :param order_type: The type of the order (limit, market, etc.)
        :param order_side: The side of the order (buy or sell)
        :param amount: The amount to trade
        :param price: The price at which the order is to be placed
        :param is_maker: True if the order is a maker order, False if it is a taker order
        :return: A TradeFeeBase object containing the fee information
        """
        is_maker = order_type is OrderType.LIMIT_MAKER

        # Combine the base and quote currencies to form the trading pair
        trading_pair = combine_to_hb_trading_pair(base=base_currency, quote=quote_currency)

        # Convert the trading pair to the exchange symbol format
        try:
            exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

            # Get fee information from the Luno API
            params = {"pair": exchange_symbol}
            fee_info_response = await self._api_get(
                path_url=CONSTANTS.FEE_INFO_URL,
                params=params,
                is_auth_required=True
            )

            # Extract the maker and taker fees from the response
            maker_fee = Decimal(str(fee_info_response.get("maker_fee", "0")))
            taker_fee = Decimal(str(fee_info_response.get("taker_fee", "0")))

            # Use the appropriate fee based on whether the order is a maker or taker
            fee_percent = maker_fee if is_maker else taker_fee

            return AddedToCostTradeFee(percent=fee_percent)
        except Exception as e:
            self.logger().warning(f"Error fetching fee info for {trading_pair}: {e}")
            # Fall back to estimated fee if there's an error
            return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        """
        Places an order using the Luno API.
        Returns a tuple of the exchange order ID and the transaction timestamp.
        """
        exchange_order_id = ""
        timestamp = 0.0

        # Validate order against trading rules
        trading_rule = self._trading_rules[trading_pair]

        if amount < trading_rule.min_order_size:
            self.logger().warning(f"{trade_type.name.title()} order amount {amount} is lower than the minimum order "
                                  f"size {trading_rule.min_order_size}. The order will not be created, increase the "
                                  f"amount to be higher than the minimum order size.")
            raise ValueError(f"Order amount {amount} is less than the minimum order size {trading_rule.min_order_size}")

        if amount > trading_rule.max_order_size:
            self.logger().warning(f"{trade_type.name.title()} order amount {amount} is greater than the maximum order "
                                  f"size {trading_rule.max_order_size}. The order will not be created, decrease the "
                                  f"amount to be lower than the maximum order size.")
            raise ValueError(f"Order amount {amount} is greater than the maximum order size {trading_rule.max_order_size}")

        # Calculate notional value
        notional_size = price * amount
        if notional_size < trading_rule.min_notional_size:
            self.logger().warning(f"{trade_type.name.title()} order notional {notional_size} is lower than the "
                                  f"minimum notional size {trading_rule.min_notional_size}. The order will not be "
                                  f"created. Increase the amount or the price to be higher than the minimum notional.")
            raise ValueError(f"Order notional value {notional_size} is less than the minimum notional size {trading_rule.min_notional_size}")

        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # Determine order side
        side = ""
        if order_type == OrderType.LIMIT:
            side = trade_type.name.upper()
        else:  # MARKET
            side = CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL

        # Prepare API parameters based on order type
        if order_type == OrderType.LIMIT:
            # Place a limit order
            price_str = str(price)
            params = {
                "pair": exchange_symbol,
                "type": side,
                "volume": str(amount),
                "price": price_str,  # Use the exact decimal representation without any formatting
                "time_in_force": CONSTANTS.TIME_IN_FORCE_GTC,  # Good till cancelled
                "client_order_id": order_id
            }

            # Post the limit order
            response = await self._api_post(
                path_url=CONSTANTS.LIMIT_ORDER_URL,
                params=params,
                is_auth_required=True
            )

            exchange_order_id = str(response.get("order_id", ""))
            timestamp = self._time_synchronizer.time()  # Use current time as Luno doesn't return timestamp

        elif order_type == OrderType.MARKET:
            # Prepare parameters for market order
            params = {
                "pair": exchange_symbol,
                "type": side
            }

            # Add volume-specific parameter based on trade type
            if trade_type == TradeType.BUY:
                params["counter_volume"] = str(amount * price)  # For BUY, specify counter currency amount
            else:
                params["base_volume"] = str(amount)  # For SELL, specify base currency amount

            params["client_order_id"] = order_id

            # Post the market order
            response = await self._api_post(
                path_url=CONSTANTS.MARKET_ORDER_URL,
                params=params,
                is_auth_required=True
            )

            exchange_order_id = str(response.get("order_id", ""))
            timestamp = self._time_synchronizer.time()  # Use current time

        else:
            raise ValueError(f"Unsupported order type: {order_type}")

        return exchange_order_id, timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancels an order using the Luno API.
        """
        # Luno requires the exchange_order_id to cancel an order
        params = {
            "order_id": tracked_order.exchange_order_id
        }

        cancel_result = await self._api_post(
            path_url=CONSTANTS.LIMIT_ORDER_URL,
            params=params,
            is_auth_required=True
        )

        # Luno returns {"success": true} on successful cancellation
        return cancel_result.get("success", False)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Converts exchange trading pair information into a list of TradingRule instances.
        """
        trading_rules = []

        markets = exchange_info_dict.get("markets", [])
        for market_info in markets:
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=market_info.get("market_id"))

                # Extract trading rule parameters
                min_volume = Decimal(str(market_info.get("min_volume", "0")))
                max_volume = Decimal(str(market_info.get("max_volume", "0")))
                min_price = Decimal(str(market_info.get("min_price", "0")))
                price_scale = market_info.get("price_scale", 0)
                volume_scale = market_info.get("volume_scale", 0)

                # Calculate min increment values based on scale
                # Handle negative price_scale values correctly
                if price_scale < 0:
                    min_price_increment = Decimal(f"1e{abs(price_scale)}")
                else:
                    min_price_increment = Decimal(f"1e-{price_scale}")

                # Handle negative volume_scale values correctly
                if volume_scale < 0:
                    min_base_amount_increment = Decimal(f"1e{abs(volume_scale)}")
                else:
                    min_base_amount_increment = Decimal(f"1e-{volume_scale}")

                # Create TradingRule object
                rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_volume,
                    max_order_size=max_volume,
                    min_price_increment=min_price_increment,
                    min_base_amount_increment=min_base_amount_increment,
                    min_quote_amount_increment=None,  # Not provided by Luno
                    min_notional_size=min_price * min_volume,  # Minimum order value
                    min_order_value=min_price * min_volume,  # Same as min_notional_size
                    max_price_significant_digits=price_scale,
                    supports_limit_orders=True,
                    supports_market_orders=True
                )

                trading_rules.append(rule)

            except Exception as e:
                self.logger().error(
                    f"Error parsing trading pair rule {market_info}. Error: {e}. Skipping.",
                    exc_info=True
                )

        return trading_rules

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange using the fee_info endpoint
        """
        try:
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                params = {"pair": exchange_symbol}
                fee_info_response = await self._api_get(
                    path_url=CONSTANTS.FEE_INFO_URL,
                    params=params,
                    is_auth_required=True
                )

                # Extract the maker and taker fees from the response
                maker_fee = Decimal(str(fee_info_response.get("maker_fee", "0")))
                taker_fee = Decimal(str(fee_info_response.get("taker_fee", "0")))

                # Store the fees for later use
                self._trading_fees[trading_pair] = (maker_fee, taker_fee)

                self.logger().info(f"Updated trading fees for {trading_pair}: maker_fee={maker_fee}, taker_fee={taker_fee}")
        except Exception as e:
            self.logger().warning(f"Error updating trading fees: {e}")

    async def _user_stream_event_listener(self):
        """
        Processes events received from the user stream data source.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type", "")

                # Process order status updates
                if event_type == "order_status":
                    await self._process_order_status_update(event_message)

                # Process order fills
                elif event_type == "order_fill":
                    await self._process_order_fill_update(event_message)

                # Process balance updates
                elif event_type == "balance_update":
                    await self._process_balance_update(event_message)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(
                    f"Unexpected error in user stream listener: {e}",
                    exc_info=True
                )
                await asyncio.sleep(5.0)

    async def _process_order_status_update(self, event_message: Dict[str, Any]):
        """
        Processes order status update events from the user stream
        """
        order_id = event_message.get("order_id")
        client_order_id = event_message.get("client_order_id")
        new_status = event_message.get("status")

        tracked_order = None
        if client_order_id is not None:
            tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)

        if tracked_order is not None:
            # Convert Luno status to Hummingbot OrderState
            new_state = CONSTANTS.ORDER_STATE.get(new_status, None)

            if new_state is not None:
                order_update = OrderUpdate(
                    client_order_id=client_order_id,
                    exchange_order_id=order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self._time_synchronizer.time(),  # Use current time
                    new_state=new_state
                )

                self._order_tracker.process_order_update(order_update)

    async def _process_order_fill_update(self, event_message: Dict[str, Any]):
        """
        Processes order fill update events from the user stream
        """
        order_id = event_message.get("order_id")
        client_order_id = event_message.get("client_order_id")

        # Get base and counter delta amounts
        base_delta = Decimal(str(event_message.get("base_delta", "0")))
        counter_delta = Decimal(str(event_message.get("counter_delta", "0")))

        # Get fees
        base_fee = Decimal(str(event_message.get("base_fee", "0")))
        counter_fee = Decimal(str(event_message.get("counter_fee", "0")))
        fee_currency = event_message.get("fee_currency", "")

        tracked_order = None
        if client_order_id is not None:
            tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)

        if tracked_order is not None:
            trading_pair = tracked_order.trading_pair

            # Build TradeUpdate with the fill information
            fill_price = counter_delta / base_delta if base_delta > Decimal("0") else Decimal("0")

            # Create a trade fee object
            flat_fees = []
            if base_fee > Decimal("0"):
                flat_fees.append(TokenAmount(amount=base_fee, token=trading_pair.split("-")[0]))
            if counter_fee > Decimal("0"):
                flat_fees.append(TokenAmount(amount=counter_fee, token=trading_pair.split("-")[1]))

            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=tracked_order.trade_type,
                percent_token=fee_currency,
                flat_fees=flat_fees
            )

            trade_update = TradeUpdate(
                trade_id=f"{order_id}_{self._time_synchronizer.time()}",  # Generate a unique trade ID
                client_order_id=client_order_id,
                exchange_order_id=order_id,
                trading_pair=trading_pair,
                fee=fee,
                fill_base_amount=base_delta,
                fill_quote_amount=counter_delta,
                fill_price=fill_price,
                fill_timestamp=self._time_synchronizer.time(),
            )

            self._order_tracker.process_trade_update(trade_update)

    async def _process_balance_update(self, event_message: Dict[str, Any]):
        """
        Processes balance update events from the user stream
        """
        currency = None
        # Need to extract the currency from the response, it may vary based on Luno's format

        balance = Decimal(str(event_message.get("balance", "0")))
        available = Decimal(str(event_message.get("available", "0")))

        if currency is not None:
            self._account_available_balances[currency] = available
            self._account_balances[currency] = balance

    async def _update_order_fills_from_trades(self):
        """
        This is intended to be a backup method to get filled events with trade ID for orders,
        in case Luno's user stream events are not working.
        """
        small_interval_last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        small_interval_current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        long_interval_last_tick = self._last_poll_timestamp / self.LONG_POLL_INTERVAL
        long_interval_current_tick = self.current_timestamp / self.LONG_POLL_INTERVAL

        if (long_interval_current_tick > long_interval_last_tick
                or (self.in_flight_orders and small_interval_current_tick > small_interval_last_tick)):
            query_time = int(self._last_trades_poll_luno_timestamp * 1e3)
            self._last_trades_poll_luno_timestamp = self._time_synchronizer.time()
            order_by_exchange_id_map = {}
            for order in self._order_tracker.all_fillable_orders.values():
                order_by_exchange_id_map[order.exchange_order_id] = order

            tasks = []
            trading_pairs = self.trading_pairs
            for trading_pair in trading_pairs:
                pair_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                if pair_symbol is None:
                    self.logger().warning(f"Trading pair {trading_pair} not found in exchange.")
                    continue
                params = {
                    "pair": pair_symbol,
                    "since": query_time if self._last_poll_timestamp > 0 else None,
                    "limit": 100  # Fetch up to 100 recent trades
                }
                tasks.append(self._api_get(
                    path_url=CONSTANTS.TRADES_URL,
                    params=params,
                    is_auth_required=True))

            self.logger().debug(f"Polling for order fills of {len(tasks)} trading pairs.")
            results = await safe_gather(*tasks, return_exceptions=True)

            for trades, trading_pair in zip(results, trading_pairs):
                if isinstance(trades, Exception):
                    self.logger().network(
                        f"Error fetching trades update for {trading_pair}: {trades}.",
                        app_warning_msg=f"Failed to fetch trade update for {trading_pair}."
                    )
                    # Log more details about the error for debugging
                    self.logger().debug(f"Error details for {trading_pair}: {type(trades).__name__}, {str(trades)}")
                    continue

                # Check if trades is a list (expected format)
                if not isinstance(trades, list) and isinstance(trades, dict) and "trades" in trades:
                    trades = trades.get("trades", [])

                # Handle None trades to prevent TypeError
                if trades is None:
                    self.logger().warning(f"Received None instead of trades list for {trading_pair}. Skipping trade updates.")
                    continue

                for trade in trades:
                    # Skip invalid trades
                    if not trade or not isinstance(trade, dict):
                        continue

                    # Safely get order_id, handling None values
                    order_id = trade.get("order_id")
                    if order_id is None:
                        continue

                    exchange_order_id = str(order_id)
                    if exchange_order_id in order_by_exchange_id_map:
                        # This is a fill for a tracked order
                        tracked_order = order_by_exchange_id_map[exchange_order_id]

                        # Extract trade details with safe handling of None values
                        trade_id = str(trade.get("trade_id", ""))

                        # Safely convert price to Decimal, handling None values
                        price_str = trade.get("price")
                        price = Decimal(str(price_str if price_str is not None else "0"))

                        # Safely convert volume to Decimal, handling None values
                        volume_str = trade.get("volume")
                        amount = Decimal(str(volume_str if volume_str is not None else "0"))

                        # Safely convert fee to Decimal, handling None values
                        fee_str = trade.get("fee")
                        fee_amount = Decimal(str(fee_str if fee_str is not None else "0"))

                        fee_currency = trade.get("fee_currency", "")

                        # Safely handle timestamp, ensuring it's not None
                        timestamp_val = trade.get("timestamp", 0)
                        fill_timestamp = float(timestamp_val) * 1e-3 if timestamp_val is not None else 0.0

                        # Build trade update
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=tracked_order.trade_type,
                            percent_token=fee_currency,
                            flat_fees=[TokenAmount(amount=fee_amount, token=fee_currency)]
                        )

                        trade_update = TradeUpdate(
                            trade_id=trade_id,
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=exchange_order_id,
                            trading_pair=trading_pair,
                            fee=fee,
                            fill_base_amount=amount,
                            fill_quote_amount=amount * price,
                            fill_price=price,
                            fill_timestamp=fill_timestamp,
                        )

                        self._order_tracker.process_trade_update(trade_update)

                    elif self.is_confirmed_new_order_filled_event(
                            str(trade.get("trade_id", "")), exchange_order_id, trading_pair):
                        # This is a fill of an order registered in the DB but not tracked anymore
                        self._current_trade_fills.add(TradeFillOrderDetails(
                            market=self.display_name,
                            exchange_trade_id=str(trade.get("trade_id", "")),
                            symbol=trading_pair))

                        # Get timestamp with safe handling of None values
                        timestamp_val = trade.get("timestamp", 0)
                        timestamp = float(timestamp_val) * 1e-3 if timestamp_val is not None else 0.0  # Convert to seconds if in milliseconds

                        # Get order_id with safe handling
                        order_id_val = trade.get("order_id")
                        order_id = self._exchange_order_ids.get(str(order_id_val) if order_id_val is not None else "", None)

                        # Safely convert price to Decimal, handling None values
                        price_str = trade.get("price")
                        price = Decimal(str(price_str if price_str is not None else "0"))

                        # Safely convert volume to Decimal, handling None values
                        volume_str = trade.get("volume")
                        amount = Decimal(str(volume_str if volume_str is not None else "0"))

                        # Safely convert fee to Decimal, handling None values
                        fee_str = trade.get("fee")
                        fee_amount = Decimal(str(fee_str if fee_str is not None else "0"))

                        fee_currency = trade.get("fee_currency", "")

                        # Create a filled event
                        self.trigger_event(
                            MarketEvent.OrderFilled,
                            OrderFilledEvent(
                                timestamp=timestamp,
                                order_id=order_id,
                                trading_pair=trading_pair,
                                trade_type=TradeType.BUY if trade.get("is_buy", False) else TradeType.SELL,
                                order_type=OrderType.LIMIT,  # Assuming limit orders by default
                                price=price,
                                amount=amount,
                                trade_fee=DeductedFromReturnsTradeFee(
                                    flat_fees=[
                                        TokenAmount(
                                            fee_currency,
                                            fee_amount
                                        )
                                    ]
                                ),
                                exchange_trade_id=str(trade.get("trade_id", ""))
                            ))
                        self.logger().info(f"Recreating missing trade in TradeFill: {trade}")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Retrieves all trades for a specific order
        :param order: the order for which to retrieve the trades
        :return: a list of TradeUpdate objects
        """
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = order.exchange_order_id
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)

            # Fetch trades for the order using Luno's API
            all_fills_response = await self._api_get(
                path_url=CONSTANTS.TRADES_URL,
                params={
                    "pair": trading_pair,
                    "order_id": exchange_order_id
                },
                is_auth_required=True,
                limit_id=CONSTANTS.TRADES_URL)

            # Check if response is a dictionary containing trades list
            trades = []
            if all_fills_response is None:
                self.logger().warning(f"Received None response when fetching trades for order {exchange_order_id}")
            elif isinstance(all_fills_response, dict) and "trades" in all_fills_response:
                trades = all_fills_response["trades"]
            elif isinstance(all_fills_response, list):
                trades = all_fills_response

            for trade in trades:
                # Skip invalid trades
                if not trade or not isinstance(trade, dict):
                    continue

                # Extract trade information with safe handling of None values
                trade_id = str(trade.get("trade_id", ""))

                # Safely convert price to Decimal, handling None values
                price_str = trade.get("price")
                price = Decimal(str(price_str if price_str is not None else "0"))

                # Safely convert volume to Decimal, handling None values
                volume_str = trade.get("volume")
                amount = Decimal(str(volume_str if volume_str is not None else "0"))

                # Safely convert fee to Decimal, handling None values
                fee_str = trade.get("fee")
                fee = Decimal(str(fee_str if fee_str is not None else "0"))

                fee_currency = trade.get("fee_currency", "")

                # Safely handle timestamp, ensuring it's not None
                timestamp_val = trade.get("timestamp", 0)
                timestamp = float(timestamp_val) * 1e-3 if timestamp_val is not None else 0.0  # Convert to seconds if in milliseconds

                # Create fee object
                trade_fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=fee_currency,
                    flat_fees=[TokenAmount(amount=fee, token=fee_currency)]
                )

                # Create TradeUpdate
                trade_update = TradeUpdate(
                    trade_id=trade_id,
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=order.trading_pair,
                    fee=trade_fee,
                    fill_base_amount=amount,
                    fill_quote_amount=amount * price,
                    fill_price=price,
                    fill_timestamp=timestamp,
                )

                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Requests order status from the exchange for a specific order
        :param tracked_order: the order for which to request the status
        :return: an OrderUpdate object
        """
        exchange_order_id = tracked_order.exchange_order_id
        client_order_id = tracked_order.client_order_id

        # Check if we have an exchange order ID
        if not exchange_order_id and not client_order_id:
            # We can't request order status without either ID
            raise ValueError("Unable to request order status without exchange_order_id or client_order_id")

        # Prepare the request parameters based on available IDs
        params = {}
        if exchange_order_id:
            params["id"] = exchange_order_id
        if client_order_id:
            params["client_order_id"] = client_order_id

        # Request order details from Luno
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.GET_ORDER_URL,
            params=params,
            is_auth_required=True)

        # Extract order state from response
        status = updated_order_data.get("state", updated_order_data.get("status", ""))
        new_state = CONSTANTS.ORDER_STATE.get(status, None)

        if new_state is None:
            self.logger().warning(f"Unrecognized order status: {status} for {client_order_id}")
            # Use a default state to avoid errors
            new_state = OrderState.PENDING_CREATE

        # Create OrderUpdate
        order_update = OrderUpdate(
            client_order_id=client_order_id,
            exchange_order_id=str(updated_order_data.get("order_id", exchange_order_id)),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._time_synchronizer.time(),  # Use current time as Luno doesn't provide update time
            new_state=new_state,
        )

        # If the order is complete, create a trade update to update the executed amount
        if status == "COMPLETE" and tracked_order.executed_amount_base < tracked_order.amount:
            # Create a trade update with the full order amount
            trade_id = f"{exchange_order_id}_{self._time_synchronizer.time()}"
            trade_update = TradeUpdate(
                trade_id=trade_id,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                fee=DeductedFromReturnsTradeFee(),  # Use a default fee as we don't have fee info
                fill_base_amount=tracked_order.amount - tracked_order.executed_amount_base,
                fill_quote_amount=(tracked_order.amount - tracked_order.executed_amount_base) * tracked_order.price,
                fill_price=tracked_order.price,
                fill_timestamp=self._time_synchronizer.time(),
            )
            self._order_tracker.process_trade_update(trade_update)

        return order_update

    async def get_account_balances(self):
        """
        Retrieves all account balances.

        :return: A dictionary of all account balances, with asset names as keys and objects with 'available' and 'locked' properties as values.
        """
        from dataclasses import dataclass

        @dataclass
        class Balance:
            available: Decimal
            locked: Decimal

        # Fetch account balance information
        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_URL,
            is_auth_required=True)

        balances = {}

        # Process balance entries
        for balance_entry in account_info.get("balance", []):
            asset_name = balance_entry.get("asset")
            total_balance = Decimal(balance_entry.get("balance", "0"))
            reserved_balance = Decimal(balance_entry.get("reserved", "0"))

            balances[asset_name] = Balance(
                available=total_balance,
                locked=reserved_balance
            )

        return balances

    async def _update_balances(self):
        """
        Updates the local balances with the latest information from the exchange
        """
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        # Fetch account balance information
        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_URL,
            is_auth_required=True)

        # Process balance updates
        for balance_entry in account_info.get("balance", []):
            asset_name = balance_entry.get("asset")
            total_balance = Decimal(balance_entry.get("balance", "0"))
            available_balance = total_balance - Decimal(balance_entry.get("reserved", "0"))

            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        # Remove assets that are no longer in the remote balances
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initializes the trading pair symbols map from exchange info
        :param exchange_info: the exchange info response
        """
        mapping = bidict()

        for market_data in exchange_info.get("markets", []):
            symbol = market_data.get("market_id")
            base_asset = market_data.get("base_currency")
            quote_asset = market_data.get("counter_currency")

            if symbol and base_asset and quote_asset:
                mapping[symbol] = combine_to_hb_trading_pair(base=base_asset, quote=quote_asset)

        self._set_trading_pair_symbol_map(mapping)

    async def _initialize_trading_pair_symbol_map(self):
        """
        Override the parent method to clear the trading pair symbol map when the API call fails.
        This ensures that all_trading_pairs() returns an empty list when the API call fails.
        """
        try:
            exchange_info = await self._make_trading_pairs_request()
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")
            # Clear the trading pair symbol map when the API call fails
            self._set_trading_pair_symbol_map(bidict())

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        Return a dictionary the trading_pair as key and the current price as value for each trading pair passed as
        parameter
        :param trading_pairs: list of trading pairs to get the prices for
        :return: Dictionary of associations between token pair and its latest price
        """
        tasks = [self._get_last_traded_price(trading_pair=trading_pair) for trading_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    async def _get_last_traded_price(self, trading_pair: str) -> Decimal:
        """
        Retrieves the last traded price for a trading pair using the trades endpoint
        :param trading_pair: the trading pair
        :return: the last traded price
        """
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # Request recent trades for the pair
        params = {
            "pair": exchange_symbol
        }

        trades_response = await self._api_get(
            path_url=CONSTANTS.TRADES_URL,
            params=params
        )

        # Extract the most recent trade price (trades are sorted from newest to oldest)
        trades = trades_response.get("trades", [])
        if not trades:
            self.logger().warning(f"No trades found for {trading_pair}. Returning 0.")
            return Decimal("0")

        # Get the most recent trade (first in the list)
        most_recent_trade = trades[0]
        last_price = Decimal(str(most_recent_trade.get("price", "0")))
        return last_price

    def get_price_for_volume(self, trading_pair: str, is_buy: bool, volume: Decimal):
        """
        Returns the price required for an order with the given volume.
        :param trading_pair: The trading pair for which to calculate the price
        :param is_buy: True if the order is a buy order, False otherwise
        :param volume: The volume of the order
        :return: A ClientOrderBookQueryResult with the required price
        """
        from decimal import Decimal

        from hummingbot.core.data_type.order_book_query_result import ClientOrderBookQueryResult

        order_book = self.get_order_book(trading_pair)

        # Get the appropriate side of the order book
        entries = order_book.ask_entries() if is_buy else order_book.bid_entries()

        # Calculate the price for the given volume
        cumulative_volume = Decimal('0')
        weighted_price = Decimal('0')
        result_price = Decimal('0')

        for entry in entries:
            entry_price = Decimal(str(entry.price))
            entry_amount = Decimal(str(entry.amount))

            if cumulative_volume + entry_amount >= volume:
                # This entry will complete the volume
                remaining_volume = volume - cumulative_volume
                weighted_price += entry_price * remaining_volume
                cumulative_volume += remaining_volume
                result_price = weighted_price / cumulative_volume
                break
            else:
                # Add this entry's contribution
                weighted_price += entry_price * entry_amount
                cumulative_volume += entry_amount
                result_price = weighted_price / cumulative_volume if cumulative_volume > 0 else Decimal('0')

        # Return the result in the expected format
        return ClientOrderBookQueryResult(
            Decimal('NaN'),  # query_price
            volume,  # query_volume
            result_price,  # result_price
            cumulative_volume  # result_volume
        )

    def get_quote_volume_for_base_amount(self, trading_pair: str, is_buy: bool, base_amount: Decimal):
        """
        Calculates the quote volume needed for a given base amount.
        :param trading_pair: The trading pair for which to calculate the volume
        :param is_buy: True if the order is a buy order, False otherwise
        :param base_amount: The base amount for which to calculate the quote volume
        :return: A ClientOrderBookQueryResult with the required quote volume
        """
        from decimal import Decimal

        from hummingbot.core.data_type.order_book_query_result import ClientOrderBookQueryResult

        order_book = self.get_order_book(trading_pair)

        # Get the appropriate side of the order book
        entries = order_book.ask_entries() if is_buy else order_book.bid_entries()

        # Calculate the quote volume for the given base amount
        cumulative_base_amount = Decimal('0')
        quote_volume = Decimal('0')

        for entry in entries:
            entry_price = Decimal(str(entry.price))
            entry_amount = Decimal(str(entry.amount))

            if cumulative_base_amount + entry_amount >= base_amount:
                # This entry will complete the base amount
                remaining_base_amount = base_amount - cumulative_base_amount
                quote_volume += remaining_base_amount * entry_price
                cumulative_base_amount += remaining_base_amount
                break
            else:
                # Add this entry's contribution
                quote_volume += entry_amount * entry_price
                cumulative_base_amount += entry_amount

        # Return the result in the expected format
        return ClientOrderBookQueryResult(
            Decimal('NaN'),  # query_price
            base_amount,  # query_volume
            Decimal('NaN'),  # result_price
            quote_volume  # result_volume
        )

    def get_order_book(self, trading_pair: str) -> OrderBook:
        """
        Returns the current order book for a particular market.
        :param trading_pair: the pair of tokens for which the order book should be retrieved
        :return: OrderBook for the specified trading pair
        """
        if trading_pair not in self.order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self.order_book_tracker.order_books[trading_pair]

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        """
        Override the default quantize_order_price to ensure that the price is not truncated.
        Instead of using integer division which truncates, we use Decimal's quantize method with
        ROUND_HALF_UP rounding mode to ensure proper rounding.
        """

        if price.is_nan():
            return price

        trading_rule = self._trading_rules[trading_pair]
        price_quantum = Decimal(trading_rule.min_price_increment)

        # Use Decimal's quantize method with ROUND_HALF_UP to ensure proper rounding
        quantized_price = price.quantize(price_quantum, rounding='ROUND_HALF_UP')

        return quantized_price

    @property
    def budget_checker(self) -> BudgetChecker:
        """
        Returns the BudgetChecker associated with this exchange.
        """
        # noinspection PyUnresolvedReferences
        return self._budget_checker
