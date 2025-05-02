import asyncio  # Added for safe_gather type hinting if needed
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional  # Added List, Any

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.exchange.luno.luno_exchange import LunoExchange


class LunoRateSource(RateSourceBase):
    # Initialization with delayed exchange reference
    def __init__(self):
        super().__init__()
        self._luno_exchange: Optional[LunoExchange] = None

    @property
    def name(self) -> str:
        return "luno"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        """
        Fetches mid-prices from Luno tickers and caches them.
        Filters by quote_token if provided.
        """
        self.logger().info(f"LunoRateSource: Getting prices with quote_token={quote_token}")
        self._ensure_exchanges()  # Ensure connector instance exists
        self.logger().info(f"LunoRateSource: Exchange instance ready: {self._luno_exchange is not None}")

        results = {}
        tasks = [
            self._get_luno_prices(exchange=self._luno_exchange, quote_token=quote_token),
        ]
        self.logger().info("LunoRateSource: Fetching prices from Luno...")
        task_results: List[Any] = await safe_gather(*tasks, return_exceptions=True)

        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg=f"Unexpected error while retrieving rates from Luno: {str(task_result)}",
                    exc_info=task_result,
                )
                break  # Stop processing further tasks if one fails
            elif isinstance(task_result, dict):  # Ensure each result is a dict
                self.logger().info(f"LunoRateSource: Got {len(task_result)} prices from Luno")
                if task_result:
                    self.logger().info(f"LunoRateSource: Available pairs: {list(task_result.keys())}")
                results.update(task_result)
            else:
                self.logger().warning(f"LunoRateSource: Unexpected result type: {type(task_result)}")

        self.logger().info(f"LunoRateSource: Returning {len(results)} prices")
        return results

    def _ensure_exchanges(self):
        """Creates the LunoExchange instance if it doesn't exist."""
        if self._luno_exchange is None:
            self.logger().info("LunoRateSource: No Luno exchange instance exists, creating one")
            self._luno_exchange = self._build_luno_connector_without_private_keys()
            self.logger().info("LunoRateSource: Luno exchange instance created")
        else:
            self.logger().info("LunoRateSource: Using existing Luno exchange instance")

    @staticmethod
    async def _get_luno_prices(exchange: 'LunoExchange', quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        """
        Internal method to fetch and process prices from Luno tickers API.

        :param exchange: The LunoExchange instance.
        :param quote_token: Optional quote token to filter results by.
        :return: Dictionary mapping Hummingbot trading pairs to their mid-price.
        """
        results = {}
        try:
            exchange.logger().info("LunoRateSource: Fetching all pairs prices from Luno API...")
            # Assuming get_all_pairs_prices returns a list like [{"pair": "XBTZAR", "bid": "...", "ask": "..."}, ...]
            # Verify this method and its return format in LunoExchange.
            # If it's get_tickers(), adjust parsing below.
            pairs_prices = await exchange.get_all_pairs_prices()  # Verify this method exists/works

            if not isinstance(pairs_prices, list):
                exchange.logger().warning(f"LunoRateSource: get_all_pairs_prices did not return a list: {type(pairs_prices)}")
                return results  # Return empty if the format is wrong

            exchange.logger().info(f"LunoRateSource: Received {len(pairs_prices)} pairs from Luno API")

            for price_data in pairs_prices:
                if not isinstance(price_data, dict):
                    exchange.logger().warning(f"LunoRateSource: Skipping non-dict price data: {type(price_data)}")
                    continue  # Skip invalid entries

                exchange_symbol = price_data.get("pair")
                if not exchange_symbol:
                    exchange.logger().warning(f"LunoRateSource: Skipping entry without pair symbol: {price_data}")
                    continue  # Skip entries without a pair symbol

                exchange.logger().info(f"LunoRateSource: Processing pair: {exchange_symbol}")

                try:
                    # Convert exchange symbol to HB trading pair format
                    trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=exchange_symbol)
                    exchange.logger().info(f"LunoRateSource: Converted {exchange_symbol} to {trading_pair}")
                except KeyError:
                    exchange.logger().debug(f"LunoRateSource: Skipping unsupported Luno pair: {exchange_symbol}")
                    continue  # Skip pairs not tracked or mapped by the connector instance

                # --- Apply quote_token filtering ---
                if quote_token is not None:
                    try:
                        base, quote = split_hb_trading_pair(trading_pair=trading_pair)
                        exchange.logger().info(f"LunoRateSource: Split {trading_pair} into base={base}, quote={quote}")
                        if quote != quote_token:
                            exchange.logger().info(f"LunoRateSource: Skipping {trading_pair} as quote {quote} != {quote_token}")
                            continue  # Skip if quote doesn't match filter
                    except ValueError:
                        exchange.logger().warning(f"LunoRateSource: Could not parse trading pair {trading_pair} for filtering.")
                        continue  # Skip if parsing fails

                # Extract and validate bid/ask prices
                bid_price_str = price_data.get("bid")
                ask_price_str = price_data.get("ask")

                if bid_price_str is None or ask_price_str is None:
                    exchange.logger().debug(f"LunoRateSource: Missing bid/ask for {trading_pair}. Data: {price_data}")
                    continue  # Skip if a bid or ask is missing

                try:
                    bid_price = Decimal(bid_price_str)
                    ask_price = Decimal(ask_price_str)
                    exchange.logger().info(f"LunoRateSource: {trading_pair} bid={bid_price}, ask={ask_price}")

                    # Check for valid prices (positive, bid <= ask)
                    if 0 < bid_price <= ask_price and ask_price > 0:
                        mid_price = (bid_price + ask_price) / Decimal("2")
                        exchange.logger().info(f"LunoRateSource: {trading_pair} mid_price={mid_price}")
                        results[trading_pair] = mid_price
                    else:
                        exchange.logger().warning(f"LunoRateSource: Invalid prices for {trading_pair}: bid={bid_price}, ask={ask_price}")
                except InvalidOperation as e:
                    exchange.logger().warning(f"LunoRateSource: Error converting prices for {trading_pair}: {str(e)}")
                    continue  # Skip if conversion fails

        except asyncio.CancelledError:
            raise  # Propagate cancellation
        except Exception as e:
            # Log error-specific to fetching/processing prices for this exchange
            exchange.logger().error(f"Error fetching or processing Luno prices: {str(e)}", exc_info=True)
            exchange.logger().info("LunoRateSource: Check if Luno API is accessible and returning expected data format")
            # Do not raise here, allow get_prices to handle via safe_gather

        return results

    @staticmethod
    def _build_luno_connector_without_private_keys() -> 'LunoExchange':
        """Builds a LunoExchange instance without API keys for public endpoints."""
        # Need to import locally to avoid circular dependencies
        from hummingbot.client.hummingbot_application import HummingbotApplication
        from hummingbot.connector.exchange.luno.luno_exchange import LunoExchange

        # Get logger for static method
        from hummingbot.logger import HummingbotLogger
        logger = HummingbotLogger.logger(__name__)

        logger.info("LunoRateSource: Building Luno connector without private keys")

        app = HummingbotApplication.main_application()
        # Ensure client_config_map is available
        if app is None or app.client_config_map is None:
            # This might happen in unit tests or if run outside main app context
            # Provide a default or raise an error
            from hummingbot.client.config.client_config_map import ClientConfigMap
            from hummingbot.client.config.config_helpers import ClientConfigAdapter
            client_config_map = ClientConfigAdapter(ClientConfigMap())
            logger.info("LunoRateSource: Using default client config (no app context found)")
        else:
            client_config_map = app.client_config_map
            logger.info("LunoRateSource: Using app client config")

        logger.info("LunoRateSource: Creating Luno exchange instance")
        exchange = LunoExchange(
            client_config_map=client_config_map,
            luno_api_key="",  # No API key needed for public endpoints
            luno_api_secret="",  # No secret needed
            trading_pairs=[],  # Initialize with an empty list, will fetch the map later if needed
            trading_required=False,  # Does not require trading functionality
        )
        logger.info("LunoRateSource: Luno exchange instance created successfully")
        return exchange
