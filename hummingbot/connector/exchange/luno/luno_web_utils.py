from typing import Callable, Optional

import hummingbot.connector.exchange.luno.luno_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: the endpoint path
    :param domain: ignored for Luno (included for compatibility)
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :param domain: the Binance domain to connect to ("com" or "us"). The default value is "com"
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + path_url


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        auth: Optional[AuthBase] = None,
        time_provider: Optional[Callable] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> WebAssistantsFactory:
    """
    Creates a web assistant factory configured with the provided throttler and auth
    :param throttler: the throttler to use for rate limiting
    :param auth: the authentication implementation
    :param time_provider: a function that returns the current server time in milliseconds
    :param domain: ignored for Luno (included for compatibility)
    :return: a fully configured web assistant factory
    """
    throttler = throttler or create_throttler()
    time_provider = time_provider or (lambda: get_current_server_time(
        throttler=throttler,
        domain=domain,
    ))
    from hummingbot.connector.time_synchronizer import TimeSynchronizer
    from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
    time_synchronizer = TimeSynchronizer()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ]
    )
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Creates a throttler for Luno API
    :return: an AsyncThrottler configured with Luno's rate limits
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    """
    Creates a web assistant factory without time synchronizer pre-processor
    :param throttler: the throttler to use for rate limiting
    :return: a web assistant factory without time synchronizer pre-processor
    """
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Gets the current server time from Luno
    :param throttler: the throttler to use for rate limiting
    :param domain: ignored for Luno (included for compatibility)
    :return: the current server time in milliseconds
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    # The orderbook_top API requires a trading pair parameter
    # Using XBTZAR as a common trading pair
    params = {"pair": "XBTZAR"}
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.ORDERBOOK_TOP_URL),
        method=RESTMethod.GET,
        params=params,
        throttler_limit_id=CONSTANTS.ORDERBOOK_TOP_URL,
    )
    server_time = response["timestamp"]
    return server_time
