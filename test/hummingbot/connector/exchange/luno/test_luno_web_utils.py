import inspect
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.luno import luno_constants as CONSTANTS, luno_web_utils as web_utils


class LunoWebUtilsTests(unittest.TestCase):

    def test_public_rest_url_ignores_domain(self):
        url = web_utils.public_rest_url(path_url="foo", domain="us")
        self.assertEqual(f"{CONSTANTS.REST_URL}foo", url)

    def test_private_rest_url_ignores_domain(self):
        url = web_utils.private_rest_url(path_url="bar", domain="us")
        self.assertEqual(f"{CONSTANTS.REST_URL}bar", url)

    def test_public_rest_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_URL)
        expected = f"{CONSTANTS.REST_URL}{CONSTANTS.TICKER_URL}"
        self.assertEqual(expected, url)

    def test_private_rest_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNTS_URL)
        expected = f"{CONSTANTS.REST_URL}{CONSTANTS.ACCOUNTS_URL}"
        self.assertEqual(expected, url)

    def test_build_api_factory(self):
        throttler = MagicMock()
        auth = MagicMock()
        time_provider = MagicMock()

        api_factory = web_utils.build_api_factory(
            throttler=throttler,
            auth=auth,
            time_provider=time_provider
        )

        self.assertEqual(throttler, api_factory._throttler)
        self.assertEqual(auth, api_factory._auth)
        self.assertEqual(1, len(api_factory._rest_pre_processors))

    def test_build_api_factory_defaults(self):
        api_factory = web_utils.build_api_factory()

        from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
        self.assertIsInstance(api_factory._throttler, AsyncThrottler)
        self.assertIsNone(api_factory._auth)

        from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
        pps = api_factory._rest_pre_processors
        self.assertEqual(1, len(pps))
        self.assertIsInstance(pps[0], TimeSynchronizerRESTPreProcessor)
        self.assertTrue(inspect.iscoroutine(pps[0]._time_provider()))

    def test_create_throttler(self):
        throttler = web_utils.create_throttler()
        self.assertEqual(str(CONSTANTS.RATE_LIMITS), str(throttler._rate_limits))

    def test_build_api_factory_without_time_synchronizer_pre_processor(self):
        throttler = MagicMock()
        api_factory = web_utils.build_api_factory_without_time_synchronizer_pre_processor(throttler)
        self.assertEqual(throttler, api_factory._throttler)
        self.assertEqual(0, len(api_factory._rest_pre_processors))


class LunoWebUtilsAsyncTests(unittest.IsolatedAsyncioTestCase):

    async def test_get_current_server_time_happy_path(self):
        mock_rest = AsyncMock()
        mock_rest.execute_request.return_value = {"timestamp": 1234}

        with patch(
                "hummingbot.connector.exchange.luno.luno_web_utils.build_api_factory_without_time_synchronizer_pre_processor",
                return_value=MagicMock(get_rest_assistant=AsyncMock(return_value=mock_rest))
        ):
            result = await web_utils.get_current_server_time()
            self.assertEqual(1234, result)

    async def test_get_current_server_time_uses_provided_throttler(self):
        custom_throttler = MagicMock()
        mock_rest = AsyncMock()
        mock_rest.execute_request.return_value = {"timestamp": 42}

        with patch(
                "hummingbot.connector.exchange.luno.luno_web_utils.build_api_factory_without_time_synchronizer_pre_processor"
        ) as mock_build:
            mock_build.return_value = MagicMock(get_rest_assistant=AsyncMock(return_value=mock_rest))
            result = await web_utils.get_current_server_time(throttler=custom_throttler)
            self.assertEqual(42, result)
            mock_build.assert_called_once_with(throttler=custom_throttler)

    async def test_get_current_server_time_propagates_exception(self):
        mock_rest = AsyncMock()
        mock_rest.execute_request.side_effect = KeyError("no timestamp")
        with patch(
                "hummingbot.connector.exchange.luno.luno_web_utils.build_api_factory_without_time_synchronizer_pre_processor",
                return_value=MagicMock(get_rest_assistant=AsyncMock(return_value=mock_rest))
        ):
            with self.assertRaises(KeyError):
                await web_utils.get_current_server_time()
