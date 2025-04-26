import base64
import unittest
from unittest.mock import MagicMock

from hummingbot.connector.exchange.luno.luno_auth import LunoAuth
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class LunoAuthSyncTests(unittest.TestCase):
    def setUp(self):
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"
        self.auth = LunoAuth(api_key=self.api_key, secret_key=self.secret_key)

    def test_generate_auth_dict_returns_basic_auth_header(self):
        auth_dict = self.auth.generate_auth_dict()
        self.assertIn("Authorization", auth_dict)
        self.assertTrue(auth_dict["Authorization"].startswith("Basic "))

    def test_generate_auth_dict_exact_value(self):
        raw = f"{self.api_key}:{self.secret_key}"
        token = self.auth.generate_auth_dict()["Authorization"].split(" ", 1)[1]
        decoded = base64.b64decode(token).decode()
        self.assertEqual(decoded, raw)

    def test_generate_auth_dict_with_special_characters(self):
        special_key = "key:with:colon"
        special_secret = "sec/ret?*"
        auth = LunoAuth(api_key=special_key, secret_key=special_secret)
        token = auth.generate_auth_dict()["Authorization"].split(" ", 1)[1]
        decoded = base64.b64decode(token).decode()
        self.assertEqual(decoded, f"{special_key}:{special_secret}")


class LunoAuthAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.auth = LunoAuth(api_key="testApiKey", secret_key="testSecretKey")

    async def test_rest_authenticate_adds_auth_header(self):
        request = RESTRequest(method="GET", url="https://test.url", headers={}, data="{}")
        authenticated = await self.auth.rest_authenticate(request)
        self.assertIs(authenticated, request)
        self.assertIn("Authorization", authenticated.headers)

    async def test_rest_authenticate_preserves_existing_headers_and_none(self):
        # Existing headers
        req1 = RESTRequest(method="GET", url="https://x", headers={"X": "Y"}, data=None)
        auth1 = await self.auth.rest_authenticate(req1)
        self.assertIn("X", auth1.headers)
        # None headers
        req2 = RESTRequest(method="GET", url="https://x", headers=None, data=None)
        auth2 = await self.auth.rest_authenticate(req2)
        self.assertIsInstance(auth2.headers, dict)
        self.assertIn("Authorization", auth2.headers)

    async def test_ws_authenticate_passes_through(self):
        request = MagicMock(spec=WSRequest)
        request.payload = {"test": "payload"}
        authenticated = await self.auth.ws_authenticate(request)
        self.assertIs(authenticated, request)
