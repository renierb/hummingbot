import base64
from typing import Dict

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class LunoAuth(AuthBase):
    """
    Auth class required by Hummingbot's web assistant for Luno API authentication
    """

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the auth credentials to the request using HTTP Basic Authentication
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.generate_auth_dict())
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Prepares a websocket request for authentication. For Luno, authentication is done
        separately by sending credentials in the first message after connection.
        """
        return request  # Pass-through, WebSocket auth is done in the first message

    def generate_auth_dict(self) -> Dict[str, str]:
        """
        Generates the HTTP Basic Authentication header
        """
        auth_str = f"{self.api_key}:{self.secret_key}"
        encoded = base64.b64encode(auth_str.encode("utf8")).decode("utf8")
        return {"Authorization": f"Basic {encoded}"}
