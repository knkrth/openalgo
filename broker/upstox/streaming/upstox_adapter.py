"""
Upstox WebSocket adapter for OpenAlgo WebSocket proxy
"""
import logging
import threading
import sys
import os
import uuid
from typing import Dict, Any, Optional, List

from websocket_proxy.base_adapter import BaseBrokerWebSocketAdapter
from websocket_proxy.mapping import SymbolMapper
from .upstox_mapping import UpstoxExchangeMapper, UpstoxCapabilityRegistry
from .upstox_websocket import UpstoxWebSocket
from database.auth_db import get_auth_token

class UpstoxWebSocketAdapter(BaseBrokerWebSocketAdapter):
    """Upstox-specific implementation of the WebSocket adapter"""
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("upstox_websocket_adapter")
        self.ws_client = None
        self.user_id = None
        self.broker_name = "upstox"
        self.running = False
        self.lock = threading.Lock()
        self.access_token = None
        self.subscriptions = {}  # {(symbol, exchange, mode): True}

    def initialize(self, broker_name: str, user_id: str = None, auth_data: Optional[Dict[str, str]] = None) -> None:
        self.broker_name = broker_name
        self.user_id = user_id
        token = get_auth_token(user_id) if user_id else None
        if token:
            self.access_token = token
        else:
            self.logger.warning(f"No Upstox access token found for user_id '{user_id}' in DB.")

    def connect(self) -> None:
        if not self.access_token:
            raise Exception("Upstox access_token required for WebSocket connection.")
        self.ws_client = UpstoxWebSocket(
            access_token=self.access_token,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open
        )
        self.ws_client.connect()
        self.running = True

    def disconnect(self) -> None:
        if self.ws_client:
            self.ws_client.disconnect()
        self.running = False

    def subscribe(self, symbol: str, exchange: str, mode: str = "ltpc", depth_level: int = 5) -> Dict[str, Any]:
        guid = str(uuid.uuid4())
        instrument_key = f"{UpstoxExchangeMapper.to_upstox_exchange(exchange)}|{symbol}"
        self.logger.debug(f"Adapter subscribing: symbol={symbol}, exchange={exchange}, mode={mode}, guid={guid}")
        self.ws_client.subscribe(guid, mode, [instrument_key])
        self.subscriptions[(symbol, exchange, mode)] = True
        return {"status": "subscribed", "symbol": symbol, "exchange": exchange, "mode": mode}

    def unsubscribe(self, symbol: str, exchange: str, mode: str = "ltpc") -> Dict[str, Any]:
        guid = str(uuid.uuid4())
        instrument_key = f"{UpstoxExchangeMapper.to_upstox_exchange(exchange)}|{symbol}"
        self.logger.debug(f"Adapter unsubscribing: symbol={symbol}, exchange={exchange}, mode={mode}, guid={guid}")
        self.ws_client.unsubscribe(guid, [instrument_key])
        self.subscriptions.pop((symbol, exchange, mode), None)
        return {"status": "unsubscribed", "symbol": symbol, "exchange": exchange, "mode": mode}

    def _on_open(self, ws):
        self.logger.info("Upstox WebSocket connection opened.")

    def _on_message(self, ws, message):
        self.logger.debug(f"Adapter received message: {message}")
        self.logger.info(f"Received message: {message}")
        normalized = self._normalize_market_data(message)
        # self.publish_market_data(normalized)  # Uncomment if publish method exists

    def _normalize_market_data(self, message: dict) -> dict:
        # Placeholder: implement normalization as per OpenAlgo format
        # For now, just return the message
        return message

    def _on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        self.logger.info(f"WebSocket closed: {close_status_code} {close_msg}")
