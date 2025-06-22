"""
Low-level WebSocket client for Upstox Market Data Feed V3
Handles connection, authentication, subscription, and message parsing (protobuf decoding to be added).
"""
import threading
import websocket
import requests
import json
import logging
from typing import Callable, List, Dict, Any, Optional
from . import MarketDataFeedV3_pb2 as pb

class UpstoxWebSocket:
    WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"

    def __init__(self, access_token: str, on_message: Optional[Callable] = None, on_error: Optional[Callable] = None, on_close: Optional[Callable] = None, on_open: Optional[Callable] = None):
        self.access_token = access_token
        self.ws = None
        self.connected = False
        self.logger = logging.getLogger("upstox_websocket")
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        self.on_open = on_open
        self._thread = None
        self._stop_event = threading.Event()

    def connect(self):
        self.logger.debug(f"Connecting to Upstox WebSocket at {self.WS_URL} with token: {self.access_token[:6]}... (truncated)")
        headers = [
            f"Authorization: Bearer {self.access_token}",
            "Accept: */*"
        ]
        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            header=headers,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open
        )
        self._thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self._thread.start()
        # Wait for connection
        for _ in range(30):
            if self.connected:
                return True
            self._stop_event.wait(0.1)
        self.logger.debug("WebSocket connection attempt timed out.")
        return False

    def disconnect(self):
        self.logger.debug("Disconnecting Upstox WebSocket.")
        self._stop_event.set()
        if self.ws:
            self.ws.close()
        self.connected = False

    def _on_open(self, ws):
        self.connected = True
        self.logger.info("WebSocket connection opened.")
        if self.on_open:
            self.on_open(ws)

    def _on_message(self, ws, message):
        self.logger.debug(f"Raw message from broker: {message}")
        # Protobuf decoding for binary messages
        try:
            if isinstance(message, bytes):
                self.logger.debug(f"Received binary message of length {len(message)} bytes.")
                feed_response = pb.FeedResponse()
                feed_response.ParseFromString(message)
                msg_dict = self._protobuf_to_dict(feed_response)
                if self.on_message:
                    self.on_message(ws, msg_dict)
            else:
                self.logger.debug(f"Received text message: {message}")
                if self.on_message:
                    self.on_message(ws, message)
        except Exception as e:
            self.logger.error(f"Failed to decode protobuf message: {e}")
            if self.on_error:
                self.on_error(ws, e)

    def _protobuf_to_dict(self, proto_msg):
        # Recursively convert protobuf message to dict
        from google.protobuf.json_format import MessageToDict
        return MessageToDict(proto_msg, preserving_proto_field_name=True)

    def _on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")
        if self.on_error:
            self.on_error(ws, error)

    def _on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        self.logger.info(f"WebSocket closed: {close_status_code} {close_msg}")
        if self.on_close:
            self.on_close(ws, close_status_code, close_msg)

    def send_binary(self, data: bytes):
        if self.ws and self.connected:
            self.ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)

    def subscribe(self, guid: str, mode: str, instrument_keys: List[str]):
        self.logger.debug(f"Subscribing to: {instrument_keys} with mode: {mode} and guid: {guid}")
        req = {
            "guid": guid,
            "method": "sub",
            "data": {
                "mode": mode,
                "instrumentKeys": instrument_keys
            }
        }
        self.ws.send(json.dumps(req))

    def unsubscribe(self, guid: str, instrument_keys: List[str]):
        self.logger.debug(f"Unsubscribing from: {instrument_keys} with guid: {guid}")
        req = {
            "guid": guid,
            "method": "unsub",
            "data": {
                "instrumentKeys": instrument_keys
            }
        }
        self.ws.send(json.dumps(req))
