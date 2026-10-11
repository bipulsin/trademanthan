"""Streamable HTTP client for the official NSE MCP servers.

Speaks MCP protocol version 2025-03-26 (JSON-RPC), which is what
``mcp.nseindia.in`` accepted on 2026-10-11. This is not a REST API.

The official ``mcp`` Python SDK was not added: it requires a newer ``httpx``
than the pinned ``httpx==0.25.2`` in ``backend/requirements.txt``. This client
uses that pinned httpx and only calls tools whose names were returned by a
live ``tools/list``.

Verified 2026-10-11 (initialize + tools/list, no auth header):

* ``https://mcp.nseindia.in/bhavcopy/cm/mcp`` — server ``nse-bhavcopy-redis-mcp``
  1.0.0, 21 tools, daily cash history.
* ``https://mcp.nseindia.in/cmmkt/mcp`` — server ``cm-market-mcp`` 1.0.0, 15
  tools, delayed cash quotes and index quotes.

``tools/list`` and ``tools/call`` came back as ``text/event-stream``. No
rate-limit headers were present on those responses. Cash segment only.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional

import httpx

# Names returned by tools/list on 2026-10-11. Anything else fails closed.
VERIFIED_TOOLS = {
    "bhavcopy": frozenset(
        {
            "get_top_by_volume",
            "get_top_movers",
            "nse_lookup_symbol",
            "get_market_mood",
            "get_index_valuation",
            "get_market_breadth",
            "get_corporate_actions",
            "compare_indices",
            "get_index_movers",
            "get_ltp_by_date",
            "get_bulk_quote",
            "get_volume_analysis",
            "get_stock_history",
            "get_index_snapshot",
            "search_symbols",
            "get_stock_vs_index",
            "compare_stocks",
            "get_index_history",
            "moving_average",
            "get_52_week_high_low",
            "get_index_performance",
        }
    ),
    "cmmkt": frozenset(
        {
            "cm_get_live_market_data",
            "cm_get_equity_stocks",
            "nse_get_losers",
            "cm_get_call_auction_stocks",
            "cm_get_bond_stocks",
            "cm_get_live_gainers",
            "nse_get_gainers",
            "cm_get_data_status",
            "cm_get_stock_quote",
            "cm_get_index_quote",
            "cm_get_sme_stocks",
            "cm_get_live_losers",
            "cm_get_live_indices",
            "nse_get_market_movers",
            "cm_get_allstocks_status",
        }
    ),
}

_PROTOCOL = "2025-03-26"


class McpCallError(Exception):
    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        super().__init__(detail or reason)


def _parse_sse_payloads(text: str) -> list:
    msgs = []
    for block in (text or "").split("\n\n"):
        data_lines = []
        for line in block.splitlines():
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
        if not data_lines:
            continue
        raw = "\n".join(data_lines).strip()
        if not raw:
            continue
        msgs.append(json.loads(raw))
    return msgs


def _response_messages(response: httpx.Response) -> list:
    ctype = (response.headers.get("content-type") or "").lower()
    if "text/event-stream" in ctype:
        return _parse_sse_payloads(response.text)
    if not response.content:
        return []
    return [response.json()]


class NseMcpClient:
    """One logical MCP session per server URL. One tools/call attempt per request."""

    def __init__(
        self,
        bhavcopy_url: Optional[str] = None,
        cmmkt_url: Optional[str] = None,
        http: Optional[httpx.Client] = None,
    ):
        from backend.config import settings

        self.urls = {
            "bhavcopy": (bhavcopy_url or settings.NSE_BHAVCOPY_MCP_URL).rstrip("/"),
            "cmmkt": (cmmkt_url or settings.NSE_CM_MARKET_MCP_URL).rstrip("/"),
        }
        self._http = http or httpx.Client(follow_redirects=True)
        self._owns_http = http is None
        self._sessions: Dict[str, str] = {}
        self._lock = threading.Lock()

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def call_tool(
        self,
        server: str,
        tool: str,
        arguments: Dict[str, Any],
        timeout: float,
    ) -> Dict[str, Any]:
        allowed = VERIFIED_TOOLS.get(server)
        if not allowed or tool not in allowed:
            raise McpCallError("unverified_tool", f"{server}/{tool}")
        url = self.urls.get(server)
        if not url:
            raise McpCallError("unverified_tool", server)
        deadline = time.monotonic() + max(float(timeout), 0.05)
        self._ensure_session(server, url, deadline)
        payload = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments or {}},
        }
        messages = self._post(url, payload, deadline, session=True)
        return _tool_payload(messages)

    def _ensure_session(self, server: str, url: str, deadline: float) -> None:
        with self._lock:
            if self._sessions.get(server):
                return
        init = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": _PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "trademanthan-nse-mcp", "version": "1"},
            },
        }
        response = self._raw_post(url, init, deadline, session_id=None)
        sid = response.headers.get("mcp-session-id")
        if response.status_code >= 400 or not sid:
            raise McpCallError("transport", f"initialize {response.status_code}")
        with self._lock:
            self._sessions[server] = sid
        # Required by the streamable HTTP lifecycle. Empty 202 is success.
        note = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        noted = self._raw_post(url, note, deadline, session_id=sid)
        if noted.status_code >= 400:
            with self._lock:
                self._sessions.pop(server, None)
            raise McpCallError("transport", f"initialized {noted.status_code}")

    def _post(self, url: str, payload: dict, deadline: float, *, session: bool) -> list:
        server = next(name for name, u in self.urls.items() if u == url)
        sid = self._sessions.get(server) if session else None
        response = self._raw_post(url, payload, deadline, session_id=sid)
        if response.status_code >= 400:
            raise McpCallError("transport", f"http {response.status_code}")
        try:
            return _response_messages(response)
        except (json.JSONDecodeError, ValueError) as exc:
            raise McpCallError("malformed", str(exc)) from exc

    def _raw_post(
        self,
        url: str,
        payload: dict,
        deadline: float,
        session_id: Optional[str],
    ) -> httpx.Response:
        left = deadline - time.monotonic()
        if left <= 0:
            raise McpCallError("timeout")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": _PROTOCOL,
        }
        if session_id:
            headers["Mcp-Session-Id"] = session_id
        try:
            return self._http.post(url, headers=headers, json=payload, timeout=left)
        except httpx.TimeoutException as exc:
            raise McpCallError("timeout", str(exc)) from exc
        except httpx.HTTPError as exc:
            raise McpCallError("transport", str(exc)) from exc


def _tool_payload(messages: list) -> Dict[str, Any]:
    if not messages:
        raise McpCallError("malformed", "empty tool response")
    msg = messages[0]
    if msg.get("error"):
        raise McpCallError("tool_error", str(msg.get("error"))[:300])
    result = msg.get("result")
    if not isinstance(result, dict):
        raise McpCallError("malformed", "missing result")
    if result.get("isError"):
        raise McpCallError("tool_error", _content_text(result)[:300])
    text = _content_text(result)
    if not text:
        raise McpCallError("malformed", "empty content")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise McpCallError("malformed", str(exc)) from exc
    if not isinstance(parsed, dict):
        raise McpCallError("malformed", "tool payload was not an object")
    return parsed


def _content_text(result: dict) -> str:
    parts = []
    for block in result.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(str(block.get("text") or ""))
    return "".join(parts).strip()
