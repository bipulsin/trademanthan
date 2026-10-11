# NSE MCP — commercial approval still outstanding

TradeWithCTO must not treat the public NSE MCP servers as a licensed market-data feed.

Checked on 11 October 2026:

- `https://www.nseindia.com/nse-mcp` returned HTTP 500, so the exchange page itself could not be read that day.
- Both MCP endpoints answered a Streamable HTTP `initialize` (protocol `2025-03-26`, no API key): `https://mcp.nseindia.in/bhavcopy/cm/mcp` and `https://mcp.nseindia.in/cmmkt/mcp`.
- The server instructions and each tool description that was listed say the data is for informational and educational use and is not investment advice. NSEIL disclaims AI-generated output.
- NSE’s Data Usage and Data Sharing Policy (circular, October 2024) says market data used for commercial purposes is provided by NSE Data & Analytics Limited at a board-approved price and on its terms. The public MCP disclaimer does not grant that right.

`NSE_MCP_ENABLED` and `NSE_MCP_PRODUCTION_APPROVED` both default to **false**. Turning a flag on is an operator switch inside this repo. It is not permission from NSE.

## Still required before any production use

Written approval from NSE Data & Analytics Limited that TradeWithCTO (a commercial trading product at tradewithcto.com) may use these MCP responses in production, including delayed cash quotes and daily bhavcopy history. Until that approval is on file, leave both flags false.

Futures, options, open interest, and intraday candles are not on the verified tool list. Those stay on Upstox even if the flags are later enabled.
