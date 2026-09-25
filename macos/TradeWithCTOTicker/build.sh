#!/bin/sh
set -eu
ROOT="$(cd "$(dirname "$0")" && pwd)"
APP="$ROOT/TradeWithCTOTicker.app"
mkdir -p "$APP/Contents/MacOS"
swiftc -O -target "$(uname -m)-apple-macos12.0" \
  -framework Cocoa -framework Security \
  -o "$APP/Contents/MacOS/TradeWithCTOTicker" \
  "$ROOT/main.swift"
cat > "$APP/Contents/Info.plist" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key><string>TradeWithCTOTicker</string>
  <key>CFBundleIdentifier</key><string>com.tradewithcto.ticker</string>
  <key>CFBundleName</key><string>TradeWithCTO Ticker</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>1.0</string>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>LSUIElement</key><true/>
</dict>
</plist>
EOF
echo "Built $APP"
