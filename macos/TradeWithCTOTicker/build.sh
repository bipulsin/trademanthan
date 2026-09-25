#!/bin/sh
set -eu
ROOT="$(cd "$(dirname "$0")" && pwd)"
APP="$ROOT/TradeWithCTOTicker.app"
LOGO="$ROOT/../../frontend/public/tradewithcto-logo.png"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
swiftc -O -target "$(uname -m)-apple-macos12.0" \
  -framework Cocoa -framework Security \
  -o "$APP/Contents/MacOS/TradeWithCTOTicker" \
  "$ROOT/main.swift"

ICONSET="$(mktemp -d "${TMPDIR:-/tmp}/twcto-icon.XXXXXX")"
ICONSET="${ICONSET}.iconset"
mkdir -p "$ICONSET"
python3 - "$LOGO" "$ICONSET" "$APP/Contents/Resources/MenuBarIcon.png" <<'PY'
import sys
from PIL import Image

logo = Image.open(sys.argv[1]).convert("RGBA")
iconset = sys.argv[2]
menu_path = sys.argv[3]

def square(size):
    canvas = Image.new("RGBA", (size, size), (0, 0, 0, 255))
    max_w = int(size * 0.86)
    max_h = int(size * 0.86)
    scale = min(max_w / logo.width, max_h / logo.height)
    fitted = logo.resize((max(1, int(logo.width * scale)), max(1, int(logo.height * scale))), Image.LANCZOS)
    canvas.paste(fitted, ((size - fitted.width) // 2, (size - fitted.height) // 2), fitted)
    return canvas

sizes = {
    "icon_16x16.png": 16,
    "icon_16x16@2x.png": 32,
    "icon_32x32.png": 32,
    "icon_32x32@2x.png": 64,
    "icon_128x128.png": 128,
    "icon_128x128@2x.png": 256,
    "icon_256x256.png": 256,
    "icon_256x256@2x.png": 512,
    "icon_512x512.png": 512,
    "icon_512x512@2x.png": 1024,
}
for name, size in sizes.items():
    square(size).save(f"{iconset}/{name}")

menu_h = 36
menu_w = max(1, int(round(menu_h * logo.width / logo.height)))
logo.resize((menu_w, menu_h), Image.LANCZOS).save(menu_path)
PY
iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/AppIcon.icns"
rm -rf "$ICONSET"

cat > "$APP/Contents/Info.plist" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key><string>TradeWithCTOTicker</string>
  <key>CFBundleIconFile</key><string>AppIcon</string>
  <key>CFBundleIdentifier</key><string>com.tradewithcto.ticker</string>
  <key>CFBundleName</key><string>TradeWithCTO Ticker</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>1.2</string>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
EOF
echo "Built $APP"
