import AppKit
import Foundation
import Security

let service = "com.tradewithcto.ticker"
let defaultBase = "https://www.tradewithcto.com"
let minW: CGFloat = 300
let minH: CGFloat = 180
let maxH: CGFloat = 560

final class TickerApp: NSObject, NSApplicationDelegate, NSWindowDelegate {
    let status = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
    let panel = NSPanel(
        contentRect: NSRect(x: 0, y: 0, width: minW, height: minH),
        styleMask: [.titled, .closable, .resizable, .nonactivatingPanel, .utilityWindow],
        backing: .buffered,
        defer: false
    )
    let scroll = NSScrollView()
    let stack = NSStackView()
    var timer: Timer?
    var lastOrigin: NSPoint?

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.accessory)
        if let button = status.button {
            button.image = NSImage(systemSymbolName: "chart.line.uptrend.xyaxis", accessibilityDescription: "TradeWithCTO")
            button.image?.isTemplate = true
        }
        status.menu = makeMenu()
        configurePanel()
        placePanel(remembered: true)
        panel.orderFrontRegardless()
        reload()
        timer = Timer.scheduledTimer(withTimeInterval: 1.0, repeats: true) { [weak self] _ in
            self?.reload()
        }
    }

    func makeMenu() -> NSMenu {
        let menu = NSMenu()
        let show = NSMenuItem(title: "Show ticker", action: #selector(showPanel), keyEquivalent: "")
        let token = NSMenuItem(title: "Set server and token…", action: #selector(editToken), keyEquivalent: "")
        let quit = NSMenuItem(title: "Quit", action: #selector(quit), keyEquivalent: "q")
        for item in [show, token, quit] { item.target = self }
        menu.addItem(show)
        menu.addItem(token)
        menu.addItem(NSMenuItem.separator())
        menu.addItem(quit)
        return menu
    }

    func configurePanel() {
        panel.title = "TradeWithCTO.com"
        panel.level = .floating
        panel.collectionBehavior = [.canJoinAllSpaces, .fullScreenAuxiliary]
        panel.hidesOnDeactivate = false
        panel.isFloatingPanel = true
        panel.becomesKeyOnlyIfNeeded = true
        panel.isMovableByWindowBackground = true
        panel.backgroundColor = NSColor(calibratedRed: 0.07, green: 0.09, blue: 0.08, alpha: 1)
        panel.minSize = NSSize(width: minW, height: minH)
        panel.maxSize = NSSize(width: 460, height: maxH)
        panel.delegate = self
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        scroll.autohidesScrollers = true
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 6
        stack.edgeInsets = NSEdgeInsets(top: 10, left: 12, bottom: 12, right: 12)
        scroll.documentView = stack
        panel.contentView = scroll
    }

    func placePanel(remembered: Bool) {
        let frame = NSScreen.main?.visibleFrame ?? NSRect(x: 0, y: 0, width: 1280, height: 800)
        var origin = NSPoint(x: frame.maxX - panel.frame.width - 16, y: frame.minY + 16)
        if remembered, let s = UserDefaults.standard.string(forKey: "ticker.origin") {
            let bits = s.split(separator: ",").compactMap { Double($0) }
            if bits.count == 2 {
                origin = NSPoint(x: bits[0], y: bits[1])
            }
        }
        panel.setFrameOrigin(origin)
    }

    func windowDidMove(_ notification: Notification) {
        let p = panel.frame.origin
        UserDefaults.standard.set("\(p.x),\(p.y)", forKey: "ticker.origin")
    }

    @objc func showPanel() { panel.orderFrontRegardless() }
    @objc func quit() { NSApp.terminate(nil) }

    @objc func editToken() {
        let alert = NSAlert()
        alert.messageText = "TradeWithCTO ticker"
        alert.informativeText = "Server URL and the app token from Settings → Live ticker."
        alert.addButton(withTitle: "Save")
        alert.addButton(withTitle: "Cancel")
        let box = NSStackView()
        box.orientation = .vertical
        let url = NSTextField(string: UserDefaults.standard.string(forKey: "ticker.base") ?? defaultBase)
        url.placeholderString = defaultBase
        let token = NSTextField(string: loadToken())
        token.placeholderString = "twt_…"
        url.frame.size.width = 320
        token.frame.size.width = 320
        box.addArrangedSubview(url)
        box.addArrangedSubview(token)
        alert.accessoryView = box
        NSApp.activate(ignoringOtherApps: true)
        if alert.runModal() == .alertFirstButtonReturn {
            UserDefaults.standard.set(url.stringValue.trimmingCharacters(in: .whitespaces), forKey: "ticker.base")
            saveToken(token.stringValue.trimmingCharacters(in: .whitespaces))
            reload()
        }
    }

    func baseURL() -> String {
        let raw = (UserDefaults.standard.string(forKey: "ticker.base") ?? defaultBase)
            .trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        return raw.isEmpty ? defaultBase : raw
    }

    func saveToken(_ token: String) {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: "token"
        ]
        SecItemDelete(query as CFDictionary)
        var add = query
        add[kSecValueData as String] = Data(token.utf8)
        SecItemAdd(add as CFDictionary, nil)
    }

    func loadToken() -> String {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: "token",
            kSecReturnData as String: true
        ]
        var item: CFTypeRef?
        guard SecItemCopyMatching(query as CFDictionary, &item) == errSecSuccess,
              let data = item as? Data,
              let s = String(data: data, encoding: .utf8) else { return "" }
        return s
    }

    func reload() {
        let token = loadToken()
        guard !token.isEmpty else {
            render(message: "Set the app token from Settings.", trades: [], signals: [])
            return
        }
        guard let url = URL(string: baseURL() + "/api/ticker/live") else { return }
        var req = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 8)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        URLSession.shared.dataTask(with: req) { [weak self] data, response, _ in
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            DispatchQueue.main.async {
                guard let self = self else { return }
                if code == 401 {
                    self.render(message: "Token was rejected. Create a new one in Settings.", trades: [], signals: [])
                    return
                }
                guard let data = data,
                      let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                    self.render(message: "Waiting for TradeWithCTO…", trades: [], signals: [])
                    return
                }
                if json["enabled"] as? Bool == false {
                    self.render(message: "Ticker is off in Settings.", trades: [], signals: [])
                    return
                }
                let trades = json["trades"] as? [[String: Any]] ?? []
                let signals = json["signals"] as? [[String: Any]] ?? []
                self.render(message: nil, trades: trades, signals: signals)
            }
        }.resume()
    }

    func render(message: String?, trades: [[String: Any]], signals: [[String: Any]]) {
        stack.arrangedSubviews.forEach { $0.removeFromSuperview() }
        stack.addArrangedSubview(heading("Live trades"))
        if let message = message {
            stack.addArrangedSubview(line(message, color: NSColor(calibratedWhite: 0.75, alpha: 1)))
        } else if trades.isEmpty {
            stack.addArrangedSubview(line("None", color: NSColor(calibratedWhite: 0.55, alpha: 1)))
        } else {
            trades.forEach { stack.addArrangedSubview(tradeRow($0)) }
        }
        stack.addArrangedSubview(heading("Active signals"))
        if message != nil {
            stack.addArrangedSubview(line("—", color: NSColor(calibratedWhite: 0.45, alpha: 1)))
        } else if signals.isEmpty {
            stack.addArrangedSubview(line("None", color: NSColor(calibratedWhite: 0.55, alpha: 1)))
        } else {
            signals.forEach { stack.addArrangedSubview(signalRow($0)) }
        }
        stack.layoutSubtreeIfNeeded()
        let height = min(maxH, max(minH, stack.fittingSize.height + 28))
        var frame = panel.frame
        let bottom = frame.minY
        frame.size.height = height
        frame.size.width = max(minW, frame.width)
        frame.origin.y = bottom
        panel.setFrame(frame, display: true)
    }

    func heading(_ text: String) -> NSTextField {
        let label = NSTextField(labelWithString: text.uppercased())
        label.font = NSFont.systemFont(ofSize: 10, weight: .semibold)
        label.textColor = NSColor(calibratedRed: 0.45, green: 0.78, blue: 0.55, alpha: 1)
        return label
    }

    func line(_ text: String, color: NSColor) -> NSTextField {
        let label = NSTextField(labelWithString: text)
        label.font = NSFont.systemFont(ofSize: 12)
        label.textColor = color
        label.lineBreakMode = .byTruncatingTail
        label.preferredMaxLayoutWidth = 270
        return label
    }

    func tradeRow(_ row: [String: Any]) -> NSView {
        let sym = "\(row["algo_label"] ?? "")  \(row["symbol"] ?? "")"
        let pnl = row["pnl"] as? Double
        let value: String
        let color: NSColor
        if let pnl = pnl {
            value = (pnl >= 0 ? "+" : "") + String(format: "₹%.0f", pnl)
            color = pnl >= 0
                ? NSColor(calibratedRed: 0.45, green: 0.86, blue: 0.55, alpha: 1)
                : NSColor(calibratedRed: 0.93, green: 0.45, blue: 0.42, alpha: 1)
        } else {
            value = "—"
            color = NSColor(calibratedWhite: 0.7, alpha: 1)
        }
        return pair(sym, value, color)
    }

    func signalRow(_ row: [String: Any]) -> NSView {
        let left = "\(row["algo_label"] ?? "")  \(row["symbol"] ?? "")"
        let right = "\(row["label"] ?? "")"
        return pair(left, right, NSColor(calibratedWhite: 0.82, alpha: 1))
    }

    func pair(_ left: String, _ right: String, _ color: NSColor) -> NSView {
        let row = NSStackView()
        row.orientation = .horizontal
        row.spacing = 8
        let a = line(left, color: NSColor(calibratedWhite: 0.9, alpha: 1))
        a.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let b = line(right, color: color)
        b.alignment = .right
        b.setContentHuggingPriority(.required, for: .horizontal)
        row.addArrangedSubview(a)
        row.addArrangedSubview(b)
        row.widthAnchor.constraint(equalToConstant: 276).isActive = true
        return row
    }
}

let app = NSApplication.shared
let delegate = TickerApp()
app.delegate = delegate
app.run()
