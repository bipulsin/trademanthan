import AppKit
import Foundation
import Security

let service = "com.tradewithcto.ticker"
let defaultBase = "https://www.tradewithcto.com"
let pollSeconds: TimeInterval = 120
let minW: CGFloat = 300
let defaultLines: CGFloat = 7
let maxLines: CGFloat = 20
let liveYellow = NSColor(calibratedRed: 1, green: 0.93, blue: 0.05, alpha: 1)

final class TopClipView: NSClipView {
    override func constrainBoundsRect(_ proposedBounds: NSRect) -> NSRect {
        var rect = super.constrainBoundsRect(proposedBounds)
        guard let doc = documentView, doc.frame.height < rect.height else { return rect }
        rect.origin.y = doc.frame.height - rect.height
        return rect
    }
}

final class PanelTextField: NSTextField {
    override func mouseDown(with event: NSEvent) {
        window?.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        super.mouseDown(with: event)
    }

    override func becomeFirstResponder() -> Bool {
        window?.makeKeyAndOrderFront(nil)
        return super.becomeFirstResponder()
    }
}

final class TickerApp: NSObject, NSApplicationDelegate, NSWindowDelegate {
    let status = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
    let panel = NSPanel(
        contentRect: NSRect(x: 0, y: 0, width: minW, height: 220),
        styleMask: [.titled, .closable, .resizable, .nonactivatingPanel, .utilityWindow],
        backing: .buffered,
        defer: false
    )
    let form = NSStackView()
    let urlField = PanelTextField(string: "")
    let tokenField = PanelTextField(string: "")
    let scroll = NSScrollView()
    let stack = NSStackView()
    let setupFooter = NSView()
    var changeButton: NSButton!
    var timer: Timer?
    var lastOrigin: NSPoint?
    var loadingLive = false
    var setupPinnedOpen = false

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        applyStatusIcon()
        applyDockIcon()
        status.menu = makeMenu()
        configurePanel()
        placePanel(remembered: loadToken().isEmpty ? false : true)
        panel.orderFrontRegardless()
        NSApp.activate(ignoringOtherApps: true)
        reload()
        timer = Timer.scheduledTimer(withTimeInterval: pollSeconds, repeats: true) { [weak self] _ in
            self?.reload()
        }
    }

    func applyStatusIcon() {
        guard let button = status.button else { return }
        if let url = Bundle.main.url(forResource: "MenuBarIcon", withExtension: "png"),
           let image = NSImage(contentsOf: url) {
            image.size = NSSize(width: 28, height: 18)
            image.isTemplate = false
            button.image = image
            button.imagePosition = .imageOnly
            button.title = ""
        } else {
            button.title = "TWCTO"
            button.image = NSImage(systemSymbolName: "chart.line.uptrend.xyaxis", accessibilityDescription: "TradeWithCTO")
            button.imagePosition = .imageLeading
        }
    }

    func applyDockIcon() {
        if let url = Bundle.main.url(forResource: "AppIcon", withExtension: "icns"),
           let image = NSImage(contentsOf: url) {
            NSApp.applicationIconImage = image
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
        panel.appearance = NSAppearance(named: .darkAqua)
        panel.minSize = NSSize(width: minW, height: 180)
        panel.maxSize = NSSize(width: 460, height: 900)
        panel.delegate = self

        form.orientation = .vertical
        form.alignment = .leading
        form.spacing = 6
        form.edgeInsets = NSEdgeInsets(top: 10, left: 12, bottom: 8, right: 12)
        styleField(urlField, placeholder: defaultBase)
        styleField(tokenField, placeholder: "twt_…")
        urlField.stringValue = UserDefaults.standard.string(forKey: "ticker.base") ?? defaultBase
        tokenField.stringValue = loadToken()
        let save = NSButton(title: "Save", target: self, action: #selector(saveFromPanel))
        save.bezelStyle = .rounded
        save.contentTintColor = NSColor(calibratedRed: 0.45, green: 0.86, blue: 0.55, alpha: 1)
        save.keyEquivalent = "\r"
        form.addArrangedSubview(heading("Server URL"))
        form.addArrangedSubview(urlField)
        form.addArrangedSubview(heading("Token"))
        form.addArrangedSubview(tokenField)
        form.addArrangedSubview(save)

        let clip = TopClipView()
        clip.drawsBackground = false
        scroll.contentView = clip
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        scroll.autohidesScrollers = true
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 6
        stack.edgeInsets = NSEdgeInsets(top: 8, left: 12, bottom: 12, right: 12)
        scroll.documentView = stack

        changeButton = NSButton(title: "Change server or token", target: self, action: #selector(revealSetup))
        changeButton.bezelStyle = .inline
        changeButton.font = NSFont.systemFont(ofSize: 11)
        changeButton.contentTintColor = NSColor(calibratedWhite: 0.62, alpha: 1)
        changeButton.translatesAutoresizingMaskIntoConstraints = false
        setupFooter.addSubview(changeButton)
        NSLayoutConstraint.activate([
            changeButton.topAnchor.constraint(equalTo: setupFooter.topAnchor, constant: 2),
            changeButton.bottomAnchor.constraint(equalTo: setupFooter.bottomAnchor, constant: -6),
            changeButton.leadingAnchor.constraint(equalTo: setupFooter.leadingAnchor, constant: 10)
        ])

        let root = NSStackView()
        root.orientation = .vertical
        root.alignment = .leading
        root.spacing = 0
        root.detachesHiddenViews = true
        root.addArrangedSubview(form)
        root.addArrangedSubview(scroll)
        root.addArrangedSubview(setupFooter)
        form.translatesAutoresizingMaskIntoConstraints = false
        scroll.translatesAutoresizingMaskIntoConstraints = false
        setupFooter.translatesAutoresizingMaskIntoConstraints = false
        form.setContentHuggingPriority(.required, for: .vertical)
        setupFooter.setContentHuggingPriority(.required, for: .vertical)
        scroll.setContentHuggingPriority(NSLayoutConstraint.Priority(1), for: .vertical)
        scroll.setContentCompressionResistancePriority(NSLayoutConstraint.Priority(1), for: .vertical)
        NSLayoutConstraint.activate([
            form.widthAnchor.constraint(equalTo: root.widthAnchor),
            scroll.widthAnchor.constraint(equalTo: root.widthAnchor),
            setupFooter.widthAnchor.constraint(equalTo: root.widthAnchor)
        ])
        panel.contentView = root
        setupPinnedOpen = loadToken().isEmpty
        applySetupVisibility()
        applyWindowHeight(contentHeight: listHeight(lines: defaultLines))
    }

    func styleField(_ field: NSTextField, placeholder: String) {
        field.font = NSFont.systemFont(ofSize: 12)
        field.textColor = NSColor(calibratedWhite: 0.96, alpha: 1)
        field.backgroundColor = NSColor(calibratedRed: 0.13, green: 0.16, blue: 0.14, alpha: 1)
        field.drawsBackground = true
        field.isBezeled = true
        field.bezelStyle = .roundedBezel
        field.focusRingType = .exterior
        field.maximumNumberOfLines = 1
        field.placeholderAttributedString = NSAttributedString(
            string: placeholder,
            attributes: [
                .foregroundColor: NSColor(calibratedWhite: 0.45, alpha: 1),
                .font: NSFont.systemFont(ofSize: 12)
            ]
        )
        if let cell = field.cell as? NSTextFieldCell {
            cell.wraps = false
            cell.isScrollable = true
            cell.usesSingleLineMode = true
        }
        field.translatesAutoresizingMaskIntoConstraints = false
        field.widthAnchor.constraint(equalToConstant: 276).isActive = true
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

    @objc func revealSetup() {
        setupPinnedOpen = true
        applySetupVisibility()
        panel.makeKeyAndOrderFront(nil)
        tokenField.window?.makeFirstResponder(tokenField)
    }

    func applySetupVisibility() {
        let showForm = setupPinnedOpen || loadToken().isEmpty
        form.isHidden = !showForm
        setupFooter.isHidden = showForm
    }

    @objc func saveFromPanel() {
        persist(url: urlField.stringValue, token: tokenField.stringValue)
    }

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
        panel.makeKeyAndOrderFront(nil)
        if alert.runModal() == .alertFirstButtonReturn {
            persist(url: url.stringValue, token: token.stringValue)
        }
    }

    func persist(url rawURL: String, token rawToken: String) {
        let url = rawURL.trimmingCharacters(in: .whitespaces)
        let token = rawToken.trimmingCharacters(in: .whitespaces)
        UserDefaults.standard.set(url.isEmpty ? defaultBase : url, forKey: "ticker.base")
        saveToken(token)
        urlField.stringValue = UserDefaults.standard.string(forKey: "ticker.base") ?? defaultBase
        tokenField.stringValue = token
        if !token.isEmpty {
            setupPinnedOpen = false
        }
        applySetupVisibility()
        reload()
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
        if loadingLive { return }
        let token = loadToken()
        guard !token.isEmpty else {
            setupPinnedOpen = true
            applySetupVisibility()
            render(message: "Paste your app token above and click Save.", trades: [], signals: [])
            return
        }
        guard let url = URL(string: baseURL() + "/api/ticker/live") else { return }
        loadingLive = true
        var req = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 8)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        URLSession.shared.dataTask(with: req) { [weak self] data, response, _ in
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            DispatchQueue.main.async {
                guard let self = self else { return }
                self.loadingLive = false
                if code == 401 {
                    self.setupPinnedOpen = true
                    self.applySetupVisibility()
                    self.render(message: "Token was rejected. Paste a new one and click Save.", trades: [], signals: [])
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
        } else {
            let rows = trades.compactMap { tradeRow($0) }
            if rows.isEmpty {
                stack.addArrangedSubview(line("None", color: NSColor(calibratedWhite: 0.55, alpha: 1)))
            } else {
                rows.forEach { stack.addArrangedSubview($0) }
            }
        }
        stack.addArrangedSubview(heading("Active signals"))
        if message != nil {
            stack.addArrangedSubview(line("—", color: NSColor(calibratedWhite: 0.45, alpha: 1)))
        } else {
            let rows = signals.compactMap { signalRow($0) }
            if rows.isEmpty {
                stack.addArrangedSubview(line("None", color: NSColor(calibratedWhite: 0.55, alpha: 1)))
            } else {
                rows.forEach { stack.addArrangedSubview($0) }
            }
        }
        stack.layoutSubtreeIfNeeded()
        form.layoutSubtreeIfNeeded()
        let stackSize = stack.fittingSize
        stack.setFrameSize(NSSize(width: max(stackSize.width, 276), height: stackSize.height))
        applyWindowHeight(contentHeight: stackSize.height)
        scrollContentToTop()
    }

    func rowHeight() -> CGFloat {
        max(16, ceil(line("X", color: .labelColor).fittingSize.height))
    }

    func listHeight(lines: CGFloat) -> CGFloat {
        let gap = stack.spacing
        let insets = stack.edgeInsets.top + stack.edgeInsets.bottom
        return rowHeight() * lines + gap * max(0, lines - 1) + insets
    }

    func applyWindowHeight(contentHeight: CGFloat) {
        let minList = listHeight(lines: defaultLines)
        let maxList = listHeight(lines: maxLines)
        let layoutH = panel.contentLayoutRect.height
        let chrome: CGFloat = layoutH > 1 ? panel.frame.height - layoutH : 28
        let formH: CGFloat = form.isHidden ? 0 : form.fittingSize.height
        let footerH: CGFloat = setupFooter.isHidden ? 0 : 28
        let screenH = (panel.screen ?? NSScreen.main)?.visibleFrame.height ?? 800
        let screenCap = max(chrome + formH + footerH + rowHeight() * 3, screenH - 16)
        var height = formH + footerH + min(maxList, max(minList, contentHeight)) + chrome
        if height > screenCap { height = screenCap }
        let minWindow = min(height, formH + footerH + minList + chrome)
        panel.minSize = NSSize(width: minW, height: max(160, minWindow))
        let maxWindow = min(screenCap, formH + footerH + maxList + chrome)
        panel.maxSize = NSSize(width: 460, height: max(panel.minSize.height, maxWindow))
        height = min(max(height, panel.minSize.height), panel.maxSize.height)
        var frame = panel.frame
        let bottom = frame.minY
        frame.size.height = height
        frame.size.width = max(minW, frame.width)
        frame.origin.y = bottom
        if let vis = (panel.screen ?? NSScreen.main)?.visibleFrame {
            if frame.maxY > vis.maxY { frame.origin.y = vis.maxY - frame.height }
            if frame.minY < vis.minY { frame.origin.y = vis.minY }
        }
        panel.setFrame(frame, display: true)
    }

    func scrollContentToTop() {
        panel.layoutIfNeeded()
        guard let doc = scroll.documentView else { return }
        let clipH = scroll.contentView.bounds.height
        guard doc.frame.height > clipH + 1 else { return }
        let y: CGFloat = doc.isFlipped ? 0 : doc.frame.height - clipH
        scroll.contentView.scroll(to: NSPoint(x: 0, y: y))
        scroll.reflectScrolledClipView(scroll.contentView)
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

    func scriptName(_ row: [String: Any]) -> String {
        "\(row["symbol"] ?? "")".trimmingCharacters(in: .whitespacesAndNewlines)
    }

    func usableScript(_ name: String) -> Bool {
        !name.isEmpty && name != "—" && name != "-" && name != "–"
    }

    func tradeRow(_ row: [String: Any]) -> NSView? {
        let sym = scriptName(row)
        guard usableScript(sym), let pnl = row["pnl"] as? Double else { return nil }
        return pair(sym, formatRupees(pnl), liveYellow)
    }

    func formatRupees(_ amount: Double) -> String {
        let rounded = amount.rounded()
        if rounded == 0 { return "₹0" }
        let sign = rounded > 0 ? "+" : "-"
        return sign + "₹" + String(format: "%.0f", abs(rounded))
    }

    func signalRow(_ row: [String: Any]) -> NSView? {
        let left = scriptName(row)
        guard usableScript(left) else { return nil }
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
