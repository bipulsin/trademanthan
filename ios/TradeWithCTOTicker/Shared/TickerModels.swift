import ActivityKit
import SwiftUI

/// Same rupee string the Mac ticker prints: sign, then the rupee symbol.
func formatRupees(_ amount: Double) -> String {
    let rounded = amount.rounded()
    if rounded == 0 { return "₹0" }
    let sign = rounded > 0 ? "+" : "-"
    return sign + "₹" + String(format: "%.0f", abs(rounded))
}

enum TickerConfig {
    static let defaultBase = "https://www.tradewithcto.com"
    static let pollSeconds: TimeInterval = 120
    static let requestTimeout: TimeInterval = 8
    static let keychainService = "com.tradewithcto.ticker"
    /// Keep this identical to BGTaskSchedulerPermittedIdentifiers in the app Info.plist.
    static let refreshTaskId = "com.tradewithcto.ticker.refresh"
    static let staleAfter: TimeInterval = 15 * 60
}

enum TickerColor {
    static let charcoal = Color(red: 0.07, green: 0.09, blue: 0.08)
    static let field = Color(red: 0.13, green: 0.16, blue: 0.14)
    static let green = Color(red: 0.45, green: 0.78, blue: 0.55)
    static let greenButton = Color(red: 0.45, green: 0.86, blue: 0.55)
    static let pnl = Color(red: 1, green: 0.93, blue: 0.05)
    static let text = Color(white: 0.92)
    static let muted = Color(white: 0.62)
}

struct OpenTrade: Codable, Hashable {
    var symbol: String
    var pnl: Double

    static func usableScript(_ name: String) -> Bool {
        !name.isEmpty && name != "—" && name != "-" && name != "–"
    }

    /// Open rows only: a script name and a numeric PnL. Desk names are not kept.
    static func fromRows(_ rows: [[String: Any]]) -> [OpenTrade] {
        rows.compactMap { row in
            let name = (row["symbol"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            guard usableScript(name), let pnl = rupeeNumber(row["pnl"]) else { return nil }
            return OpenTrade(symbol: name, pnl: pnl)
        }
    }

    static func ranked(_ trades: [OpenTrade]) -> [OpenTrade] {
        trades.sorted { lhs, rhs in
            let left = abs(lhs.pnl)
            let right = abs(rhs.pnl)
            if left != right { return left > right }
            return lhs.symbol < rhs.symbol
        }
    }

    private static func rupeeNumber(_ value: Any?) -> Double? {
        if value is Bool { return nil }
        if let number = value as? Double { return number }
        if let number = value as? Int { return Double(number) }
        if let number = value as? NSNumber { return number.doubleValue }
        return nil
    }
}

struct TickerActivityAttributes: ActivityAttributes {
    struct ContentState: Codable, Hashable {
        /// Largest absolute P&L first. At most the rows the island can show.
        var trades: [OpenTrade]
        var openCount: Int
    }

    /// Six rows max. Past that, five trades and a “+N more” row use `openCount`.
    static func contentState(for trades: [OpenTrade]) -> ContentState {
        let ranked = OpenTrade.ranked(trades)
        let stored = ranked.count > 6 ? Array(ranked.prefix(5)) : ranked
        return ContentState(trades: stored, openCount: ranked.count)
    }
}

func normalizeTickerBase(_ raw: String) -> String {
    var value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    while value.hasSuffix("/") { value.removeLast() }
    if value.isEmpty { return TickerConfig.defaultBase }
    if !value.contains("://") {
        value = "https://" + value
    }
    return value
}
