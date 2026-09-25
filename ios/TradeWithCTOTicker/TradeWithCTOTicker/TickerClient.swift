import Foundation

struct TickerSnapshot {
    var enabled: Bool
    var trades: [OpenTrade]
}

enum TickerClientError: Error {
    case badURL
    case unauthorized
    case badResponse
}

enum TickerClient {
    /// `GET /api/ticker/live`. Signals are ignored; only the open-trade array is kept.
    static func fetch(base: String, token: String) async throws -> TickerSnapshot {
        let root = normalizeTickerBase(base)
        guard let url = URL(string: root + "/api/ticker/live") else {
            throw TickerClientError.badURL
        }
        var request = URLRequest(
            url: url,
            cachePolicy: .reloadIgnoringLocalCacheData,
            timeoutInterval: TickerConfig.requestTimeout
        )
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await URLSession.shared.data(for: request)
        let code = (response as? HTTPURLResponse)?.statusCode ?? 0
        if code == 401 { throw TickerClientError.unauthorized }
        guard code == 200,
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw TickerClientError.badResponse
        }
        let enabled = json["enabled"] as? Bool ?? true
        let rows = json["trades"] as? [[String: Any]] ?? []
        return TickerSnapshot(enabled: enabled, trades: OpenTrade.fromRows(rows))
    }
}
