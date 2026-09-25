import Foundation
import SwiftUI
import UIKit

@MainActor
final class TickerSession: ObservableObject {
    static let shared = TickerSession()

    @Published var baseURL: String
    @Published var token: String
    @Published var status = "Paste the app token from Settings → Live ticker."
    @Published var trades: [OpenTrade] = []
    @Published var updatedAt: Date?

    private var foregroundTimer: Timer?
    private var ongoing: Task<Void, Never>?
    private let backgroundLease = BackgroundLease()

    private init() {
        let storedBase = KeychainStore.read("base")
        baseURL = storedBase.isEmpty ? TickerConfig.defaultBase : storedBase
        token = KeychainStore.read("token")
        if !token.isEmpty {
            status = "Waiting for TradeWithCTO…"
        }
    }

    func becameActive() {
        startTimer()
        refresh()
    }

    func enteredBackground() {
        stopTimer()
        let application = UIApplication.shared
        backgroundLease.begin(application)
        Task {
            await refreshNow()
            BackgroundRefresh.schedule()
            backgroundLease.end(application)
        }
    }

    func save() {
        let base = normalizeTickerBase(baseURL)
        let trimmed = token.trimmingCharacters(in: .whitespacesAndNewlines)
        baseURL = base
        token = trimmed
        guard KeychainStore.set(base, account: "base") else {
            status = "Could not store the server URL in Keychain."
            return
        }
        if trimmed.isEmpty {
            KeychainStore.delete("token")
        } else if !KeychainStore.set(trimmed, account: "token") {
            status = "Could not store the app token in Keychain."
            return
        }
        refresh()
    }

    func refresh() {
        Task { await refreshNow() }
    }

    func refreshNow() async {
        if let ongoing {
            await ongoing.value
            return
        }
        let task = Task { @MainActor in
            await self.perform()
        }
        ongoing = task
        await task.value
        ongoing = nil
    }

    private func perform() async {
        if Task.isCancelled { return }
        let savedToken = token.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !savedToken.isEmpty else {
            trades = []
            status = "Paste the app token from Settings → Live ticker."
            _ = await LiveActivitySync.sync(trades: [])
            return
        }
        do {
            let snapshot = try await TickerClient.fetch(base: baseURL, token: savedToken)
            if Task.isCancelled { return }
            if snapshot.enabled == false {
                trades = []
                updatedAt = Date()
                _ = await LiveActivitySync.sync(trades: [])
                status = "Ticker is off in Settings."
                return
            }
            let open = OpenTrade.ranked(snapshot.trades)
            trades = open
            updatedAt = Date()
            let result = await LiveActivitySync.sync(trades: open)
            status = message(for: result, count: open.count)
        } catch TickerClientError.unauthorized {
            status = "Token was rejected. Paste a new one and tap Save."
        } catch is CancellationError {
            return
        } catch let error as URLError where error.code == .cancelled {
            return
        } catch {
            status = "Waiting for TradeWithCTO…"
        }
    }

    private func message(for result: LiveActivityResult, count: Int) -> String {
        if count == 0 { return "No open trades" }
        let noun = count == 1 ? "open trade" : "open trades"
        switch result {
        case .updated:
            return "\(count) \(noun) on the Lock Screen"
        case .ended:
            return "No open trades"
        case .unavailable:
            return "\(count) \(noun). Turn on Live Activities for this app in Settings."
        case .failed:
            return "\(count) \(noun). The Live Activity will start on the next refresh."
        }
    }

    private func startTimer() {
        foregroundTimer?.invalidate()
        let timer = Timer(timeInterval: TickerConfig.pollSeconds, repeats: true) { [weak self] _ in
            Task { @MainActor in
                self?.refresh()
            }
        }
        RunLoop.main.add(timer, forMode: .common)
        foregroundTimer = timer
    }

    private func stopTimer() {
        foregroundTimer?.invalidate()
        foregroundTimer = nil
    }
}

/// Ends the UIKit background task if iOS expires it before the fetch returns.
private final class BackgroundLease: @unchecked Sendable {
    private let lock = NSLock()
    private var id: UIBackgroundTaskIdentifier = .invalid
    private var closed = false

    func begin(_ application: UIApplication) {
        end(application)
        lock.lock()
        closed = false
        lock.unlock()
        let lease = self
        var started: UIBackgroundTaskIdentifier = .invalid
        started = application.beginBackgroundTask(withName: "ticker.background") {
            lease.end(application)
        }
        lock.lock()
        let alreadyClosed = closed
        if !alreadyClosed {
            id = started
        }
        lock.unlock()
        if alreadyClosed, started != .invalid {
            application.endBackgroundTask(started)
        }
    }

    func end(_ application: UIApplication) {
        lock.lock()
        closed = true
        let current = id
        id = .invalid
        lock.unlock()
        if current != .invalid {
            application.endBackgroundTask(current)
        }
    }
}
