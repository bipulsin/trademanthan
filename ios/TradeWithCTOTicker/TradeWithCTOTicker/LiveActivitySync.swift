import ActivityKit
import Foundation

enum LiveActivityResult {
    case updated
    case ended
    case unavailable
    case failed
}

/// One local Live Activity. `pushType` stays nil; this never reads a push token.
@MainActor
enum LiveActivitySync {
    static func sync(trades: [OpenTrade]) async -> LiveActivityResult {
        let onScreen = Activity<TickerActivityAttributes>.activities.filter { isOnScreen($0) }

        guard !trades.isEmpty else {
            for activity in onScreen {
                await endNow(activity)
            }
            return .ended
        }

        let content = ActivityContent(
            state: TickerActivityAttributes.contentState(for: trades),
            staleDate: Date().addingTimeInterval(TickerConfig.staleAfter)
        )

        if let current = onScreen.first {
            for extra in onScreen.dropFirst() {
                await endNow(extra)
            }
            await current.update(content)
            return .updated
        }

        guard ActivityAuthorizationInfo().areActivitiesEnabled else {
            return .unavailable
        }

        do {
            _ = try Activity.request(
                attributes: TickerActivityAttributes(),
                content: content,
                pushType: nil
            )
            return .updated
        } catch {
            let retry = Activity<TickerActivityAttributes>.activities.filter { isOnScreen($0) }
            if let current = retry.first {
                for extra in retry.dropFirst() {
                    await endNow(extra)
                }
                await current.update(content)
                return .updated
            }
            return .failed
        }
    }

    private static func endNow(_ activity: Activity<TickerActivityAttributes>) async {
        let content = ActivityContent(
            state: TickerActivityAttributes.ContentState(trades: [], openCount: 0),
            staleDate: nil
        )
        await activity.end(content, dismissalPolicy: .immediate)
    }

    /// Ended and dismissed activities stay in `Activity.activities` but are not updated.
    private static func isOnScreen(_ activity: Activity<TickerActivityAttributes>) -> Bool {
        switch activity.activityState {
        case .active, .stale:
            return true
        case .ended, .dismissed:
            return false
        @unknown default:
            return false
        }
    }
}
