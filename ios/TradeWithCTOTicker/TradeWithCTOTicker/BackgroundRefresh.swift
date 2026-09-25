import BackgroundTasks
import Foundation

/// Opportunistic top-up. iOS may never run this; the foreground poll is the real cadence.
enum BackgroundRefresh {
    static func register() {
        BGTaskScheduler.shared.register(forTaskWithIdentifier: TickerConfig.refreshTaskId, using: nil) { task in
            guard let refresh = task as? BGAppRefreshTask else {
                task.setTaskCompleted(success: false)
                return
            }
            handle(refresh)
        }
    }

    static func schedule() {
        BGTaskScheduler.shared.cancel(taskRequestWithIdentifier: TickerConfig.refreshTaskId)
        let request = BGAppRefreshTaskRequest(identifier: TickerConfig.refreshTaskId)
        request.earliestBeginDate = Date(timeIntervalSinceNow: TickerConfig.pollSeconds)
        try? BGTaskScheduler.shared.submit(request)
    }

    private static func handle(_ task: BGAppRefreshTask) {
        schedule()
        let work = Task { @MainActor in
            await TickerSession.shared.refreshNow()
        }
        let gate = CompletionGate(task)
        task.expirationHandler = {
            work.cancel()
            gate.finish(false)
        }
        Task {
            await work.value
            gate.finish(!work.isCancelled)
        }
    }
}

private final class CompletionGate {
    private let task: BGAppRefreshTask
    private let lock = NSLock()
    private var finished = false

    init(_ task: BGAppRefreshTask) {
        self.task = task
    }

    func finish(_ success: Bool) {
        lock.lock()
        defer { lock.unlock() }
        if finished { return }
        finished = true
        task.setTaskCompleted(success: success)
    }
}
