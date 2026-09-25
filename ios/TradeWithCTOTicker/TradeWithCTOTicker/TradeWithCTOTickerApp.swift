import SwiftUI
import UIKit

final class AppDelegate: NSObject, UIApplicationDelegate {
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        BackgroundRefresh.register()
        return true
    }
}

@main
struct TradeWithCTOTickerApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var session = TickerSession.shared

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(session)
                .onAppear {
                    if scenePhase == .active {
                        session.becameActive()
                    }
                }
        }
        .onChange(of: scenePhase) { phase in
            switch phase {
            case .active:
                session.becameActive()
            case .background:
                session.enteredBackground()
            case .inactive:
                break
            @unknown default:
                break
            }
        }
    }
}
