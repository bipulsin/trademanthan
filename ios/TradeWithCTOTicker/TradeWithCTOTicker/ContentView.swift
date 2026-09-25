import SwiftUI
import UIKit

struct ContentView: View {
    @EnvironmentObject private var session: TickerSession

    var body: some View {
        ZStack {
            TickerColor.charcoal.ignoresSafeArea()
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    Text("TRADEWITHCTO")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(TickerColor.green)
                    Text("Open trades")
                        .font(.title2.weight(.semibold))
                        .foregroundStyle(TickerColor.text)

                    fieldLabel("Server URL")
                    TextField(
                        "",
                        text: $session.baseURL,
                        prompt: Text(TickerConfig.defaultBase).foregroundColor(TickerColor.muted)
                    )
                    .keyboardType(.URL)
                    .textContentType(.URL)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled(true)
                    .fieldChrome()

                    fieldLabel("App token")
                    SecureField(
                        "",
                        text: $session.token,
                        prompt: Text("twt_…").foregroundColor(TickerColor.muted)
                    )
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled(true)
                    .fieldChrome()

                    Button(action: save) {
                        Text("Save")
                            .font(.body.weight(.semibold))
                            .foregroundStyle(TickerColor.charcoal)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 12)
                            .background(TickerColor.greenButton)
                            .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                    }

                    Text(session.status)
                        .font(.subheadline)
                        .foregroundStyle(TickerColor.muted)
                        .fixedSize(horizontal: false, vertical: true)

                    if let updatedAt = session.updatedAt {
                        Text("Updated \(updatedAt.formatted(date: .omitted, time: .shortened))")
                            .font(.caption)
                            .foregroundStyle(TickerColor.muted)
                    }

                    if !session.trades.isEmpty {
                        VStack(spacing: 8) {
                            ForEach(Array(session.trades.enumerated()), id: \.offset) { _, trade in
                                HStack(spacing: 8) {
                                    Text(trade.symbol)
                                        .foregroundStyle(TickerColor.text)
                                        .lineLimit(1)
                                    Spacer(minLength: 8)
                                    Text(formatRupees(trade.pnl))
                                        .font(.body.weight(.semibold).monospacedDigit())
                                        .foregroundStyle(TickerColor.pnl)
                                }
                                .accessibilityElement(children: .combine)
                            }
                        }
                        .padding(.top, 4)
                    }

                    Text("Refreshes every 2 minutes while this app is open, and again when you come back.")
                        .font(.caption)
                        .foregroundStyle(TickerColor.muted)
                        .fixedSize(horizontal: false, vertical: true)
                }
                .padding(20)
            }
        }
        .preferredColorScheme(.dark)
        .tint(TickerColor.green)
    }

    private func fieldLabel(_ title: String) -> some View {
        Text(title.uppercased())
            .font(.caption2.weight(.semibold))
            .foregroundStyle(TickerColor.green)
    }

    private func save() {
        UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
        session.save()
    }
}

private extension View {
    func fieldChrome() -> some View {
        self
            .font(.body)
            .foregroundStyle(TickerColor.text)
            .padding(12)
            .background(TickerColor.field)
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
    }
}
