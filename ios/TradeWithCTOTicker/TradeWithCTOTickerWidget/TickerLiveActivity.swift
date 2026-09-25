import ActivityKit
import SwiftUI
import WidgetKit

@main
struct TickerWidgetBundle: WidgetBundle {
    var body: some Widget {
        TickerLiveActivity()
    }
}

struct TickerLiveActivity: Widget {
    var body: some WidgetConfiguration {
        ActivityConfiguration(for: TickerActivityAttributes.self) { context in
            LockScreenTrades(state: context.state)
                .activityBackgroundTint(TickerColor.charcoal)
                .activitySystemActionForegroundColor(TickerColor.green)
        } dynamicIsland: { context in
            DynamicIsland {
                DynamicIslandExpandedRegion(.leading) {
                    Text("TWCTO")
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(TickerColor.green)
                }
                DynamicIslandExpandedRegion(.trailing) {
                    if context.state.openCount > 1 {
                        Text("\(context.state.openCount)")
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(TickerColor.green)
                    }
                }
                DynamicIslandExpandedRegion(.bottom) {
                    TradeListView(state: context.state, tight: true)
                }
            } compactLeading: {
                CompactLeading(state: context.state)
            } compactTrailing: {
                CompactTrailing(state: context.state)
            } minimal: {
                Image(systemName: "chart.line.uptrend.xyaxis")
                    .foregroundStyle(TickerColor.green)
            }
            .keylineTint(TickerColor.green)
        }
    }
}

private struct LockScreenTrades: View {
    let state: TickerActivityAttributes.ContentState

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("TRADEWITHCTO")
                .font(.caption2.weight(.semibold))
                .foregroundStyle(TickerColor.green)
            TradeListView(state: state, tight: false)
        }
        .padding(14)
    }
}

private struct TradeListView: View {
    let state: TickerActivityAttributes.ContentState
    var tight: Bool

    var body: some View {
        let overflow = state.openCount > 6
        let rows = overflow ? Array(state.trades.prefix(5)) : state.trades
        let extra = state.openCount - rows.count
        VStack(alignment: .leading, spacing: tight ? 2 : 4) {
            ForEach(Array(rows.enumerated()), id: \.offset) { _, trade in
                HStack(spacing: 8) {
                    Text(trade.symbol)
                        .font(.system(size: tight ? 12 : 14, weight: .medium))
                        .foregroundStyle(TickerColor.text)
                        .lineLimit(1)
                    Spacer(minLength: 6)
                    Text(formatRupees(trade.pnl))
                        .font(.system(size: tight ? 12 : 14, weight: .semibold).monospacedDigit())
                        .foregroundStyle(TickerColor.pnl)
                        .lineLimit(1)
                }
                .accessibilityElement(children: .combine)
            }
            if overflow && extra > 0 {
                Text("+\(extra) more")
                    .font(.system(size: tight ? 12 : 14, weight: .semibold))
                    .foregroundStyle(TickerColor.green)
            }
        }
    }
}

private struct CompactLeading: View {
    let state: TickerActivityAttributes.ContentState

    var body: some View {
        if let head = state.trades.first {
            if state.openCount > 1 {
                (Text(head.symbol).foregroundColor(TickerColor.text)
                    + Text(" ")
                    + Text(formatRupees(head.pnl)).foregroundColor(TickerColor.pnl))
                    .font(.caption2.weight(.semibold))
                    .lineLimit(1)
                    .minimumScaleFactor(0.6)
            } else {
                Text(head.symbol)
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(TickerColor.text)
                    .lineLimit(1)
                    .minimumScaleFactor(0.6)
            }
        } else {
            Text("TWCTO")
                .font(.caption2.weight(.semibold))
                .foregroundStyle(TickerColor.green)
        }
    }
}

private struct CompactTrailing: View {
    let state: TickerActivityAttributes.ContentState

    var body: some View {
        if state.openCount > 1 {
            Text("\(state.openCount)")
                .font(.caption.weight(.bold))
                .foregroundStyle(TickerColor.green)
        } else if let head = state.trades.first {
            Text(formatRupees(head.pnl))
                .font(.caption2.weight(.semibold))
                .foregroundStyle(TickerColor.pnl)
                .lineLimit(1)
                .minimumScaleFactor(0.7)
        }
    }
}
