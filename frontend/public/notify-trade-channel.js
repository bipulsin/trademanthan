/**
 * POST /auth/notify-trade-channel — Telegram TradeWithCTO channel (server-side).
 * Requires logged-in JWT (trademanthan_token).
 */
(function () {
    window.getTrademanthanApiBase = function () {
        return window.location.hostname === "localhost"
            ? "http://localhost:8000"
            : "https://trademanthan.in";
    };

    /**
     * @param {"intraoption"|"pivot_breakout"} context
     */
    window.notifyTradeChannel = async function (context) {
        const token = localStorage.getItem("trademanthan_token");
        if (!token || !token.includes(".")) {
            throw new Error("Please log in to send a notification.");
        }
        const res = await fetch(
            window.getTrademanthanApiBase() + "/auth/notify-trade-channel",
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: "Bearer " + token,
                },
                body: JSON.stringify({ context }),
            }
        );
        let data = {};
        try {
            data = await res.json();
        } catch (_) {}
        const errMsg =
            typeof data.detail === "string"
                ? data.detail
                : Array.isArray(data.detail)
                  ? data.detail.map((x) => x.msg || JSON.stringify(x)).join("; ")
                  : data.message || res.statusText || "Notify failed";
        if (!res.ok) throw new Error(errMsg);
        return data;
    };
})();
