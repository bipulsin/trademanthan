/**
 * POST /auth/notify-trade-channel — Telegram TradeWithCTO channel (server-side).
 * Requires logged-in JWT (trademanthan_token).
 */
(function () {
    window.getTrademanthanApiBase = function () {
        if (window.location.hostname === "localhost") {
            return "http://localhost:8000";
        }
        // tradewithcto.com proxies /auth to FastAPI; trademanthan.in may return 405 for POST /auth (nginx).
        const h = window.location.hostname || "";
        if (h === "tradewithcto.com" || h === "www.tradewithcto.com") {
            return window.location.origin;
        }
        return "https://trademanthan.in";
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
