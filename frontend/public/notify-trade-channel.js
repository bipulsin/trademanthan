/**
 * POST /auth/notify-* — Telegram TradeWithCTO channel (server-side).
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

    function parseNotifyError(data, res) {
        const errMsg =
            typeof data.detail === "string"
                ? data.detail
                : Array.isArray(data.detail)
                  ? data.detail.map((x) => x.msg || JSON.stringify(x)).join("; ")
                  : data.message || res.statusText || "Notify failed";
        return errMsg;
    }

    async function authPostJson(path, body) {
        const token = localStorage.getItem("trademanthan_token");
        if (!token || !token.includes(".")) {
            throw new Error("Please log in to send a notification.");
        }
        const res = await fetch(window.getTrademanthanApiBase() + path, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Authorization: "Bearer " + token,
            },
            body: JSON.stringify(body),
        });
        let data = {};
        try {
            data = await res.json();
        } catch (_) {}
        if (!res.ok) throw new Error(parseNotifyError(data, res));
        return data;
    }

    /**
     * @param {"intraoption"|"pivot_breakout"} context
     */
    window.notifyTradeChannel = async function (context) {
        return authPostJson("/auth/notify-trade-channel", { context });
    };

    /**
     * Custom message from left menu; server appends username and posts to @TradeWithCTO.
     * @param {string} message
     */
    window.notifyTelegramUserMessage = async function (message) {
        return authPostJson("/auth/notify-telegram-user-message", {
            message: String(message || "").trim(),
        });
    };
})();
