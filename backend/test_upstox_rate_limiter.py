"""Timing tests for the shared candle-request rate limiter."""
import threading
import time

from backend.services.upstox_rate_limiter import SlidingWindowRateLimiter


def test_per_second_cap_paces_requests():
    # 5 requests per second; the 6th must wait until the window rolls.
    rl = SlidingWindowRateLimiter([(5, 1.0)])
    start = time.monotonic()
    for _ in range(6):
        rl.acquire()
    elapsed = time.monotonic() - start
    # 6th request can only be granted ~1s after the first.
    assert elapsed >= 0.9, elapsed


def test_no_limits_is_noop():
    rl = SlidingWindowRateLimiter([])
    start = time.monotonic()
    for _ in range(100):
        rl.acquire()
    assert (time.monotonic() - start) < 0.1


def test_min_interval_evens_out_bursts():
    # Even spacing of 0.1s -> 5 grants take ~0.4s (4 gaps), no instant burst.
    rl = SlidingWindowRateLimiter([(100, 1.0)], min_interval=0.1)
    start = time.monotonic()
    for _ in range(5):
        rl.acquire()
    assert (time.monotonic() - start) >= 0.35


def test_thread_safe_total_count_respects_cap():
    # 10/sec cap; 20 threads each acquire once -> at least ~1s of pacing total.
    rl = SlidingWindowRateLimiter([(10, 1.0)])
    start = time.monotonic()

    def worker():
        rl.acquire()

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 20 grants at 10/sec -> the last batch waits ~1s. (``_events`` is pruned of
    # entries older than the widest window, so its length is not the total count.)
    assert (time.monotonic() - start) >= 0.9


def test_multiple_windows_enforced():
    # Tight per-minute cap dominates a loose per-second cap.
    rl = SlidingWindowRateLimiter([(100, 1.0), (3, 60.0)])
    start = time.monotonic()
    for _ in range(3):
        rl.acquire()  # all immediate
    immediate = time.monotonic() - start
    assert immediate < 0.2
    # 4th must wait against the per-minute cap; bound the test by not actually
    # waiting 60s — just confirm a positive wait is computed.
    with rl._lock:
        wait = rl._wait_needed(time.monotonic())
    assert wait > 0
