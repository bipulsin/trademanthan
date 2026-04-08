"""
Traditional Renko (close-based) brick series.
Brick size is supplied externally (e.g. ATR(1H,14)).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Sequence, Tuple

BrickColor = Literal["GREEN", "RED"]


@dataclass
class RenkoBrick:
    color: BrickColor
    open_level: float
    close_level: float


def build_traditional_renko(closes: Sequence[float], brick_size: float) -> List[RenkoBrick]:
    """
    Classic close-only Renko: emit bricks when price moves brick_size from last brick close.
    Multiple bricks can form from one close if gap is large.
    """
    bricks: List[RenkoBrick] = []
    if not closes or brick_size <= 0:
        return bricks

    last_brick_close = float(closes[0])
    for c in closes[1:]:
        price = float(c)
        while True:
            diff = price - last_brick_close
            if diff >= brick_size:
                o = last_brick_close
                last_brick_close = o + brick_size
                bricks.append(RenkoBrick("GREEN", o, last_brick_close))
            elif diff <= -brick_size:
                o = last_brick_close
                last_brick_close = o - brick_size
                bricks.append(RenkoBrick("RED", o, last_brick_close))
            else:
                break
    return bricks


def last_n_brick_colors(bricks: List[RenkoBrick], n: int) -> List[BrickColor]:
    if not bricks or n <= 0:
        return []
    out: List[BrickColor] = [b.color for b in bricks[-n:]]
    return out


def count_alternations(colors: Sequence[BrickColor]) -> int:
    if len(colors) < 2:
        return 0
    alt = 0
    for i in range(1, len(colors)):
        if colors[i] != colors[i - 1]:
            alt += 1
    return alt


def max_run_length(colors: Sequence[BrickColor]) -> int:
    if not colors:
        return 0
    best = 1
    cur = 1
    for i in range(1, len(colors)):
        if colors[i] == colors[i - 1]:
            cur += 1
            best = max(best, cur)
        else:
            cur = 1
    return best


def renko_structure_filter_long(bricks: List[RenkoBrick]) -> Tuple[bool, str]:
    """
    LONG structure: last 3–5 greens, not zig-zag, max 1 red in last 6, <=2 alternations in window.
    """
    if len(bricks) < 5:
        return False, "not_enough_bricks"
    colors = [b.color for b in bricks[-6:]] if len(bricks) >= 6 else [b.color for b in bricks]
    reds = sum(1 for c in colors if c == "RED")
    if reds > 1:
        return False, "too_many_reds_last6"
    last5 = [b.color for b in bricks[-5:]]
    greens5 = sum(1 for c in last5 if c == "GREEN")
    if greens5 < 3:
        return False, "not_enough_green_last5"
    if count_alternations(last5) > 2:
        return False, "too_much_alternation"
    # zig-zag: strict alternation pattern G R G R
    if len(last5) >= 4:
        zig = all(last5[i] != last5[i + 1] for i in range(len(last5) - 1))
        if zig and len(last5) >= 4:
            return False, "zigzag_pattern"
    return True, "ok"


def renko_structure_filter_short(bricks: List[RenkoBrick]) -> Tuple[bool, str]:
    """SHORT structure: mirror of long with RED as trend."""
    if len(bricks) < 5:
        return False, "not_enough_bricks"
    colors = [b.color for b in bricks[-6:]] if len(bricks) >= 6 else [b.color for b in bricks]
    greens = sum(1 for c in colors if c == "GREEN")
    if greens > 1:
        return False, "too_many_greens_last6"
    last5 = [b.color for b in bricks[-5:]]
    reds5 = sum(1 for c in last5 if c == "RED")
    if reds5 < 3:
        return False, "not_enough_red_last5"
    if count_alternations(last5) > 2:
        return False, "too_much_alternation"
    if len(last5) >= 4:
        zig = all(last5[i] != last5[i + 1] for i in range(len(last5) - 1))
        if zig and len(last5) >= 4:
            return False, "zigzag_pattern"
    return True, "ok"


def entry_pullback_long(bricks: List[RenkoBrick]) -> Tuple[bool, str]:
    """
    LONG entry: trend green, pullback max 1 red brick, new green forms (last brick green).
    """
    if len(bricks) < 3:
        return False, "short_series"
    last3 = [b.color for b in bricks[-3:]]
    if bricks[-1].color != "GREEN":
        return False, "last_not_green"
    reds = sum(1 for c in last3 if c == "RED")
    if reds > 1:
        return False, "pullback_gt1"
    return True, "ok"


def entry_pullback_short(bricks: List[RenkoBrick]) -> Tuple[bool, str]:
    if len(bricks) < 3:
        return False, "short_series"
    last3 = [b.color for b in bricks[-3:]]
    if bricks[-1].color != "RED":
        return False, "last_not_red"
    greens = sum(1 for c in last3 if c == "GREEN")
    if greens > 1:
        return False, "pullback_gt1"
    return True, "ok"


def exit_two_opposite_bricks(bricks: List[RenkoBrick], position_direction: str) -> bool:
    """
    Exit when last 2 bricks are opposite to trade direction (for 1m faster renko).
    position_direction: LONG or SHORT
    """
    if len(bricks) < 2:
        return False
    a, b = bricks[-2].color, bricks[-1].color
    if a != b:
        return False
    if position_direction == "LONG":
        return a == "RED" and b == "RED"
    if position_direction == "SHORT":
        return a == "GREEN" and b == "GREEN"
    return False
