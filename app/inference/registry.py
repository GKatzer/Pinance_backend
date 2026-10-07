"""Реестр торговых пар.

Единственный источник истины для символов, которые поддерживает система, и
для перевода между отображаемой формой ('BTC/USDT') и формой Binance/VDS2
('BTCUSDT'). Раньше это множество было продублировано в candles.py, stream.py
и binance_ws.py по отдельности — здесь оно одно, остальные модули импортируют.
"""

from __future__ import annotations

DISPLAY_SYMBOLS: tuple[str, ...] = ("BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT")

_VALID = set(DISPLAY_SYMBOLS)


def to_binance(symbol: str) -> str:
    """'BTC/USDT' → 'BTCUSDT' (форма, которую понимают Binance API и VDS2)."""
    return symbol.replace("/", "")


def normalize(raw: str) -> str:
    """Произвольный ввод ('btcusdt', 'BTC-USDT', 'BTC%2FUSDT') → каноническая
    отображаемая форма, либо '' если символ не входит в DISPLAY_SYMBOLS."""
    s = raw.upper().replace("-", "/").replace("%2F", "/")
    if "/" not in s and s.endswith("USDT"):
        s = s[:-4] + "/USDT"
    return s if s in _VALID else ""
