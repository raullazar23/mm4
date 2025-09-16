import asyncio
import contextlib
import json
import logging
import math
import statistics
import time
from datetime import datetime, timezone

import ccxt            # pip install ccxt
import websockets      # pip install websockets


# ================== CONFIG ==================
with open('config.json') as f:
    config = json.load(f)

API_KEY = config["API_KEY"]
API_SECRET = config["API_SECRET"]

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('ccxt.base.exchange').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class MomentumScalper:
    """
    Binance USDT-M Futures scalper using 1-second candles from aggTrade.
    Goal: +$0.50 net per round-trip (after taker fees), regardless of position size.
    """

    def __init__(
        self,
        symbol: str = "BTC/USDT",     # USDT-M perpetual
        trade_amount: float = 50.0,   # USDT per entry (converted to qty)
        net_target_usd: float = 0.50, # desired NET profit in USDT per trade
        taker_fee: float = 0.0004,    # 0.04% default on Binance futures; adjust if needed
        wait_time: float = 1.0,       # seconds between decision cycles
        use_testnet: bool = True,     # show activity in testnet UI
        leverage: int = 5,
        margin_mode: str = "cross",
        keep_seconds: int = 600,      # ~10 minutes of 1-second bars
        min_warmup_seconds: int = 180,# need this many 1s bars before trading
        use_forward_fill: bool = True # forward-fill missing seconds
    ):
        self.symbol = symbol
        self.trade_amount = float(trade_amount)
        self.net_target_usd = float(net_target_usd)
        self.taker_fee = float(taker_fee)
        self.wait_time = float(wait_time)
        self.keep_ms = int(keep_seconds * 1000)
        self.min_warmup_seconds = int(min_warmup_seconds)
        self.use_ffill = bool(use_forward_fill)

        # internal state / data
        self.candles_1s = []  # ASC: [ts_ms, o, h, l, c, v]
        self.ws_task = None

        # position state
        self.last_buy_order_id = None
        self.entry_price = 0.0
        self.position_qty = 0.0  # in base (ETH)
        self.tp_price = None
        self.sl_price = None

        # base/quote split
        try:
            self.base_ccy, self.quote_ccy = self.symbol.split("/")
        except Exception:
            raise ValueError("Symbol must be like 'BTC/USDT'")

        # CCXT: Binance USDT-M futures client
        self.ex = ccxt.binanceusdm({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        })
        self.ex.set_sandbox_mode(bool(use_testnet))
        self.ex.load_markets()

        self.leverage = int(leverage)
        self.margin_mode = margin_mode.lower()

        self._setup_futures_account()

    # ---------- Setup ----------
    def _setup_futures_account(self):
        try:
            # one-way (not hedged)
            if hasattr(self.ex, "set_position_mode"):
                self.ex.set_position_mode(hedged=False)
            # margin mode
            if hasattr(self.ex, "set_margin_mode"):
                self.ex.set_margin_mode(self.margin_mode.upper(), self.symbol)
            # leverage
            if hasattr(self.ex, "set_leverage"):
                self.ex.set_leverage(self.leverage, self.symbol)
        except Exception as e:
            logger.warning(f"Futures account setup warning: {e}")

    # ---------- Lifecycle ----------
    async def start(self):
        logger.info("Starting Futures scalper (1s live data; %+0.2f USDT net target)", self.net_target_usd)
        await self.cancel_all_orders()

        # seed a small window from recent trades (best-effort)
        self._seed_from_recent_trades(minutes=5)

        # start websocket aggregator for 1s bars
        self.ws_task = asyncio.create_task(self._ws_aggtrade_to_1s())

        try:
            while True:
                await self.run_cycle()
                await asyncio.sleep(self.wait_time)
        finally:
            if self.ws_task and not self.ws_task.done():
                self.ws_task.cancel()
                with contextlib.suppress(Exception):
                    await self.ws_task

    # ---------- Core loop ----------
    async def run_cycle(self):
        candles = self._get_contiguous_1s_window()
        if len(candles) < self.min_warmup_seconds:
            logger.info("Warming up 1s data: %d/%d", len(candles), self.min_warmup_seconds)
            return

        closes = [c[4] for c in candles]
        vols   = [c[5] for c in candles]

        # ---- Indicators (1s) ----
        ema_fast = self._ema_series(closes, 20)
        ema_slow = self._ema_series(closes, 60)
        rsi      = self._rsi_series(closes, 14)
        vwap     = self._vwap(candles)                  # 10m rolling window by design
        bb_lower = self._bb_lower(closes, period=60, k=2)
        atr_60   = self._atr(candles, period=60)        # for stop sizing

        if not ema_fast or not ema_slow or not rsi:
            return

        px = closes[-1]
        ema_fast_now = ema_fast[-1]
        ema_slow_now = ema_slow[-1]
        rsi_now = rsi[-1] if rsi[-1] is not None else float("nan")

        trend_up = ema_fast_now > ema_slow_now and ema_slow[-1] > (ema_slow[-2] if len(ema_slow) >= 2 else ema_slow[-1])

        logger.info(
            "1s | px=%.2f ema20=%.2f>ema60=%.2f? %s | rsi=%.1f | vwap=%.2f | bbL=%.2f | atr60=%.3f",
            px, ema_fast_now, ema_slow_now, trend_up, rsi_now, vwap, bb_lower, atr_60
        )

        # Refresh position snapshot
        self.position_qty = self._get_position_qty()
        in_pos = abs(self.position_qty) > 1e-9

        # ---- Position management (TP/SL) ----
        if in_pos:
            self.entry_price = self._get_entry_price() or self.entry_price or px
            # Recompute TP/SL continuously (in case fees or size changed)
            self.tp_price = self._compute_take_profit(self.entry_price, abs(self.position_qty))
            self.sl_price = self._compute_stop_loss(self.entry_price, atr_60)

            logger.info("POS qty=%.6f @ %.2f | TP=%.2f | SL=%.2f", self.position_qty, self.entry_price, self.tp_price, self.sl_price)

            # exits (reduce-only)
            if px >= self.tp_price:
                qty = self._truncate_amount(abs(self.position_qty))
                if qty > 0:
                    logger.info("TP hit — closing %.6f", qty)
                    await self._market_sell(qty, reduce_only=True)
                    return
            if px <= self.sl_price:
                qty = self._truncate_amount(abs(self.position_qty))
                if qty > 0:
                    logger.info("SL hit — closing %.6f", qty)
                    await self._market_sell(qty, reduce_only=True)
                    return

        # ---- Entry logic (long only) ----
        if not in_pos:
            # High-accuracy filter: uptrend + momentum healthy + not extended down vs VWAP + pullback proximity
            pullback_ok = px <= max(bb_lower, ema_fast_now)
            rsi_ok = (not math.isnan(rsi_now)) and (45 <= rsi_now <= 70)
            if trend_up and rsi_ok and px >= vwap and pullback_ok:
                qty = self._size_from_quote(px, self.trade_amount)
                qty = self.max_qty_using_only_cash()
                if qty > 0:
                    logger.info("ENTRY signal: trend_up=%s rsi_ok=%s pullback_ok=%s → BUY %.6f", trend_up, rsi_ok, pullback_ok, qty)
                    await self._market_buy(qty)
                    # after entry, recompute TP/SL instantly
                    self.position_qty = self._get_position_qty()
                    self.entry_price = self._get_entry_price() or px
                    self.tp_price = self._compute_take_profit(self.entry_price, abs(self.position_qty))
                    self.sl_price = self._compute_stop_loss(self.entry_price, atr_60)
                    logger.info("After entry: qty=%.6f @ %.2f | TP=%.2f | SL=%.2f", self.position_qty, self.entry_price, self.tp_price, self.sl_price)

    # ---------- Indicators ----------
    def _ema_series(self, closes, period: int):
        if not closes:
            return []
        if period <= 1:
            return closes[:]
        k = 2 / (period + 1)
        ema = []
        for i, p in enumerate(closes):
            if i == 0:
                ema.append(p)
            elif i < period:
                ema.append(ema[-1] + k * (p - ema[-1]))
            elif i == period - 1:
                sma = sum(closes[:period]) / period
                ema[-1] = sma
                ema.append(sma + k * (closes[period] - sma))
            else:
                ema.append(ema[-1] + k * (p - ema[-1]))
        return ema

    def _rsi_series(self, closes, period: int = 14):
        n = len(closes)
        if n == 0 or period <= 0:
            return []
        gains = [0.0]
        losses = [0.0]
        for i in range(1, n):
            d = closes[i] - closes[i - 1]
            gains.append(max(d, 0.0))
            losses.append(max(-d, 0.0))
        rsi = []
        avg_gain = sum(gains[1:period + 1]) / period if n > period else sum(gains) / max(1, len(gains))
        avg_loss = sum(losses[1:period + 1]) / period if n > period else sum(losses) / max(1, len(losses))
        for i in range(n):
            if i < period:
                rsi.append(None)
            elif i == period:
                rs = (avg_gain / avg_loss) if avg_loss != 0 else float('inf')
                rsi.append(100 - (100 / (1 + rs)))
            else:
                avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                avg_loss = (avg_loss * (period - 1) + losses[i]) / period
                rs = (avg_gain / avg_loss) if avg_loss != 0 else float('inf')
                rsi.append(100 - (100 / (1 + rs)))
        return rsi

    def _bb_lower(self, closes, period=60, k=2):
        if len(closes) < period:
            return float('nan')
        window = closes[-period:]
        sma = sum(window) / period
        std = statistics.pstdev(window) if len(window) > 1 else 0.0
        return sma - k * std

    def _atr(self, candles, period=60):
        """True Range with previous close; on 1s data this approximates to high-low."""
        if len(candles) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(candles)):
            prev_c = candles[i - 1][4]
            h = candles[i][2]
            l = candles[i][3]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)
        window = trs[-period:]
        return sum(window) / len(window) if window else 0.0

    def _vwap(self, candles):
        """Session VWAP over the kept window (~10 minutes)."""
        vp = 0.0
        vol = 0.0
        for _, _, h, l, c, v in candles:
            typical = (h + l + c) / 3.0
            vp += typical * v
            vol += v
        return (vp / vol) if vol else float('nan')

    # ---------- Targets & sizing ----------
    def _compute_take_profit(self, entry_price: float, qty_base: float) -> float:
        """
        Solve for dP so that NET PnL >= net_target_usd considering taker fees on both legs.
        Net = qty*dP - fee*qty*(2*entry + dP)  >= target
        => dP >= (target/qty + 2*fee*entry) / (1 - fee)
        """
        fee = self.taker_fee
        qty = max(qty_base, 1e-12)
        dP = (self.net_target_usd / qty + 2 * fee * entry_price) / max(1e-9, (1 - fee))
        raw_tp = entry_price + dP
        # snap to price precision & ensure at least one tick above entry
        tp = self._price_to_precision(max(raw_tp, entry_price + self._min_tick()))
        return tp

    def _compute_stop_loss(self, entry_price: float, atr_60: float) -> float:
        # protective stop: max(1.5*ATR_60s, 0.2% of entry)
        dist = max(1.5 * atr_60, entry_price * 0.002)
        raw_sl = entry_price - dist
        sl = self._price_to_precision(min(raw_sl, entry_price - self._min_tick()))
        return sl

    def _size_from_quote(self, price: float, quote_amount: float) -> float:
        qty = quote_amount / max(price, 1e-12)
        qty = self._truncate_amount(qty)
        return qty

    # ---------- Broker ops ----------
    async def _market_buy(self, qty: float):
        try:
            qty = self._truncate_amount(qty)
            if qty <= 0:
                return
            self.ex.create_order(self.symbol, "market", "buy", qty, params={"reduceOnly": False})
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"Buy failed: {e}")

    async def _market_sell(self, qty: float, reduce_only: bool = True):
        try:
            qty = self._truncate_amount(qty)
            if qty <= 0:
                return
            self.ex.create_order(self.symbol, "market", "sell", qty, params={"reduceOnly": bool(reduce_only)})
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.error(f"Sell failed: {e}")

    async def cancel_all_orders(self):
        try:
            for o in self.ex.fetch_open_orders(self.symbol):
                with contextlib.suppress(Exception):
                    self.ex.cancel_order(o["id"], self.symbol)
            logger.info("All open orders cancelled.")
        except Exception as e:
            logger.info(f"No open orders or fetch failed: {e}")

    def _get_position_qty(self) -> float:
        try:
            pos = None
            if hasattr(self.ex, "fetch_positions"):
                arr = self.ex.fetch_positions([self.symbol])
                if arr:
                    pos = arr[0]
            if not pos:
                return 0.0
            # CCXT normalized
            contracts = pos.get("contracts")
            if contracts is not None:
                return float(contracts)
            # raw
            return float(pos["info"].get("positionAmt", 0))
        except Exception as e:
            logger.warning(f"fetch_positions error: {e}")
            return 0.0

    def _get_entry_price(self):
        try:
            arr = self.ex.fetch_positions([self.symbol])
            if arr:
                p = arr[0]
                ep = p.get("entryPrice")
                if ep is not None:
                    return float(ep)
                raw = p["info"].get("entryPrice")
                return float(raw) if raw else None
            return None
        except Exception:
            return None

    # ---------- Precision helpers ----------
    def _truncate_amount(self, qty: float) -> float:
        try:
            return float(self.ex.amount_to_precision(self.symbol, qty))
        except Exception:
            factor = 10 ** 6
            return math.trunc(qty * factor) / factor

    def _price_to_precision(self, p: float) -> float:
        try:
            return float(self.ex.price_to_precision(self.symbol, p))
        except Exception:
            factor = 10 ** 2
            return math.trunc(p * factor) / factor

    def _min_tick(self) -> float:
        try:
            m = self.ex.market(self.symbol)
            step = None
            if "limits" in m and m["limits"].get("price"):
                step = m["limits"]["price"].get("min") or m["precision"].get("price")
            # CCXT may not expose exact tick; fallback by precision
            if not step:
                prec = m["precision"]["price"]
                step = 10 ** (-prec)
            return float(step)
        except Exception:
            return 0.01

    # ---------- 1-second bars (WebSocket & seed) ----------
    def _ws_url_aggtrade(self) -> str:
        """
        Futures aggTrade stream:
        Testnet: wss://stream.binancefuture.com/ws/<symbol>@aggTrade
        Prod:    wss://fstream.binance.com/ws/<symbol>@aggTrade
        """
        sym = self.symbol.replace("/", "").lower()  # ETH/USDT -> ethusdt
        base = "wss://stream.binancefuture.com/ws" if self.ex.sandboxMode else "wss://fstream.binance.com/ws"
        return f"{base}/{sym}@aggTrade"

    def _append_1s(self, row):
        """row = [ts, o, h, l, c, v]"""
        if not row:
            return
        ts = row[0]
        if self.candles_1s and self.candles_1s[-1][0] == ts:
            last = self.candles_1s[-1]
            last[2] = max(last[2], row[2])
            last[3] = min(last[3], row[3])
            last[4] = row[4]
            last[5] += row[5]
        else:
            self.candles_1s.append(row)

        # trim to last keep_ms
        cutoff = int(time.time() * 1000) - self.keep_ms
        while self.candles_1s and self.candles_1s[0][0] < cutoff:
            self.candles_1s.pop(0)

    def _get_contiguous_1s_window(self):
        if not self.candles_1s:
            return []
        if not self.use_ffill:
            return list(self.candles_1s)
        # forward-fill gaps up to now-1s
        data = list(self.candles_1s)
        end_ms = (int(time.time()) - 1) * 1000
        if not data:
            return []
        filled = [data[0][:]]
        for i in range(1, len(data)):
            prev_ts = filled[-1][0]
            ts, o, h, l, c, v = data[i]
            expected = prev_ts + 1000
            if ts == expected:
                filled.append([ts, o, h, l, c, v])
            elif ts > expected:
                # fill missing seconds with flat candles (vol=0)
                last_close = filled[-1][4]
                while expected < ts:
                    filled.append([expected, last_close, last_close, last_close, last_close, 0.0])
                    expected += 1000
                filled.append([ts, o, h, l, c, v])
            # if ts < expected (out-of-order), ignore
        # fill until end_ms
        if filled:
            last_ts = filled[-1][0]
            last_close = filled[-1][4]
            t = last_ts + 1000
            while t <= end_ms:
                filled.append([t, last_close, last_close, last_close, last_close, 0.0])
                t += 1000
        return filled

    def _seed_from_recent_trades(self, minutes=5):
        """
        Best-effort seed: fetch recent trades and bucket into 1s bars.
        On quiet testnet, this might produce sparse data (ffill will handle gaps).
        """
        try:
            since = self.ex.milliseconds() - minutes * 60 * 1000
            trades = self.ex.fetch_trades(self.symbol, since=since, limit=1000)
        except Exception as e:
            logger.info(f"fetch_trades seed failed: {e}")
            return

        buckets = {}  # ts_sec_ms -> [ts, o, h, l, c, v]
        for t in trades:
            p = float(t["price"])
            q = float(t["amount"])
            ts = int(t["timestamp"] // 1000 * 1000)
            b = buckets.get(ts)
            if b is None:
                buckets[ts] = [ts, p, p, p, p, q]
            else:
                b[2] = max(b[2], p)
                b[3] = min(b[3], p)
                b[4] = p
                b[5] += q

        for ts in sorted(buckets.keys()):
            self._append_1s(buckets[ts])

        logger.info("Seeded %d seconds from recent trades.", len(buckets))

    async def _ws_aggtrade_to_1s(self):
        url = self._ws_url_aggtrade()
        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("WS connected: %s", url)
                    buckets = {}
                    while True:
                        raw = await ws.recv()
                        msg = json.loads(raw)
                        p = float(msg.get("p"))
                        q = float(msg.get("q"))
                        t = int(msg.get("T"))
                        ts = (t // 1000) * 1000
                        b = buckets.get(ts)
                        if b is None:
                            buckets[ts] = [ts, p, p, p, p, q]
                        else:
                            b[2] = max(b[2], p)
                            b[3] = min(b[3], p)
                            b[4] = p
                            b[5] += q

                        # flush fully closed seconds (<= now - 1s)
                        flush_before = (int(time.time()) - 1) * 1000
                        to_emit = [k for k in buckets.keys() if k <= flush_before]
                        for k in sorted(to_emit):
                            self._append_1s(buckets.pop(k))
            except Exception as e:
                logger.warning(f"WS error ({type(e).__name__}): {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def get_cash_available_usdt(self) -> float:
        """
        Returns your *own* free USDT (wallet collateral not tied up in margin/fees)
        on Binance USDT-M Futures. No leverage multipliers — plain available cash.
        """
        try:
            bal = self.ex.fetch_balance()  # futures wallet
            # CCXT-normalized path
            if isinstance(bal, dict) and 'free' in bal and self.quote_ccy in bal['free']:
                v = bal['free'][self.quote_ccy]
                return float(v) if v is not None else 0.0

            # Fallback to raw Binance fields
            info = bal.get('info', {})
            # Sometimes 'info' is a list of assets
            if isinstance(info, list):
                for a in info:
                    if a.get('asset') == self.quote_ccy:
                        v = a.get('availableBalance') or a.get('withdrawAvailable') or a.get('balance')
                        return float(v) if v is not None else 0.0
            # Or a dict with nested assets
            assets = info.get('assets') or info.get('balances') if isinstance(info, dict) else None
            if isinstance(assets, list):
                for a in assets:
                    if a.get('asset') == self.quote_ccy:
                        v = a.get('availableBalance') or a.get('walletBalance') or a.get('maxWithdrawAmount')
                        return float(v) if v is not None else 0.0

            return 0.0
        except Exception as e:
            logger.warning(f"get_cash_available_usdt error: {e}")
            return 0.0
    
    def max_qty_using_only_cash(self, price: float | None = None, fee_rate: float | None = None, buffer: float = 0.001) -> float:
        """
        Max BASE qty you can buy using *only your cash* (no leverage).
        Reserves a small `buffer` fraction for fees/rounding.
        Formula ensures: notional + taker_fee*notional <= cash_available*(1 - buffer).
        """
        try:
            fee = self.taker_fee if fee_rate is None else float(fee_rate)
            if price is None:
                price = float(self.ex.fetch_ticker(self.symbol)['last'])
            cash = self.get_cash_available_usdt()
            cash_eff = max(0.0, cash * (1.0 - float(buffer)))
            notional = cash_eff / (1.0 + fee)  # leave room to pay taker fee from cash
            qty = notional / max(price, 1e-12)
            return self._truncate_amount(qty)
        except Exception as e:
            logger.warning(f"max_qty_using_only_cash error: {e}")
            return 0.0


    # ---------- Misc ----------
    @staticmethod
    def _fmt_ts(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    bot = MomentumScalper(
        symbol="BTC/USDT",
        trade_amount=50.0,       # USDT per entry (adjust as you like)
        net_target_usd=0.50,     # net profit target per trade
        taker_fee=0.0004,        # 0.04% taker
        use_testnet=True,
        leverage=5,
        margin_mode="cross",
        keep_seconds=600,        # 10 minutes of 1s data
        min_warmup_seconds=180,  # require 3 min before trading
        use_forward_fill=True,
        wait_time=1.0
    )
    asyncio.run(bot.start())
