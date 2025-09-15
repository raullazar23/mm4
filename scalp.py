import asyncio
import logging
import json
import statistics
import math
from datetime import datetime, timezone, timedelta
from okx import Trade, Account, MarketData

# ================== CONFIG ==================
with open('config.json') as f:
    config = json.load(f)

API_KEY = config["API_KEY"]
API_SECRET = config["API_SECRET"]
PASSPHRASE = config["PASSPHRASE"]

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class MomentumScalper:
    def __init__(self, symbol='ETH-EUR', trade_amount=100, profit_target=1, loss_limit=1.5, wait_time=5):
        self.symbol = symbol
        self.trade_amount = trade_amount      # sumă în QUOTE (EUR) pentru MARKET BUY
        self.profit_target = profit_target    # profit absolut per ETH (ex: +1 EUR/ETH)
        self.loss_limit = loss_limit          # stop absolut per ETH
        self.wait_time = wait_time
        self.bar = "1m"

        # buffer local cu istoric (ASC) sub forma [ts, o, h, l, c, vol]
        self.candles = []

        # OKX clients
        self.market_api = MarketData.MarketAPI(
            domain='https://eea.okx.com',
            api_key=API_KEY, api_secret_key=API_SECRET, passphrase=PASSPHRASE, flag='1'
        )
        self.trade_api = Trade.TradeAPI(
            domain='https://eea.okx.com',
            api_key=API_KEY, api_secret_key=API_SECRET, passphrase=PASSPHRASE, flag='1'
        )
        self.account_api = Account.AccountAPI(
            domain='https://eea.okx.com',
            api_key=API_KEY, api_secret_key=API_SECRET, passphrase=PASSPHRASE, flag='1'
        )

    # ================== ORCHESTRATOR ==================
    async def start(self):
        logger.info("Starting Momentum Scalper Bot")
        await self.cancel_all_orders()
        self.backfill_last_day()

        while True:
            await self.run_cycle()
            await asyncio.sleep(self.wait_time)

    async def run_cycle(self):
        # update incremental (1m) și cere 24h min
        self.refresh_incremental()
        if not self.has_full_day():
            logger.info("Nu am încă 24h de date, sar ciclul...")
            return

        candles = self.candles  # ASC, confirmate
        if len(candles) < 300:
            logger.info("Istoric insuficient pentru indicatori stabili (min 300).")
            return

        # ---------- indicatori calculați pe fereastră mare ----------
        closes = [float(c[4]) for c in candles]
        vols   = [float(c[5]) for c in candles]

        ema5_series   = self.calculate_ema_series(closes, 5)
        ema20_series  = self.calculate_ema_series(closes, 20)
        rsi_series    = self.calculate_rsi_series(closes, 14)
        lower_band    = self.calculate_bollinger_lower(candles, 20, 2)
        vwap_value    = self.calculate_vwap(candles)     # VWAP pe sesiunea curentă
        current_price = float(candles[-1][4])

        ema5 = ema5_series[-1]
        ema20 = ema20_series[-1]
        rsi = rsi_series[-1]

        logger.info(
            f"EMA5: {ema5:.2f}, EMA20: {ema20:.2f}, RSI: {rsi:.2f}, "
            f"Bollinger lower: {lower_band:.2f}, VWAP: {vwap_value:.2f}, Price: {current_price:.2f}"
        )

        # ---------- confirmare pe ultimele K lumânări ----------
        K = 3
        ema_bull_seq = [a > b for a, b in zip(ema5_series, ema20_series)]
        ema_bear_seq = [a < b for a, b in zip(ema5_series, ema20_series)]
        ema_bull_ok = self.confirm_last_k(ema_bull_seq, K)
        # ema_bear_ok = self.confirm_last_k(ema_bear_seq, K)  # disponibil dacă vrei short logic

        self.current_position = self.get_asset_balance()  # ETH disponibil

        if self.current_position <= 0.000001:
            # intrare „mai safe”: ema5>ema20 confirmat + rsi sub 70 + preț aproape de partea de jos a canalului
            if ema_bull_ok and (rsi is not None and rsi < 70) and current_price <= lower_band:
                logger.info("Buy Signal — EMA crossover confirmat & RSI ok & sub lower band")
                await self.place_buy_order(current_price)
        else:
            ordId = self.get_last_order_id()
            if not ordId:
                logger.warning("Nu am găsit ultimul order id pentru poziție.")
                return
            order_info = self.trade_api.get_order(instId=self.symbol, ordId=ordId)
            fill_price = float(order_info['data'][0]['fillPx'])

            # țintă și stop absolut (ținând cont de fee 0.08% pe ieșire)
            target_price = (fill_price + self.profit_target) * (1 + 0.0008)
            stop_price = fill_price - self.loss_limit

            logger.info(f"Position held at {current_price:.2f}, Target: {target_price:.2f}, Stop: {stop_price:.2f}")

            if current_price >= target_price:
                sell_qty = self.truncate(self.get_asset_balance(), 6)
                if sell_qty > 0:
                    logger.info("Profit target reached — selling")
                    await self.place_sell_order(sell_qty)
            elif current_price <= stop_price:
                sell_qty = self.truncate(self.get_asset_balance(), 6)
                if sell_qty > 0:
                    logger.info("Stop-loss hit — selling")
                    await self.place_sell_order(sell_qty)

    # ================== INDICATORI „SAFE” ==================
    def calculate_ema_series(self, closes, period: int):
        """EMA list-aligned; seed SMA la prima fereastră pentru stabilitate."""
        if len(closes) == 0:
            return []
        if period <= 1:
            return closes[:]
        k = 2 / (period + 1)
        ema = []
        for i, p in enumerate(closes):
            if i == 0:
                ema.append(p)
            elif i < period:
                prev = ema[-1]
                ema.append(prev + k * (p - prev))
            elif i == period - 1:
                sma = sum(closes[:period]) / period
                ema[-1] = sma
                ema.append(sma + k * (closes[period] - sma))
            else:
                prev = ema[-1]
                ema.append(prev + k * (p - prev))
        return ema

    def calculate_rsi_series(self, closes, period: int = 14):
        """RSI cu smoothing (Wilder). Returnează listă aliniată cu closes (primele valori None)."""
        n = len(closes)
        if n == 0 or period <= 0:
            return []
        gains = [0.0]
        losses = [0.0]
        for i in range(1, n):
            delta = closes[i] - closes[i-1]
            gains.append(max(delta, 0.0))
            losses.append(max(-delta, 0.0))
        rsi = []
        avg_gain = sum(gains[1:period+1]) / period if n > period else sum(gains) / max(1, len(gains))
        avg_loss = sum(losses[1:period+1]) / period if n > period else sum(losses) / max(1, len(losses))
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

    def calculate_bollinger_lower(self, candles, period, num_std_dev):
        """Returnează doar banda INFERIOARĂ (float) calculată corect."""
        closes = [float(k[4]) for k in candles]
        if len(closes) < period:
            return float('nan')
        window = closes[-period:]
        sma = sum(window) / period
        std_dev = statistics.pstdev(window) if len(window) > 1 else 0.0
        lower_band = sma - num_std_dev * std_dev
        return lower_band

    def calculate_vwap(self, candles):
        """VWAP cumulativ pe sesiune (toată seria primită)."""
        cumulative_vp = 0.0
        cumulative_vol = 0.0
        for candle in candles:
            high = float(candle[2]); low = float(candle[3]); close = float(candle[4]); volume = float(candle[5])
            typical_price = (high + low + close) / 3.0
            cumulative_vp += typical_price * volume
            cumulative_vol += volume
        return (cumulative_vp / cumulative_vol) if cumulative_vol else float('nan')

    def confirm_last_k(self, seq_bool, k=3):
        if len(seq_bool) < k:
            return False
        return all(seq_bool[-k:])

    # ================== ORDERS ==================
    async def place_buy_order(self, ref_price_for_log: float):
        """
        MARKET BUY folosind sumă în QUOTE (EUR) via tgtCcy='quote_ccy'.
        """
        result = self.trade_api.place_order(
            instId=self.symbol, tdMode='cash', side='buy', ordType='market',
            sz=str(self.trade_amount), tgtCcy='quote_ccy'
        )
        if result['code'] == '0':
            self.order_id = result['data'][0]['ordId']
            logger.info(f"Placed market buy for ~{self.trade_amount} {self.symbol.split('-')[1]} (ref price {ref_price_for_log:.2f})")
            await asyncio.sleep(1)  # scurt delay pentru a lăsa fill-urile să apară
            filled_qty = self.get_asset_balance()
            if filled_qty == 0:
                logger.error("No buy fills found")
                return
            self.current_position = round(filled_qty, 6)
            self.buy_price = ref_price_for_log
            logger.info(f"Bought {self.current_position} {self.symbol.split('-')[0]}")
        else:
            logger.error(f"Buy order failed: {result}")

    async def place_sell_order(self, sell_qty: float):
        """
        MARKET SELL cu cantitate în BASE (ETH).
        """
        result = self.trade_api.place_order(
            instId=self.symbol, tdMode='cash', side='sell', ordType='market', sz=str(sell_qty)
        )
        if result['code'] == '0':
            logger.info(f"Sold {sell_qty} {self.symbol.split('-')[0]} at market")
            self.current_position = 0.0
            self.buy_price = 0.0
        else:
            logger.error(f"Sell order failed: {result}")

    async def cancel_all_orders(self):
        open_orders = self.trade_api.get_order_list(instType='SPOT', instId=self.symbol)
        for order in open_orders.get('data', []):
            self.trade_api.cancel_order(instId=self.symbol, ordId=order['ordId'])
        logger.info("All open orders cancelled")

    # ================== UTILS ==================
    def usd_to_eth(self, amount_quote=None):
        """Conversie simplă pentru loguri (nu pentru sizing exact)."""
        amount_quote = self.trade_amount if amount_quote is None else amount_quote
        ticker = self.market_api.get_ticker(self.symbol)
        if ticker['code'] != '0':
            logger.error(f"Failed to fetch ticker: {ticker}")
            return 0.0
        ask_price = float(ticker['data'][0]['askPx'])
        base_amount = round(amount_quote / ask_price, 6) if ask_price else 0.0
        logger.info(f"Converting {amount_quote} {self.symbol.split('-')[1]} → {base_amount} {self.symbol.split('-')[0]} at ask {ask_price:.2f}")
        return base_amount

    def get_asset_balance(self):
        base_ccy = self.symbol.split('-')[0]
        balances = self.account_api.get_account_balance()
        for balance in balances['data'][0]['details']:
            if balance['ccy'] == base_ccy:
                return float(balance['availBal'])
        return 0.0

    def truncate(self, number, decimals):
        min_value = 0.00001
        factor = 10.0 ** decimals
        truncated = math.trunc(number * factor)
        result = truncated / factor
        return 0.0 if result < min_value else result

    def get_last_order_id(self):
        response = self.trade_api.get_orders_history(instType='SPOT', instId=self.symbol)
        orders = response.get('data', [])
        if not orders:
            return None
        last_order = orders[0]
        return last_order['ordId']

    # ================== BACKFILL + INCREMENTAL ==================
    def _okx_confirm(self, row) -> int:
        """
        OKX /market/candles:         [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm] (len >= 9)
        OKX /market/history-candles: [ts,o,h,l,c,vol,volCcy,confirm]             (len >= 8)
        """
        if len(row) >= 9:
            return int(row[8])
        if len(row) >= 8:
            return int(row[7])
        return 1

    def _fetch_candles(self, *, before=None, after=None, limit=300):
        """
        GET /api/v5/market/candles
        - 'after'  => înregistrări MAI VECHI decât ts (timestamp mai MIC).
        - 'before' => înregistrări MAI NOI  decât ts (timestamp mai MARE). Folosit singur => ultimele date.
        Returnează lista 'data' (cele mai noi primele).
        """
        params = {"instId": self.symbol, "bar": self.bar, "limit": limit}
        if before is not None:
            params["before"] = str(int(before))
        if after is not None:
            params["after"] = str(int(after))
        res = self.market_api.get_candlesticks(**params)
        return res.get("data", [])

    def _fetch_history(self, *, before=None, after=None, limit=100):
        """GET /api/v5/market/history-candles (fallback)"""
        params = {"instId": self.symbol, "bar": self.bar, "limit": limit}
        if before is not None:
            params["before"] = str(int(before))
        if after is not None:
            params["after"] = str(int(after))
        try:
            res = self.market_api.get_history_candlesticks(**params)
            return res.get("data", [])
        except Exception:
            return []

    def backfill_last_day(self):
        """
        Strategie robustă:
        1) Ia un lot de „ultimele” lumânări (fără cursor) -> cele mai noi primele.
        2) Dacă cel mai vechi ts din out e > since_ms, mergi ÎNAPOI în timp cu after=oldest_ts (mai vechi).
        3) Continuă până acoperi 24h sau rar -> fallback pe /history-candles.
        4) Normalizează la [ts,o,h,l,c,vol] ASC, doar confirmate și în fereastra 24h.
        """
        since_ms = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
        out = []

        # pas 1: ultima pagină (cele mai noi)
        batch = self._fetch_candles(limit=300)
        if batch:
            out.extend(batch)
        else:
            # fallback direct
            batch = self._fetch_history(limit=100)
            out.extend(batch)

        max_iters = 200
        iter_count = 0
        while True:
            iter_count += 1
            if iter_count > max_iters:
                logger.warning("backfill_last_day: max iters reached; breaking to avoid loop.")
                break
            if not out:
                break

            # cel mai vechi ts din ce avem până acum (în batch-urile OKX datele sunt DESC)
            oldest_ts = min(int(r[0]) for r in out)
            if oldest_ts <= since_ms:
                break  # avem acoperire până la since_ms

            # pas 2: mergem mai vechi cu after=oldest_ts
            older = self._fetch_candles(after=oldest_ts, limit=300)
            if not older:
                # fallback: history
                older = self._fetch_history(after=oldest_ts, limit=100)

            if not older:
                logger.info("backfill_last_day: no more older data; break.")
                break

            prev_min = oldest_ts
            out.extend(older)

            new_min = min(int(r[0]) for r in older)
            if new_min >= prev_min:
                # fără progres (SDK ignoră cursorul) => ieșim
                logger.warning("backfill_last_day: no pagination progress (after=%s -> min still %s).", oldest_ts, new_min)
                break

        # Normalizează: confirmate + în fereastra 24h + sort ASC
        norm = []
        for r in out:
            ts = int(r[0])
            confirm = self._okx_confirm(r)
            if ts >= since_ms and confirm == 1:
                norm.append([ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])])
        norm.sort(key=lambda x: x[0])
        self.candles = norm

        if self.candles:
            span = (self.candles[-1][0] - self.candles[0][0]) / 1000
            logger.info("Backfill: %d candles, span %.1f min (%.2f h).", len(self.candles), span/60, span/3600)
        else:
            logger.warning("Backfill: 0 candles loaded.")

    def refresh_incremental(self):
        """
        Ia DOAR barele noi față de ultimul timestamp din cache:
        - latest_ts = self.candles[-1][0]
        - folosim before=latest_ts pentru a obține „mai noi decât ts” (spre prezent)
        - deduplicăm și păstrăm ASC
        """
        if not getattr(self, "candles", None):
            self.backfill_last_day()
            return

        latest_ts = int(self.candles[-1][0])
        # cerem cele mai noi față de ce avem
        batch = self._fetch_candles(before=latest_ts, limit=300)
        if not batch:
            return

        latest = {}
        for r in batch:
            ts = int(r[0])
            confirm = self._okx_confirm(r)
            if confirm == 0:
                continue
            latest[ts] = [ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]

        existing = {row[0]: row for row in self.candles}
        existing.update(latest)
        merged = sorted(existing.values(), key=lambda x: x[0])

        # păstrăm ultimele 2 zile ca limită de memorie
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000)
        self.candles = [r for r in merged if r[0] >= cutoff]

        logger.info("Refresh: now %d candles; newest ts=%s.", len(self.candles), self.candles[-1][0] if self.candles else "n/a")

    def has_full_day(self) -> bool:
        if not self.candles:
            return False
        span_ms = self.candles[-1][0] - self.candles[0][0]
        return span_ms >= (24 * 60 * 60 * 1000 - 60 * 1000)


if __name__ == "__main__":
    scalper = MomentumScalper()
    asyncio.run(scalper.start())
