import asyncio
import logging
import json
import math
from datetime import datetime, timezone, timedelta
from okx import Trade, Account, MarketData

# Load API keys
with open('config.json') as f:
    config = json.load(f)

API_KEY = config["API_KEY"]
API_SECRET = config["API_SECRET"]
PASSPHRASE = config["PASSPHRASE"]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('httpx').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

class MomentumScalper:
    def __init__(self, symbol='ETH-USDC', trade_amount=1000, profit_target=0.20, loss_limit=0.20, wait_time=5):
        self.symbol = symbol
        self.trade_amount = trade_amount
        self.profit_target = profit_target
        self.loss_limit = loss_limit
        self.wait_time = wait_time

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

        self.current_position = 0.0
        self.buy_price = 0.0

    async def start(self):
        logger.info("Starting Momentum Scalper Bot")
        await self.cancel_all_orders()

        while True:
            await self.run_cycle()
            await asyncio.sleep(self.wait_time)

    async def run_cycle(self):
        candles = self.get_candles()
        if len(candles) < 20:
            logger.info("Not enough candle data, skipping...")
            return

        ema5 = self.calculate_ema(candles, 5)
        ema20 = self.calculate_ema(candles, 20)
        rsi = self.calculate_rsi(candles, 14)
        current_price = float(candles[-1][4])

        logger.info(f"EMA5: {ema5:.2f}, EMA20: {ema20:.2f}, RSI: {rsi:.2f}, Price: {current_price:.2f}")
        self.current_position = self.get_asset_balance()
        if self.current_position <= 0.000001:
            if ema5 > ema20 and rsi < 70:
                logger.info("Buy Signal — EMA crossover & RSI healthy")
                await self.place_buy_order(current_price)
        else:
            target_price = (self.buy_price + self.profit_target) * (1 + 0.0008)
            stop_price = self.buy_price - self.loss_limit

            logger.info(f"Position held at {self.buy_price:.2f}, Target: {target_price:.2f}, Stop: {stop_price:.2f}")

            if current_price >= target_price:
                sell_qty = self.truncate(self.get_asset_balance(),6)
                logger.info("Profit target reached — selling")
                await self.place_sell_order(sell_qty)
            elif current_price <= stop_price:
                sell_qty = self.truncate(self.get_asset_balance(),6)
                logger.info("Stop-loss hit — selling")
                await self.place_sell_order(sell_qty)

    def get_candles(self):
        resp = self.market_api.get_candlesticks(instId=self.symbol, bar='1m', limit=50)
        return resp['data'][::-1]  # reverse to chronological order

    def calculate_ema(self, candles, period):
        closes = [float(k[4]) for k in candles]
        ema = sum(closes[:period]) / period
        multiplier = 2 / (period + 1)
        for price in closes[period:]:
            ema = (price - ema) * multiplier + ema
        return ema

    def calculate_rsi(self, candles, period):
        closes = [float(k[4]) for k in candles]
        gains, losses = [], []
        for i in range(1, len(closes)):
            delta = closes[i] - closes[i - 1]
            if delta >= 0:
                gains.append(delta)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(delta))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    async def place_buy_order(self, price):
        eth_amount = round(self.usd_to_eth(), 6)
        result = self.trade_api.place_order(
                instId=self.symbol, tdMode='cash', side='buy', ordType='market', sz=str(100))
        if result['code'] == '0':
            logger.info(f"Placed market buy for {eth_amount} ETH at ~{price:.2f}")
            # Update to fetch actual filled size
            await asyncio.sleep(1)
            filled_qty = self.get_asset_balance()
            if filled_qty == 0:
                logger.error("No buy fills found")
                return
            self.current_position = round(filled_qty, 6)
            self.buy_price = price
            logger.info(f"Bought {self.current_position} ETH")
        else:
            logger.error(f"Buy order failed: {result}")

    async def place_sell_order(self, price):
        result = self.trade_api.place_order(
        instId=self.symbol, tdMode='cash', side='sell', ordType='market', sz=str(price))
        if result['code'] == '0':
            logger.info(f"Sold {self.current_position} ETH at ~{price:.2f}")
            self.current_position = 0.0
            self.buy_price = 0.0
        else:
            logger.error(f"Sell order failed: {result}")

    async def cancel_all_orders(self):
        open_orders = self.trade_api.get_order_list(instType='SPOT', instId=self.symbol)
        for order in open_orders['data']:
            self.trade_api.cancel_order(instId=self.symbol, ordId=order['ordId'])
        logger.info("All open orders cancelled")
    
    def usd_to_eth(self):
        usd_amount=1000
        # Fetch the latest ticker for ETH-USDC
        ticker = self.market_api.get_ticker(self.symbol)
        if ticker['code'] != '0':
            logger.error(f"Failed to fetch ticker: {ticker}")
            return 0.0

        ask_price = float(ticker['data'][0]['askPx'])
        eth_amount = round(usd_amount / ask_price, 6)
        logger.info(f"Converting {usd_amount} USDC → {eth_amount} ETH at ask price {ask_price:.2f}")
        return eth_amount
    
    def get_asset_balance(self):
        balances = self.account_api.get_account_balance()
        for balance in balances['data'][0]['details']:
            if balance['ccy'] == 'ETH':
                return float(balance['availBal'])
        return 0.0
    

    def truncate(self, number, decimals):
        min_value = 0.00001
        factor = 10.0 ** decimals
        truncated = math.trunc(number * factor)
        result = truncated / factor
        if result < min_value:
            return 0.0
        return result

if __name__ == "__main__":
    scalper = MomentumScalper()
    asyncio.run(scalper.start())
