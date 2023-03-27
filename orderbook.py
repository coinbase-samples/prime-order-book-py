# Copyright 2023-present Coinbase Global, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import asyncio
import json, hmac, hashlib, base64, os, time, sys, math
import websockets
import pandas as pd
import sqlite3
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.environ.get("API_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
PASSPHRASE = os.environ.get("PASSPHRASE")
SVC_ACCOUNTID = os.environ.get("SVC_ACCOUNTID")

URI = 'wss://ws-feed.prime.coinbase.com'
TIMESTAMP = str(int(time.time()))
conn = sqlite3.connect('./prime_orderbook.db')

channel = 'l2_data'
product_id = 'ETH-USD'
agg_level = '0.1'


async def main_loop():
    while True:
        try:
            async with websockets.connect(URI, ping_interval=None, max_size=None) as websocket:
                signature = sign(
                    channel,
                    ACCESS_KEY,
                    SECRET_KEY,
                    SVC_ACCOUNTID,
                    product_id)
                auth_message = json.dumps({
                    'type': 'subscribe',
                    'channel': channel,
                    'access_key': ACCESS_KEY,
                    'api_key_id': SVC_ACCOUNTID,
                    'timestamp': TIMESTAMP,
                    'passphrase': PASSPHRASE,
                    'signature': signature,
                    'product_ids': [product_id]
                })
                await websocket.send(auth_message)
                while True:
                    response = await websocket.recv()
                    parsed = json.loads(response)

                    if parsed["channel"] == "l2_data" and parsed["events"][0]["type"] == "snapshot":
                        processor = OrderBookProcessor(response)
                    elif processor is not None:
                        processor.apply_update(response)
                    if processor is not None:
                        table = processor.create_df(agg_level=agg_level)
                        print('updated')
                        table.to_sql(
                            'book', conn, if_exists='replace', index=False)
                        sys.stdout.flush()
        except websockets.ConnectionClosed:
            continue


class OrderBookProcessor():
    def __init__(self, snapshot):
        self.bids = []
        self.offers = []
        snapshot_data = json.loads(snapshot)
        px_levels = snapshot_data["events"][0]["updates"]
        for i in range(len(px_levels)):
            level = px_levels[i]
            if level["side"] == "bid":
                self.bids.append(level)
            elif level["side"] == "offer":
                self.offers.append(level)
            else:
                raise IOError()
        self._sort()

    def apply_update(self, data):
        event = json.loads(data)
        if event["channel"] != "l2_data":
            return
        events = event["events"]
        for e in events:
            updates = e["updates"]
            for update in updates:
                self._apply(update)
        self._filter_closed()
        self._sort()

    def _apply(self, level):
        if level["side"] == "bid":
            found = False
            for i in range(len(self.bids)):
                if self.bids[i]["px"] == level["px"]:
                    self.bids[i] = level
                    found = True
                    break
            if not found:
                self.bids.append(level)
        else:
            found = False
            for i in range(len(self.offers)):
                if self.offers[i]["px"] == level["px"]:
                    self.offers[i] = level
                    found = True
                    break
            if not found:
                self.offers.append(level)

    def _filter_closed(self):
        self.bids = [x for x in self.bids if abs(float(x["qty"])) > 0]
        self.offers = [x for x in self.offers if abs(float(x["qty"])) > 0]

    def _sort(self):
        self.bids = sorted(self.bids, key=lambda x: float(x["px"]) * -1)
        self.offers = sorted(self.offers, key=lambda x: float(x["px"]))

    def create_df(self, agg_level):

        bids = self.bids[:300]
        asks = self.offers[:300]

        bid_df = pd.DataFrame(bids, columns=['px', 'qty'], dtype=float)
        ask_df = pd.DataFrame(asks, columns=['px', 'qty'], dtype=float)

        bid_df = self.aggregate_levels(
            bid_df, agg_level=Decimal(agg_level), side='bid')
        ask_df = self.aggregate_levels(
            ask_df, agg_level=Decimal(agg_level), side='offer')

        bid_df = bid_df.sort_values('px', ascending=False)
        ask_df = ask_df.sort_values('px', ascending=False)

        bid_df.reset_index(inplace=True)
        bid_df['id'] = bid_df['index'].index.astype(str) + '_bid'

        ask_df = ask_df.iloc[::-1]
        ask_df.reset_index(inplace=True)
        ask_df['id'] = ask_df['index'].index.astype(str) + '_ask'
        ask_df = ask_df.iloc[::-1]

        order_book = pd.concat([ask_df, bid_df])
        return order_book

    def aggregate_levels(self, levels_df, agg_level, side):
        if side == 'bid':
            right = False
            def label_func(x): return x.left
        elif side == 'offer':
            right = True
            def label_func(x): return x.right

        min_level = math.floor(Decimal(min(levels_df.px)) / agg_level - 1) * agg_level
        max_level = math.ceil(Decimal(max(levels_df.px)) / agg_level + 1) * agg_level

        level_bounds = [float(min_level + agg_level * x)
                        for x in range(int((max_level - min_level) / agg_level) + 1)]

        levels_df['bin'] = pd.cut(levels_df.px, bins=level_bounds, precision=10, right=right)

        levels_df = levels_df.groupby('bin').agg(qty=('qty', 'sum')).reset_index()

        levels_df['px'] = levels_df.bin.apply(label_func)
        levels_df = levels_df[levels_df.qty > 0]
        levels_df = levels_df[['px', 'qty']]

        return levels_df


def sign(channel, key, secret, account_id, product_ids):
    message = channel + key + account_id + TIMESTAMP + product_ids
    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        message.encode('utf-8'),
        digestmod=hashlib.sha256).digest()
    signature_b64 = base64.b64encode(signature).decode()
    return signature_b64


if __name__ == "__main__":
    asyncio.run(main_loop())
