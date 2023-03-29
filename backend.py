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
import asyncio, websockets, sqlite3, json, hmac, hashlib, base64, os, time, sys
from dotenv import load_dotenv
from orderbook import OrderBookProcessor

load_dotenv()

ACCESS_KEY = os.environ.get("API_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
PASSPHRASE = os.environ.get("PASSPHRASE")
SVC_ACCOUNTID = os.environ.get("SVC_ACCOUNTID")

URI = 'wss://ws-feed.prime.coinbase.com'
TIMESTAMP = str(int(time.time()))
conn = sqlite3.connect('prime_orderbook.db')
channel = 'l2_data'

product_id = 'ETH-USD'
agg_level = '0.1'
row_count = '50'


async def create_auth_message(channel, ACCESS_KEY, SECRET_KEY, SVC_ACCOUNTID, product_id, PASSPHRASE, TIMESTAMP):
    signature = sign(
        channel,
        ACCESS_KEY,
        SECRET_KEY,
        SVC_ACCOUNTID,
        product_id,
    )
    auth_message = json.dumps({
        'type': 'subscribe',
        'channel': channel,
        'access_key': ACCESS_KEY,
        'api_key_id': SVC_ACCOUNTID,
        'timestamp': TIMESTAMP,
        'passphrase': PASSPHRASE,
        'signature': signature,
        'product_ids': [product_id],
    })
    return auth_message


async def main_loop():
    while True:
        try:
            async with websockets.connect(URI, ping_interval=None, max_size=None) as websocket:
                auth_message = await create_auth_message(
                    channel,
                    ACCESS_KEY,
                    SECRET_KEY,
                    SVC_ACCOUNTID,
                    product_id,
                    PASSPHRASE,
                    TIMESTAMP
                )
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
                        table.to_sql('book', conn, if_exists='replace', index=False)
                        sys.stdout.flush()
        except websockets.ConnectionClosed:
            continue


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
