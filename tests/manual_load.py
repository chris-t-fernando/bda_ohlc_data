import asyncio
import time
from urllib.parse import quote_plus
import requests

start_str = "2023-03-29 01:04:31.850056+11:00"
since = quote_plus(start_str)
total_runs = 1000


def get_ohlc(id):
    url = f"http://127.0.0.1:8002/symbols/BTC-USD/ohlc/5m?start={since}"
    ret_get = requests.get(url)
    return ret_get


async def coro_get_ohlc(i):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, get_ohlc, i)


async def parallel():
    thislist = [coro_get_ohlc(i) for i in range(total_runs)]
    await asyncio.gather(*thislist)


if __name__ == "__main__":
    now = time.time()
    asyncio.run(parallel())
    duration = time.time() - now
    avg = total_runs / duration
    print(f"Finished run in {duration}, average of {avg:.1f} runs per second")
