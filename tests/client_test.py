from persistent_ohlc_client import PersistentOhlcClient

client = PersistentOhlcClient()
client.get_ohlc("BTC-USD", ta="some fake TA")
print("banana")
