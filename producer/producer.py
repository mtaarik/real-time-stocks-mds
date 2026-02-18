import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# 1. Load the variables from the .env file
load_dotenv()

# 2. Get the key from the environment
API_KEY = os.getenv("FINNHUB_API_KEY")
# Check if the key exists to avoid errors later
if not API_KEY:
    raise ValueError("API_KEY not found! Make sure it is set in the .env file.")

BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

#Initial Producer
producer = KafkaProducer (
    bootstrap_servers=["host.docker.internal:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#Retrive Data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int (time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

#Looping and Pushing to Stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}")
            producer.send("stock-quotes", value=quote)
    time.sleep(6)