## Fetch and Store Multiple Stocks with MetaData
import yfinance as yf
import pandas as pd
from arcticdb import Arctic

DB_PATH = "lmdb://./advanced_stock_db"
LIBRARY_NAME = "technical_analysis_data"

## Connect and Create Library
ac = Arctic(DB_PATH)
if LIBRARY_NAME not in ac.list_libraries():
    ac.create_library(LIBRARY_NAME)

lib = ac[LIBRARY_NAME]

## Symbols
symbols = ["AAPL", "NVDA", "TSLA", "MSFT", "GOOG"]

for symbol in symbols:
    ticker = yf.Ticker(symbol)
    hist_data = ticker.history(
        period = '3y',
        auto_adjust = False
    )

    ## Add Metadata with Data Source and Timestamp
    metadata = {"Source" : "Yahoo Finance",
               "retrieval_date" : pd.Timestamp.now()}
    
    if not hist_data.empty:
        lib.write(symbol, hist_data, metadata = metadata)
        print(f"{symbol} data stored with metadata")

## Batch Read and Computer Technical Indicators
import numpy as np

# Batch Read multiple stock data
symbols_data = lib.read_batch(symbols=symbols)

for symbol_data in symbols_data:
    df = symbol_data.data
    symbol = symbol_data.symbol

    ## Compute Technical Indicators : Bollinger Bands and MACD
    df['SMA_20'] = df['Close'].rolling(window = 20).mean()
    df['upper_band'] = df['SMA_20'] + 2 * df['Close'].rolling(window = 20).std()
    df['lower_band'] = df['SMA_20'] - 2 * df['Close'].rolling(window = 20).std()

    ## MACD Calculation
    df['EMA_12'] = df['Close'].ewm(span = 12, adjust = False).mean()
    df['EMA_26'] = df['Close'].ewm(span = 26, adjust = False).mean()
    df["MACD"] = df['EMA_12'] - df['EMA_26']
    df['signal_line'] = df['MACD'].ewm(span = 9, adjust = False).mean()

    ## Write back updated data
    lib.write(symbol, df)
    print(f"{symbol} Updated with Technical Indicators")

## Deduplication and Versioning

# Fetch and Update AAPL data to simulate updates
result = lib.read("AAPL")
existing_data = result.data

# Simulate new overlapping data
new_data = existing_data.tail(10)

# Combine and Deduplicate
combined = pd.concat([existing_data, new_data])
deduplicated_data = combined[~combined.index.duplicated(keep = 'last')]

# Write as a new version
lib.write("AAPL", deduplicated_data, prune_previous_versions=False)
print("AAPL Data Updated without Deleting Previous Version")

## Metadata and Symbol Statistics
# Fetch Metadata
# result = lib.read("AAPL")
# print("Metadata for AAPL : ", result.metadata)

# # Get Symbol Statistics
# info = lib.get_symbol_info("AAPL")
# print("Symbol Info for AAPL :", info)

## Visualization and Analysis
import matplotlib.pyplot as plt

# Plot Bollinger Bands and Closing Prices
df = lib.read('AAPL').data
plt.figure(figsize = (12,6))
plt.plot(df.index, df['Close'], label = 'Close Price')
plt.plot(df.index, df['SMA_20'], label = '20-Day SMA')
plt.fill_between(df.index, df['upper_band'], df['lower_band'], color = 'lightgray', label = 'Bollinger Bands')
plt.title("AAPL Bollinger Bands")
plt.legend()
plt.show()