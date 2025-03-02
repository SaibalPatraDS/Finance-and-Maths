## Download Stock Data Using Yfinance
import yfinance as yf
import pandas as pd

## Fetch Historical Data for Google
ticker = yf.Ticker("MSFT")
hist_data = ticker.history(
    period = '5y',
    interval = '1d',
    auto_adjust = False
)
# print(hist_data.shape)

## Store Data in Arctic DB
from arcticdb import Arctic

DB_PATH = "lmdb://./stock_db"
LIBRARY_NAME = 'financial_data'

## Connect to ArcticDB and Create a library
ac = Arctic(DB_PATH)
if LIBRARY_NAME not in ac.list_libraries():
    ac.create_library(LIBRARY_NAME)

lib = ac[LIBRARY_NAME]

## Storing Data to ArcticDB
lib.write("AAPL", hist_data)
# print("Data Written to ArcticDB")

## Read and Analyze Data from ArcticDB
# Read Data from ArcticDB
result = lib.read("AAPL")
stock_df = result.data

# Performing Simple Moving Average (SMA)
stock_df['SMA_50'] = stock_df['Close'].rolling(window = 50).mean()

# Plotting the stock price with SMA
print(stock_df[['Close', 'SMA_50']].plot(
    title = 'AAPL Stock Price with 50-day SMA'
))

## Update and Deduplicate the Data
new_data = ticker.history(period = '1mo', auto_adjust = False)
combined = pd.concat([result.data, new_data])
filtered = combined[~combined.index.duplicated(keep='last')]

lib.write("AAPL", filtered.sort_index())
print("Data Updated")


