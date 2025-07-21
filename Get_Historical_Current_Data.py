import yfinance as yf
import pandas as pd
import mysql.connector
import numpy as np

# Function to fetch stock data
def get_stock_data(symbol):
    try:
        stock = yf.Ticker(symbol)

        # Get historical data
        historical_data = stock.history(period="1mo")

        # Get current data
        current_data = stock.info

        if not historical_data.empty:
            # Add 'symbol' column to historical data
            historical_data['symbol'] = symbol

            # Reset index and rename 'Date' column to match MySQL table schema
            historical_data.reset_index(inplace=True)
            historical_data.rename(columns={'Date': 'date_time'}, inplace=True)

            # Reorder columns to match MySQL table schema and exclude 'Dividends' and 'Stock Splits'
            historical_data = historical_data[['date_time', 'symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]

            # Convert current data to a DataFrame
            current_data_df = pd.DataFrame({
                'symbol': [symbol],
                'marketCap': [current_data.get('marketCap', None)],
                'forwardPE': [current_data.get('forwardPE', None)],
                'trailingPE': [current_data.get('trailingPE', None)],
                'dayHigh': [current_data.get('dayHigh', None)],
                'dayLow': [current_data.get('dayLow', None)],
                'fiftyTwoWeekHigh': [current_data.get('fiftyTwoWeekHigh', None)],
                'fiftyTwoWeekLow': [current_data.get('fiftyTwoWeekLow', None)],
                'dividendYield': [current_data.get('dividendYield', None)],
                'beta': [current_data.get('beta', None)],
                'primaryExchange': [current_data.get('primaryExchange', None)],
                'currency': [current_data.get('currency', None)]
            })

            return historical_data, current_data_df
        else:
            print(f"No historical data available for {symbol}")
            return None, None
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None, None

# Function to fetch company name based on symbol
def get_company_name(symbol, db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT name FROM companies WHERE ticker = %s", (symbol,))
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0]  # Return the company name
    else:
        return None  # Handle case where symbol does not exist in database

# MySQL database connection details
db_user = 'root'
db_password = 'yashu@123'
db_host = 'localhost'  # or your MySQL server IP/hostname
db_name = 'test_schema'

# Establish MySQL connection using mysql.connector for company name retrieval
db_connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

nifty_50_symbols = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "HDFC.NS",
    "ICICIBANK.NS", "KOTAKBANK.NS", "HINDUNILVR.NS", "SBIN.NS", "BAJFINANCE.NS",
    "BHARTIARTL.NS", "ASIANPAINT.NS", "ITC.NS", "AXISBANK.NS", "LT.NS",
    "DMART.NS", "SUNPHARMA.NS", "ULTRACEMCO.NS", "TITAN.NS", "NESTLEIND.NS",
    "WIPRO.NS", "MARUTI.NS", "M&M.NS", "HCLTECH.NS", "NTPC.NS",
    "TECHM.NS", "POWERGRID.NS", "TATAMOTORS.NS", "INDUSINDBK.NS", "SBILIFE.NS",
    "TATASTEEL.NS", "GRASIM.NS", "BAJAJFINSV.NS", "ADANIGREEN.NS", "CIPLA.NS",
    "ONGC.NS", "HDFCLIFE.NS", "BPCL.NS", "JSWSTEEL.NS", "COALINDIA.NS",
    "BRITANNIA.NS", "HEROMOTOCO.NS", "SHREECEM.NS", "DABUR.NS", "ADANIPORTS.NS",
    "EICHERMOT.NS", "DIVISLAB.NS", "HINDALCO.NS", "UPL.NS", "APOLLOHOSP.NS"
]

def to_py(value):
    if isinstance(value, (np.int64, np.float64)):
        return value.item()  # Convert numpy types to native Python types (int or float)
    return value

# Process each symbol
for symbol in nifty_50_symbols:
    historical_data, current_data = get_stock_data(symbol)
    if historical_data is not None and current_data is not None:
        try:
            company_name = get_company_name(symbol, db_connection)
            if company_name:
                # Add company name to current_data
                current_data['company_name'] = company_name
                row = current_data.iloc[0]  # Extract the single row

                cursor = db_connection.cursor()

                # Delete old current data if present
                cursor.execute(
                    "SELECT COUNT(*) FROM CurrentStockData WHERE symbol = %s",
                    (row['symbol'],)
                )
                if cursor.fetchone()[0] > 0:
                    cursor.execute(
                        "DELETE FROM CurrentStockData WHERE symbol = %s",
                        (row['symbol'],)
                    )
                    db_connection.commit()

                record = {
                'symbol': to_py(row['symbol']),
                'company_name': to_py(row['company_name']),
                'marketCap': to_py(row['marketCap']),
                'forwardPE': to_py(row['forwardPE']),
                'trailingPE': to_py(row['trailingPE']),
                'dayHigh': to_py(row['dayHigh']),
                'dayLow': to_py(row['dayLow']),
                'fiftyTwoWeekHigh': to_py(row['fiftyTwoWeekHigh']),
                'fiftyTwoWeekLow': to_py(row['fiftyTwoWeekLow']),
                'dividendYield': to_py(row['dividendYield']),
                'beta': to_py(row['beta']),
                'primaryExchange': to_py(row['primaryExchange']),
                'currency': to_py(row['currency'])
            }
                # Insert the new current data
                cursor.execute("""
                    INSERT INTO CurrentStockData 
                      (symbol, company_name, marketCap, forwardPE, trailingPE,
                       dayHigh, dayLow, fiftyTwoWeekHigh, fiftyTwoWeekLow,
                       dividendYield, beta, primaryExchange, currency)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record['symbol'],
                    record['company_name'],
                    record['marketCap'],
                    record['forwardPE'],
                    record['trailingPE'],
                    record['dayHigh'],
                    record['dayLow'],
                    record['fiftyTwoWeekHigh'],
                    record['fiftyTwoWeekLow'],
                    record['dividendYield'],
                    record['beta'],
                    record['primaryExchange'],
                    record['currency'],
                ))
                db_connection.commit()
                print(f"Current stock data saved in MySQL for {symbol}")
            else:
                print(f"No company name found for symbol {symbol}")
        except Exception as e:
            print(f"Error saving data to MySQL for {symbol}: {e}")
    else:
        print(f"Failed to retrieve stock data for {symbol}")

# Close the database connections
db_connection.close()
