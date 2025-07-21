import yfinance as yf
import mysql.connector
from mysql.connector import Error
import logging
import math

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to MySQL database
try:
    db = mysql.connector.connect(
        host='localhost',
        user='root',
        password='yashu@123',
        database='test_schema'
    )
except Error as e:
    logger.error(f"Error connecting to MySQL: {e}")
    exit()

# Function to fetch cash flow data
def get_cash_flow(symbol):
    try:
        logger.info(f"Fetching cash flow data for {symbol}...")
        stock = yf.Ticker(symbol)
        cash_flow = stock.cash_flow
        if not cash_flow.empty:
            logger.info(f"Cash flow data found for {symbol}")
            return cash_flow.to_dict()
        else:
            logger.info(f"No cash flow data available for {symbol}")
            return None
    except Exception as e:
        logger.error(f"Error fetching cash flow data for {symbol}: {e}")
        return None

# Function to fetch company name based on symbol
def get_company_name(symbol):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT name FROM companies WHERE ticker = %s", (symbol,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[0]  # Return the company name
        else:
            return None  # Handle case where symbol does not exist in database
    except Error as e:
        logger.error(f"Error fetching company name for {symbol}: {e}")
        return None

def insert_or_update_cash_flow_data(symbol, company_name, data):
    try:
        cursor = db.cursor()

        for fiscal_year, metrics in data.items():
            fiscal_year = fiscal_year.date()  # Convert to date if it's a datetime object
            fiscal_year_str = fiscal_year.strftime('%Y-%m-%d')  # Format as YYYY-MM-DD

            # List of expected columns in the database (as field names in the table)
            columns = [
                'symbol', 'company_name', 'fiscal_year', 'free_cash_flow', 'operating_cash_flow',
                'financing_cash_flow', 'investing_cash_flow', 'changes_in_cash', 'created_at', 'repayment_of_debt'
            ]

            # Filter columns to include only the ones that exist in the data
            sql_columns = ['symbol', 'company_name', 'fiscal_year']  # Always include these fields
            sql_values = [symbol, company_name, fiscal_year_str]

            # Dynamically add other fields based on what is available in the data
            for column in columns[3:]:  # Skip 'symbol', 'company_name', 'fiscal_year' which are always included
                metric_name = column.replace('_', ' ').title()  # Make the column name user-friendly for comparison
                if metric_name in metrics:
                    value = metrics.get(metric_name, 0)
                    # Check for NaN and replace with 0
                    if isinstance(value, float) and math.isnan(value):
                        value = 0
                    sql_columns.append(column)
                    sql_values.append(value)

            # Add the current timestamp for 'created_at' field (assuming it's created at insertion time)
            sql_columns.append('created_at')
            sql_values.append(fiscal_year_str)  # You could also use a timestamp here

            # Construct the SQL query dynamically
            sql_placeholders = ', '.join(['%s'] * len(sql_columns))
            sql_update = ', '.join([f"{col} = VALUES({col})" for col in sql_columns[3:]])

            sql = f"""
                INSERT INTO cash_flow ({', '.join(sql_columns)})
                VALUES ({sql_placeholders})
                ON DUPLICATE KEY UPDATE
                {sql_update}
            """

            # Execute the SQL query with the dynamic values
            cursor.execute(sql, sql_values)

        db.commit()
        logger.info(f"Successfully inserted/updated cash flow data for {symbol}")
    except Error as e:
        db.rollback()
        logger.error(f"Error inserting/updating cash flow data for {symbol}: {e}")
    finally:
        cursor.close()
# List of symbols to fetch data for
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

# Fetch and insert or update data for each symbol
for symbol in nifty_50_symbols:
    company_name = get_company_name(symbol)
    if company_name:
        cash_flow_data = get_cash_flow(symbol)
        if cash_flow_data is not None:
            insert_or_update_cash_flow_data(symbol, company_name, cash_flow_data)
        else:
            logger.warning(f"No data to save for {symbol}")
    else:
        logger.warning(f"No company name found for symbol {symbol}")

# Close the database connection
db.close()
