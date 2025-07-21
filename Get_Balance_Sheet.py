import yfinance as yf
import mysql.connector
import math

# Connect to MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='yashu@123',
    database='test_schema'
)
# Function to fetch balance sheet data
def get_balance_sheet(symbol):
    try:
        stock = yf.Ticker(symbol)
        balance_sheet = stock.balance_sheet
        if not balance_sheet.empty:
            return balance_sheet.to_dict()
        else:
            print(f"No balance sheet data available for {symbol}")
            return None
    except Exception as e:
        print(f"Error fetching balance sheet data for {symbol}: {e}")
        return None

def load_company_names():
    cursor = db.cursor()
    cursor.execute("SELECT ticker, name FROM companies")
    result = cursor.fetchall()
    cursor.close()
    return {ticker: name for ticker, name in result}

# Function to insert or update balance sheet data into the database
def insert_or_update_balance_sheet_data(symbol, company_name, data):
    cursor = db.cursor()
    for fiscal_year, metrics in data.items():
        fiscal_year = fiscal_year.year  # Directly extract the year from the Timestamp
        
        def safe_float(value):
            try:
                f = float(value)
                if math.isnan(f):
                    return None
                return f
            except (ValueError, TypeError):
                return None

        sql = """
            INSERT INTO balance_sheet 
            (symbol, company_name, fiscal_year, total_assets, total_liabilities, total_equity, current_assets, 
            current_liabilities, long_term_debt, short_term_debt, retained_earnings, net_income, 
            total_revenue, operating_income, net_cash_provided_by_operating_activities, 
            net_cash_used_for_investing_activities, net_cash_used_for_financing_activities)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            company_name = VALUES(company_name),
            total_assets = VALUES(total_assets),
            total_liabilities = VALUES(total_liabilities),
            total_equity = VALUES(total_equity),
            current_assets = VALUES(current_assets),
            current_liabilities = VALUES(current_liabilities),
            long_term_debt = VALUES(long_term_debt),
            short_term_debt = VALUES(short_term_debt),
            retained_earnings = VALUES(retained_earnings),
            net_income = VALUES(net_income),
            total_revenue = VALUES(total_revenue),
            operating_income = VALUES(operating_income),
            net_cash_provided_by_operating_activities = VALUES(net_cash_provided_by_operating_activities),
            net_cash_used_for_investing_activities = VALUES(net_cash_used_for_investing_activities),
            net_cash_used_for_financing_activities = VALUES(net_cash_used_for_financing_activities)
        """

        values = (
            symbol, company_name, fiscal_year,
            safe_float(metrics.get('Total Assets')),
            safe_float(metrics.get('Total Liabilities Net Minority Interest')),
            safe_float(metrics.get('Total Equity Gross Minority Interest')),
            safe_float(metrics.get('Total Current Assets')),
            safe_float(metrics.get('Total Current Liabilities Net')),
            safe_float(metrics.get('Long Term Debt')),
            safe_float(metrics.get('Short Term Debt / Current Portion of Long Term Debt')),
            safe_float(metrics.get('Retained Earnings (Accumulated Deficit)')),
            safe_float(metrics.get('Net Income')),
            safe_float(metrics.get('Total Revenue')),
            safe_float(metrics.get('Operating Income')),
            safe_float(metrics.get('Net Cash from Operating Activities')),
            safe_float(metrics.get('Net Cash from Investing Activities')),
            safe_float(metrics.get('Net Cash from Financing Activities'))
        )
        cursor.execute(sql, values)
    db.commit()
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

company_names = load_company_names()

for symbol in nifty_50_symbols:
    company_name = company_names.get(symbol)
    if company_name:
        balance_sheet_data = get_balance_sheet(symbol)
        if balance_sheet_data is not None:
            insert_or_update_balance_sheet_data(symbol, company_name, balance_sheet_data)
        else:
            print(f"No data to save for {symbol}")
    else:
        print(f"No company name found for symbol {symbol}")

# Close the database connection
db.close()