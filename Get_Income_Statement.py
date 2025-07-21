import yfinance as yf
import mysql.connector
from datetime import datetime
import math

def safe_float(value):
    try:
        f = float(value)
        if math.isnan(f):
            return None
        return f
    except (ValueError, TypeError):
        return None

# Connect to MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='yashu@123',
    database='test_schema'
)

# Load all company names once
def load_company_names():
    cursor = db.cursor()
    cursor.execute("SELECT ticker, name FROM companies")
    result = cursor.fetchall()
    cursor.close()
    return {ticker: name for ticker, name in result}

# Fetch income statement (quarterly & yearly)
def get_income_statement(symbol):
    try:
        stock = yf.Ticker(symbol)
        return stock.quarterly_financials.to_dict(), stock.financials.to_dict()
    except Exception as e:
        print(f"Error fetching income statement data for {symbol}: {e}")
        return None, None

# Insert or update income statement
def insert_income_statement_data(symbol, company_name, data, statement_type):
    if not company_name:
        print(f"No company name found for symbol {symbol}")
        return

    cursor = db.cursor()
    for fiscal_period, metrics in data.items():
        fiscal_period = fiscal_period.to_pydatetime().date()

        # Check if record exists
        cursor.execute(f"""
            SELECT id FROM {statement_type}_income_statement 
            WHERE company_name = %s AND fiscal_period = %s
        """, (company_name, fiscal_period))
        existing_record = cursor.fetchone()

        values = (
            safe_float(metrics.get('Total Revenue', 0)),
            safe_float(metrics.get('Gross Profit', 0)),
            safe_float(metrics.get('Operating Income', 0)),
            safe_float(metrics.get('Net Income', 0)),
            safe_float(metrics.get('EBIT', 0)),
            safe_float(metrics.get('Diluted EPS', 0)),
            safe_float(metrics.get('Interest Expense', 0)),
            safe_float(metrics.get('Research Development', 0)),
            safe_float(metrics.get('Selling General Administrative', 0)),
        )

        if existing_record:
            sql = f"""
                UPDATE {statement_type}_income_statement SET 
                    symbol = %s,
                    total_revenue = %s,
                    gross_profit = %s,
                    operating_income = %s,
                    net_income = %s,
                    ebit = %s,
                    diluted_eps = %s,
                    interest_expense = %s,
                    research_development = %s,
                    selling_general_administrative = %s
                WHERE id = %s
            """
            cursor.execute(sql, values + (existing_record[0],))
        else:
            sql = f"""
                INSERT INTO {statement_type}_income_statement 
                (symbol, company_name, fiscal_period, total_revenue, gross_profit, operating_income, net_income, 
                 ebit, diluted_eps, interest_expense, research_development, selling_general_administrative)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (symbol, company_name, fiscal_period) + values)

    db.commit()
    cursor.close()

# Nifty 50 symbols
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
    quarterly_data, yearly_data = get_income_statement(symbol)

    if quarterly_data:
        insert_income_statement_data(symbol, company_name, quarterly_data, 'quarterly')
    else:
        print(f"No quarterly data for {symbol}")
        
    if yearly_data:
        insert_income_statement_data(symbol, company_name, yearly_data, 'yearly')
    else:
        print(f"No yearly data for {symbol}")

db.close()
