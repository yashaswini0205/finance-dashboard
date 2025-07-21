import os
from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import mysql.connector
import schedule
import time
import subprocess
import threading
import plotly.graph_objects as go
from datetime import datetime
from flask import send_from_directory
from urllib.parse import quote_plus




app = Flask(__name__)
CORS(app)



# MySQL database connection setup
db_config = {
   'host': 'localhost',
   'user': 'root',
   'password': 'yashu@123',
   'database': 'test_schema',
   'autocommit': True  # Ensure autocommit is enabled
}
db = mysql.connector.connect(**db_config)

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory(static_folder, filename)





# Define paths to static and template folders
static_folder = os.path.abspath('C:\\finalproject\\Finance-Insight-Dashboard-main\\static')
template_folder = os.path.abspath('C:\\finalproject\\Finance-Insight-Dashboard-main\\templates')


# Configure Flask to serve static files
app.static_folder = static_folder
app.template_folder = template_folder




# Function to fetch historical financial data for a company
def fetch_historical_data(company_name):
   query = """
       SELECT fiscal_year, net_income_from_continuing_operations
       FROM cash_flow
       WHERE company_name = %s
       ORDER BY fiscal_year
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching historical data: {err}")
       return None




# Function to generate Plotly graph for a company
def generate_plotly_graph(company_name):
    # Fetch yearly income statement data
    data = fetch_yearly_income_statement(company_name)

    if not data:
        return None

    # Separate fiscal periods (dates) and values (net income) for plotting
    dates = [row['fiscal_period'] for row in data]
    values = [row['net_income'] for row in data]

    # Create an interactive plot using Plotly
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=values, mode='lines+markers', name=company_name))
    fig.update_layout(
        title=f'Historical Net Income - {company_name}',
        xaxis_title='Fiscal Year',
        yaxis_title='Net Income (INR)',
        xaxis_tickangle=-45,
        hovermode='x',
        template='seaborn'  # Optional: Choose a plotly template
    )

    # Define the static folder path
    static_folder = os.path.join(os.getcwd(), 'static')
    plotly_graph_path = os.path.join(static_folder, 'plotly_graph.html')

    # Save the plot as an HTML file
    try:
        fig.write_html(plotly_graph_path)
        print(f"Plotly graph saved to: {plotly_graph_path}")
    except Exception as e:
        print(f"Error saving Plotly graph: {e}")
        return None

    return plotly_graph_path


# Function to fetch yearly income statement data for a company
def fetch_yearly_income_statement(company_name):
   print(f"Fetching yearly income statement for company: {company_name}")
   query = """
       SELECT fiscal_period, total_revenue, gross_profit, operating_income, net_income
       FROM yearly_income_statement
       WHERE company_name = %s
       ORDER BY fiscal_period
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
           print(f"Fetched data: {data}")  # Log the fetched data
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching yearly income statement: {err}")
       return None




# Function to generate Plotly line chart for financial metrics
def generate_financial_metrics_chart(company_name):
   # Fetch data
   data = fetch_yearly_income_statement(company_name)


   if not data:
       print(f"No data found for company: {company_name}")
       return None


   # Separate dates and values for plotting
   dates = [row['fiscal_period'] for row in data]
   total_revenue = [row['total_revenue'] for row in data]
   gross_profit = [row['gross_profit'] for row in data]
   operating_income = [row['operating_income'] for row in data]
   net_income = [row['net_income'] for row in data]


   # Convert dates to datetime objects
   try:
       dates = [datetime.strptime(str(date), '%Y-%m-%d') for date in dates]
   except ValueError as e:
       print(f"Error converting dates: {e}")
       return None


   # Create an interactive plot using Plotly
   fig = go.Figure()
   fig.add_trace(go.Scatter(x=dates, y=total_revenue, mode='lines+markers', name='Total Revenue'))
   fig.add_trace(go.Scatter(x=dates, y=gross_profit, mode='lines+markers', name='Gross Profit'))
   fig.add_trace(go.Scatter(x=dates, y=operating_income, mode='lines+markers', name='Operating Income'))
   fig.add_trace(go.Scatter(x=dates, y=net_income, mode='lines+markers', name='Net Income'))
   fig.update_layout(
       title=f'Financial Metrics Over Time - {company_name}',
       xaxis_title='Fiscal Period',
       yaxis_title='Amount',
       xaxis_tickangle=-45,
       hovermode='x',
       template='seaborn'  # Optional: Choose a plotly template
   )


   # Use a fixed file name
   metrics_chart_path = os.path.join(static_folder, 'metrics_chart.html')


   # Save the plot as an HTML file
   try:
       fig.write_html(metrics_chart_path)
       print(f"Financial metrics chart saved to: {metrics_chart_path}")
   except Exception as e:
       print(f"Error saving financial metrics chart: {e}")
       return None


   return metrics_chart_path




# Function to fetch cash flow data for a company
def fetch_cash_flow_data(company_name):
   query = """
       SELECT fiscal_year, operating_cash_flow, investing_cash_flow, financing_cash_flow
       FROM cash_flow
       WHERE company_name = %s
       ORDER BY fiscal_year
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching cash flow data: {err}")
       return None




# Function to generate Plotly bar chart for cash flow activities
def generate_cash_flow_bar_chart(company_name):
   # Fetch data
   data = fetch_cash_flow_data(company_name)


   if not data:
       print(f"No data found for company: {company_name}")
       return None


   # Separate dates and values for plotting
   dates = [row['fiscal_year'] for row in data]
   operating_activities = [row['operating_cash_flow'] for row in data]
   investing_activities = [row['investing_cash_flow'] for row in data]
   financing_activities = [row['financing_cash_flow'] for row in data]


   # Convert dates to datetime objects
   try:
       dates = [datetime.strptime(str(date), '%Y-%m-%d') for date in dates]
   except ValueError as e:
       print(f"Error converting dates: {e}")
       return None


   # Create a bar chart using Plotly
   fig = go.Figure()
   fig.add_trace(go.Bar(x=dates, y=operating_activities, name='Operating Activities'))
   fig.add_trace(go.Bar(x=dates, y=investing_activities, name='Investing Activities'))
   fig.add_trace(go.Bar(x=dates, y=financing_activities, name='Financing Activities'))
   fig.update_layout(
       barmode='group',
       title=f'Cash Flow Activities - {company_name}',
       xaxis_title='Fiscal Year',
       yaxis_title='Amount',
       xaxis_tickangle=-45,
       hovermode='x',
       template='seaborn'  # Optional: Choose a plotly template
   )


   # Use a fixed file name
   cash_flow_chart_path = os.path.join(static_folder, 'cash_flow_chart.html')


   # Save the plot as an HTML file
   try:
       fig.write_html(cash_flow_chart_path)
       print(f"Cash flow bar chart saved to: {cash_flow_chart_path}")
   except Exception as e:
       print(f"Error saving cash flow bar chart: {e}")
       return None


   return cash_flow_chart_path


# Function to fetch balance sheet data for a company
def fetch_balance_sheet_data(company_name):
   query = """
       SELECT fiscal_year, total_assets, total_liabilities, total_equity
       FROM balance_sheet
       WHERE company_name = %s
       ORDER BY fiscal_year
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching balance sheet data: {err}")
       return None


# Function to generate Plotly stacked bar chart for balance sheet
def generate_balance_sheet_chart(company_name):
   # Fetch data
   data = fetch_balance_sheet_data(company_name)


   if not data:
       print(f"No data found for company: {company_name}")
       return None


   # Separate dates and values for plotting
   dates = [str(row['fiscal_year']) for row in data]  # Convert year to string
   total_assets = [row['total_assets'] for row in data]
   total_liabilities = [row['total_liabilities'] for row in data]
   total_equity = [row['total_equity'] for row in data]


   # Convert dates to datetime objects (for visualization purpose, can be skipped if not using dates)
   try:
       dates = [datetime.strptime(date, '%Y') for date in dates]  # Parse year directly
   except ValueError as e:
       print(f"Error converting dates: {e}")
       return None


   # Create a stacked bar chart using Plotly
   fig = go.Figure()
   fig.add_trace(go.Bar(x=dates, y=total_assets, name='Total Assets'))
   fig.add_trace(go.Bar(x=dates, y=total_liabilities, name='Total Liabilities'))
   fig.add_trace(go.Bar(x=dates, y=total_equity, name='Total Equity'))


   fig.update_layout(
       barmode='stack',
       title=f'Balance Sheet Composition Over Time - {company_name}',
       xaxis_title='Fiscal Year',
       yaxis_title='Amount',
       xaxis_tickangle=-45,
       hovermode='x',
       template='seaborn'  # Optional: Choose a plotly template
   )


   # Use a fixed file name
   balance_sheet_chart_path = os.path.join(static_folder, 'balance_sheet_chart.html')


   # Save the plot as an HTML file
   try:
       fig.write_html(balance_sheet_chart_path)
       print(f"Balance sheet stacked bar chart saved to: {balance_sheet_chart_path}")
   except Exception as e:
       print(f"Error saving balance sheet stacked bar chart: {e}")
       return None


   return balance_sheet_chart_path






# Flask route to search for companies
@app.route('/search')
def search():
   search_query = request.args.get('q')
   if not search_query:
       return jsonify({'error': 'Missing search query'}), 400


   query = "SELECT * FROM companies WHERE name LIKE %s"
   with db.cursor(dictionary=True) as cursor:
       cursor.execute(query, ('%' + search_query + '%',))
       results = cursor.fetchall()


   return jsonify(results)




# Flask route for index
@app.route('/')
def index():
   return render_template('index.html')




# Flask route for dashboard
@app.route('/dashboard')
def dashboard():
   company_name = request.args.get("company")


   if not company_name:
       return "Company name is required", 400


   cursor = db.cursor(dictionary=True)


   # Fetch current stock data
   try:
       cursor.execute("SELECT * FROM CurrentStockData WHERE company_name = %s", (company_name,))
       company_data = cursor.fetchone()
       print("Current Stock Data:", company_data)  # Print to verify
   except mysql.connector.Error as err:
       print(f"Error fetching current stock data: {err}")


   # Fetch historical stock data
   try:
       cursor.execute("SELECT * FROM HistoricalStockData WHERE company_name = %s", (company_name,))
       historical_data = cursor.fetchall()
       print(historical_data)
   except mysql.connector.Error as err:
       print(f"Error fetching historical stock data: {err}")
   historical_data = historical_data[:20]


   # Fetch quarterly income statement
   cursor.execute("SELECT * FROM quarterly_income_statement WHERE company_name = %s", (company_name,))
   quarterly_results = cursor.fetchall()


   # Fetch yearly income statement
   cursor.execute("SELECT * FROM yearly_income_statement WHERE company_name = %s", (company_name,))
   income_statement = cursor.fetchall()


   # Fetch balance sheet data
   cursor.execute("SELECT * FROM balance_sheet WHERE company_name = %s", (company_name,))
   balance_sheet = cursor.fetchall()


   # Fetch cash flow data
   cursor.execute("SELECT * FROM cash_flow WHERE company_name = %s", (company_name,))
   cash_flow = cursor.fetchall()


   cursor.close()


   # Prepare the data
   data = {
       'company_data': company_data,
       'historical_data': historical_data,
       'quarterly_results': quarterly_results,
       'income_statement': income_statement,
       'balance_sheet': balance_sheet,
       'cash_flow': cash_flow
   }


   # Fetch historical financial data and generate Plotly graph
   plotly_graph_path = generate_plotly_graph(company_name)
   metrics_chart_path = generate_financial_metrics_chart(company_name)
   cash_flow_chart_path = generate_cash_flow_bar_chart(company_name)
   balance_sheet_chart_path = generate_balance_sheet_chart(company_name)


   return render_template('dashboard.html', data=data, company_name=company_name,
                          plotly_graph_path=plotly_graph_path, metrics_chart_path=metrics_chart_path,
                          cash_flow_chart_path=cash_flow_chart_path,balance_sheet_chart_path=balance_sheet_chart_path)




# Scheduler tasks
def run_task(script_name):
   try:
       subprocess.run(["python", script_name], check=True)
   except subprocess.CalledProcessError as e:
       print(f"Error running {script_name}: {e}")




def run_task1():
   run_task("Get_Historical_Current_Data.py")




def run_task2():
   run_task("Get_Balance_Sheet.py")




def run_task3():
   run_task("Get_CashFlow.py")




def run_task4():
   run_task("Get_Income_Statement.py")




def run_task5():
   run_task("Get_AssetProfile.py")




# Function to schedule tasks
def schedule_tasks():
   schedule.every(35).minutes.do(run_task1)
   schedule.every(35).minutes.do(run_task3)
   schedule.every(35).minutes.do(run_task4)
   schedule.every(35).minutes.do(run_task2)
   schedule.every(35).minutes.do(run_task5)


   while True:
       schedule.run_pending()
       time.sleep(1)




# Function to start scheduler in a separate thread
def start_scheduler():
   scheduler_thread = threading.Thread(target=schedule_tasks)
   scheduler_thread.daemon = True
   scheduler_thread.start()




if __name__ == '__main__':
   # Start the scheduler
   start_scheduler()


   # Run the Flask app
   app.run(debug=True)
{
   'user': 'root',
   'password': 'yashu@123',
   'database': 'test_schema',
   'autocommit': True  # Ensure autocommit is enabled
}
db = mysql.connector.connect(**db_config)
@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory(static_folder, filename)
# Define paths to static and template folders
static_folder = os.path.abspath('C:\\finalproject\\Finance-Insight-Dashboard-main\\static')
template_folder = os.path.abspath('C:\\finalproject\\Finance-Insight-Dashboard-main\\templates')
# Configure Flask to serve static files
app.static_folder = static_folder
app.template_folder = template_folder
# Function to fetch historical financial data for a company
def fetch_historical_data(company_name):
   query = """
       SELECT fiscal_year, net_income_from_continuing_operations
       FROM cash_flow
       WHERE company_name = %s
       ORDER BY fiscal_year
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching historical data: {err}")
       return None
# Function to generate Plotly graph for a company
def generate_plotly_graph(company_name):
    # Fetch yearly income statement data
    data = fetch_yearly_income_statement(company_name)
    if not data:
        return None
    # Separate fiscal periods (dates) and values (net income) for plotting
    dates = [row['fiscal_period'] for row in data]
    values = [row['net_income'] for row in data]
    # Create an interactive plot using Plotly
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=values, mode='lines+markers', name=company_name))
    fig.update_layout(
        title=f'Historical Net Income - {company_name}',
        xaxis_title='Fiscal Year',
        yaxis_title='Net Income (INR)',
        xaxis_tickangle=-45,
        hovermode='x',
        template='seaborn'  # Optional: Choose a plotly template
    )
    # Define the static folder path
    static_folder = os.path.join(os.getcwd(), 'static')
    plotly_graph_path = os.path.join(static_folder, 'plotly_graph.html')
    # Save the plot as an HTML file
    try:
        fig.write_html(plotly_graph_path)
        print(f"Plotly graph saved to: {plotly_graph_path}")
    except Exception as e:
        print(f"Error saving Plotly graph: {e}")
        return None
    return plotly_graph_path
# Function to fetch yearly income statement data for a company
def fetch_yearly_income_statement(company_name):
   print(f"Fetching yearly income statement for company: {company_name}")
   query = """
       SELECT fiscal_period, total_revenue, gross_profit, operating_income, net_income
       FROM yearly_income_statement
       WHERE company_name = %s
       ORDER BY fiscal_period
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
           print(f"Fetched data: {data}")  # Log the fetched data
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching yearly income statement: {err}")
       return None
# Function to generate Plotly line chart for financial metrics
def generate_financial_metrics_chart(company_name):
   # Fetch data
   data = fetch_yearly_income_statement(company_name)
   if not data:
       print(f"No data found for company: {company_name}")
       return None
   # Separate dates and values for plotting
   dates = [row['fiscal_period'] for row in data]
   total_revenue = [row['total_revenue'] for row in data]
   gross_profit = [row['gross_profit'] for row in data]
   operating_income = [row['operating_income'] for row in data]
   net_income = [row['net_income'] for row in data]
   # Convert dates to datetime objects
   try:
       dates = [datetime.strptime(str(date), '%Y-%m-%d') for date in dates]
   except ValueError as e:
       print(f"Error converting dates: {e}")
       return None
   # Create an interactive plot using Plotly
   fig = go.Figure()
   fig.add_trace(go.Scatter(x=dates, y=total_revenue, mode='lines+markers', name='Total Revenue'))
   fig.add_trace(go.Scatter(x=dates, y=gross_profit, mode='lines+markers', name='Gross Profit'))
   fig.add_trace(go.Scatter(x=dates, y=operating_income, mode='lines+markers', name='Operating Income'))
   fig.add_trace(go.Scatter(x=dates, y=net_income, mode='lines+markers', name='Net Income'))
   fig.update_layout(
       title=f'Financial Metrics Over Time - {company_name}',
       xaxis_title='Fiscal Period',
       yaxis_title='Amount',
       xaxis_tickangle=-45,
       hovermode='x',
       template='seaborn'  # Optional: Choose a plotly template
   )
   # Use a fixed file name
   metrics_chart_path = os.path.join(static_folder, 'metrics_chart.html')
   # Save the plot as an HTML file
   try:
       fig.write_html(metrics_chart_path)
       print(f"Financial metrics chart saved to: {metrics_chart_path}")
   except Exception as e:
       print(f"Error saving financial metrics chart: {e}")
       return None
   return metrics_chart_path
# Function to fetch cash flow data for a company
def fetch_cash_flow_data(company_name):
   query = """
       SELECT fiscal_year, operating_cash_flow, investing_cash_flow, financing_cash_flow
       FROM cash_flow
       WHERE company_name = %s
       ORDER BY fiscal_year
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching cash flow data: {err}")
       return None
# Function to generate Plotly bar chart for cash flow activities
def generate_cash_flow_bar_chart(company_name):
   # Fetch data
   data = fetch_cash_flow_data(company_name)
   if not data:
       print(f"No data found for company: {company_name}")
       return None
   # Separate dates and values for plotting
   dates = [row['fiscal_year'] for row in data]
   operating_activities = [row['operating_cash_flow'] for row in data]
   investing_activities = [row['investing_cash_flow'] for row in data]
   financing_activities = [row['financing_cash_flow'] for row in data]
   # Convert dates to datetime objects
   try:
       dates = [datetime.strptime(str(date), '%Y-%m-%d') for date in dates]
   except ValueError as e:
       print(f"Error converting dates: {e}")
       return None
   # Create a bar chart using Plotly
   fig = go.Figure()
   fig.add_trace(go.Bar(x=dates, y=operating_activities, name='Operating Activities'))
   fig.add_trace(go.Bar(x=dates, y=investing_activities, name='Investing Activities'))
   fig.add_trace(go.Bar(x=dates, y=financing_activities, name='Financing Activities'))
   fig.update_layout(
       barmode='group',
       title=f'Cash Flow Activities - {company_name}',
       xaxis_title='Fiscal Year',
       yaxis_title='Amount',
       xaxis_tickangle=-45,
       hovermode='x',
       template='seaborn'  # Optional: Choose a plotly template
   )
   # Use a fixed file name
   cash_flow_chart_path = os.path.join(static_folder, 'cash_flow_chart.html')
   # Save the plot as an HTML file
   try:
       fig.write_html(cash_flow_chart_path)
       print(f"Cash flow bar chart saved to: {cash_flow_chart_path}")
   except Exception as e:
       print(f"Error saving cash flow bar chart: {e}")
       return None
   return cash_flow_chart_path
# Function to fetch balance sheet data for a company
def fetch_balance_sheet_data(company_name):
   query = """
       SELECT fiscal_year, total_assets, total_liabilities, total_equity
       FROM balance_sheet
       WHERE company_name = %s
       ORDER BY fiscal_year
   """
   try:
       with db.cursor(dictionary=True) as cursor:
           cursor.execute(query, (company_name,))
           data = cursor.fetchall()
       return data
   except mysql.connector.Error as err:
       print(f"Error fetching balance sheet data: {err}")
       return None
# Function to generate Plotly stacked bar chart for balance sheet
def generate_balance_sheet_chart(company_name):
   # Fetch data
   data = fetch_balance_sheet_data(company_name)
   if not data:
       print(f"No data found for company: {company_name}")
       return None
   # Separate dates and values for plotting
   dates = [str(row['fiscal_year']) for row in data]  # Convert year to string
   total_assets = [row['total_assets'] for row in data]
   total_liabilities = [row['total_liabilities'] for row in data]
   total_equity = [row['total_equity'] for row in data]
   # Convert dates to datetime objects (for visualization purpose, can be skipped if not using dates)
   try:
       dates = [datetime.strptime(date, '%Y') for date in dates]  # Parse year directly
   except ValueError as e:
       print(f"Error converting dates: {e}")
       return None
   # Create a stacked bar chart using Plotly
   fig = go.Figure()
   fig.add_trace(go.Bar(x=dates, y=total_assets, name='Total Assets'))
   fig.add_trace(go.Bar(x=dates, y=total_liabilities, name='Total Liabilities'))
   fig.add_trace(go.Bar(x=dates, y=total_equity, name='Total Equity'))
   fig.update_layout(
       barmode='stack',
       title=f'Balance Sheet Composition Over Time - {company_name}',
       xaxis_title='Fiscal Year',
       yaxis_title='Amount',
       xaxis_tickangle=-45,
       hovermode='x',
       template='seaborn'  # Optional: Choose a plotly template
   )
   # Use a fixed file name
   balance_sheet_chart_path = os.path.join(static_folder, 'balance_sheet_chart.html')
   # Save the plot as an HTML file
   try:
       fig.write_html(balance_sheet_chart_path)
       print(f"Balance sheet stacked bar chart saved to: {balance_sheet_chart_path}")
   except Exception as e:
       print(f"Error saving balance sheet stacked bar chart: {e}")
       return None
   return balance_sheet_chart_path
# Flask route to search for companies
@app.route('/search')
def search():
   search_query = request.args.get('q')
   if not search_query:
       return jsonify({'error': 'Missing search query'}), 400
   query = "SELECT * FROM companies WHERE name LIKE %s"
   with db.cursor(dictionary=True) as cursor:
       cursor.execute(query, ('%' + search_query + '%',))
       results = cursor.fetchall()
   return jsonify(results)
# Flask route for index
@app.route('/')
def index():
   return render_template('index.html')
# Flask route for dashboard
@app.route('/dashboard')
def dashboard():
   company_name = request.args.get("company")
   if not company_name:
       return "Company name is required", 400
   cursor = db.cursor(dictionary=True)
   # Fetch current stock data
   try:
       cursor.execute("SELECT * FROM CurrentStockData WHERE company_name = %s", (company_name,))
       company_data = cursor.fetchone()
       print("Current Stock Data:", company_data)  # Print to verify
   except mysql.connector.Error as err:
       print(f"Error fetching current stock data: {err}")
   # Fetch historical stock data
   try:
       cursor.execute("SELECT * FROM HistoricalStockData WHERE company_name = %s", (company_name,))
       historical_data = cursor.fetchall()
       print(historical_data)
   except mysql.connector.Error as err:
       print(f"Error fetching historical stock data: {err}")
   historical_data = historical_data[:20]
   # Fetch quarterly income statement
   cursor.execute("SELECT * FROM quarterly_income_statement WHERE company_name = %s", (company_name,))
   quarterly_results = cursor.fetchall()
   # Fetch yearly income statement
   cursor.execute("SELECT * FROM yearly_income_statement WHERE company_name = %s", (company_name,))
   income_statement = cursor.fetchall()
   # Fetch balance sheet data
   cursor.execute("SELECT * FROM balance_sheet WHERE company_name = %s", (company_name,))
   balance_sheet = cursor.fetchall()
   # Fetch cash flow data
   cursor.execute("SELECT * FROM cash_flow WHERE company_name = %s", (company_name,))
   cash_flow = cursor.fetchall()
   cursor.close()
   # Prepare the data
   data = {
       'company_data': company_data,
       'historical_data': historical_data,
       'quarterly_results': quarterly_results,
       'income_statement': income_statement,
       'balance_sheet': balance_sheet,
       'cash_flow': cash_flow
   }
   # Fetch historical financial data and generate Plotly graph
   plotly_graph_path = generate_plotly_graph(company_name)
   metrics_chart_path = generate_financial_metrics_chart(company_name)
   cash_flow_chart_path = generate_cash_flow_bar_chart(company_name)
   balance_sheet_chart_path = generate_balance_sheet_chart(company_name)
   return render_template('dashboard.html', data=data, company_name=company_name,
                          plotly_graph_path=plotly_graph_path, metrics_chart_path=metrics_chart_path,
                          cash_flow_chart_path=cash_flow_chart_path,balance_sheet_chart_path=balance_sheet_chart_path)
# Scheduler tasks
def run_task(script_name):
   try:
       subprocess.run(["python", script_name], check=True)
   except subprocess.CalledProcessError as e:
       print(f"Error running {script_name}: {e}")
def run_task1():
   run_task("Get_Historical_Current_Data.py")
def run_task2():
   run_task("Get_Balance_Sheet.py")
def run_task3():
   run_task("Get_CashFlow.py")
def run_task4():
   run_task("Get_Income_Statement.py")
def run_task5():
   run_task("Get_AssetProfile.py")
# Function to schedule tasks
def schedule_tasks():
   schedule.every(35).minutes.do(run_task1)
   schedule.every(35).minutes.do(run_task3)
   schedule.every(35).minutes.do(run_task4)
   schedule.every(35).minutes.do(run_task2)
   schedule.every(35).minutes.do(run_task5)
while True:
       schedule.run_pending()
       time.sleep(1)
# Function to start scheduler in a separate thread
def start_scheduler():
   scheduler_thread = threading.Thread(target=schedule_tasks)
   scheduler_thread.daemon = True
   scheduler_thread.start()
if __name__ == '__main__':
   # Start the scheduler
   start_scheduler()
   # Run the Flask app
   app.run(debug=True)
