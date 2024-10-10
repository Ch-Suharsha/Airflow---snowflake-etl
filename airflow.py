from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests

# Function to establish Snowflake connection using Airflow connection ID
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='hw-5')  # Use your Airflow Snowflake connection ID
    return hook.get_conn().cursor()

# Task to fetch stock data from Alpha Vantage API
@task
def fetch_stock_data(symbol="AAPL"):
    api_key = Variable.get("alpha_vantage_api_key")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    # Filtering data for the last 90 days
    results = []
    today = datetime.now()
    ninety_days_ago = today - timedelta(days=90)

    for date_str, values in data.get("Time Series (Daily)", {}).items():
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        if date_obj >= ninety_days_ago:
            record = {
                'date': date_str,
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': float(values['5. volume'])
            }
            results.append(record)

    return results

# Task to load stock data into Snowflake
@task
def load_data_to_snowflake(stock_data):
    cur = return_snowflake_conn()

    try:
        # Set the database and schema context
        cur.execute("USE DATABASE your_database_name;")  # Replace with your database name
        cur.execute("USE SCHEMA raw_data;")  # Ensure this is your schema

        # Create or replace the stock prices table in Snowflake
        cur.execute("""
        CREATE OR REPLACE TABLE raw_data.stock_prices (
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT
);
        """)

        # Insert each record into the table
        for record in stock_data:
            cur.execute("""
            INSERT INTO stock_prices (date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s);
            """, (record['date'], record['open'], record['high'], record['low'], record['close'], record['volume']))

        # Commit transaction
        cur.connection.commit()
    except Exception as e:
        print(f"Error occurred while loading data: {e}")
        cur.connection.rollback()  # Rollback on error
    finally:
        cur.close()

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch stock data from API and load into Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Fetch the symbol from Airflow variables, default to "AAPL"
    symbol = Variable.get("stock_symbol", default_var='AAPL')

    # Define tasks using the @task decorator
    stock_data = fetch_stock_data(symbol)
    load_data_to_snowflake(stock_data)
