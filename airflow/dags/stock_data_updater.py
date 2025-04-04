from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import yfinance as yf
import time
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def get_db_connection():
    """Create and return a PostgreSQL connection using Airflow connection"""
    postgres_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    return postgres_hook.get_conn()

def get_symbols_from_db():
    """Fetch all symbols from database"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM companies")
    symbols = [row[0] for row in cur.fetchall()]
    conn.close()
    return symbols

def get_max_dates():
    """Get max date for each symbol from database"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT symbol, MAX(date) FROM stock_data GROUP BY symbol")
    # Convert dates to strings for XCom serialization
    results = {row[0]: row[1].isoformat() for row in cur.fetchall()}
    conn.close()
    return results

def update_symbol_data(symbol, **context):
    """Fetch and update data for a single symbol with rate limiting"""
    # Get delay between API calls from Airflow Variable (default 1 second)
    api_delay = float(Variable.get("YFINANCE_API_DELAY", default_var=1.0))
    
    # Add delay if this isn't the first symbol being processed
    task_instance = context['task_instance']
    if task_instance.task_id != f"update_{symbol}":  # Only delay for subsequent tasks
        time.sleep(api_delay)
    
    max_dates = context['ti'].xcom_pull(task_ids='get_max_dates')
    
    # Parse the date string back to datetime
    last_date = datetime.fromisoformat(max_dates.get(symbol)) if symbol in max_dates else datetime(1900, 1, 1)

    try:
        # Fetch new data from Yahoo Finance
        df = yf.download(
            symbol,
            start=last_date + timedelta(days=1),  # Start from day after last known
            end=datetime.today(),
            progress=False
        )
        
        if df.empty:
            return
        
        # Prepare data for insertion
        df = df.reset_index()
        df['symbol'] = symbol
        data = df[['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']].values.tolist()
        
        # Upsert into database
        conn = get_db_connection()
        cur = conn.cursor()
        insert_sql = """
        INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
        cur.executemany(insert_sql, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

with DAG(
    'stock_data_updater_v1',
    default_args=default_args,
    schedule_interval='0 18 * * MON-FRI',  # After market close
    start_date=datetime(2025, 4, 1),
    catchup=False,
    max_active_runs=1,  # Prevent parallel runs to help with rate limiting
) as dag:
    
    get_max_dates_task = PythonOperator(
        task_id='get_max_dates',
        python_callable=get_max_dates,
    )
    
    update_tasks = []
    for symbol in get_symbols_from_db():
        task = PythonOperator(
            task_id=f'update_{symbol}',
            python_callable=update_symbol_data,
            op_kwargs={'symbol': symbol},
            provide_context=True,
        )
        update_tasks.append(task)
    
    get_max_dates_task >> update_tasks
