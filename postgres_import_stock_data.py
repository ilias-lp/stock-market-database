import os
import pandas as pd
from sqlalchemy import create_engine, text
from decimal import Decimal, InvalidOperation

def value_convert(value, field_type):
    """Value conversion"""
    if pd.isna(value) or value is None:
        return None
    
    try:
        if field_type == 'price':
            # Convert to string first to avoid floating point quirks
            str_val = f"{float(value):.8f}"  # Extra precision for rounding
            dec = Decimal(str_val).quantize(Decimal('0.0001'))
            if not (-10**8 < dec < 10**8):  # Check reasonable stock price range
                return None
            return dec
            
        elif field_type == 'volume':
            return int(round(float(value)))
            
    except (ValueError, TypeError, InvalidOperation, OverflowError):
        return None
    return None

def import_with_retry():
    engine = create_engine('postgresql://admin:password@localhost:5432/stockmarket',
                         isolation_level="READ COMMITTED",
                         pool_pre_ping=True)
    
    csv_files = [f for f in os.listdir('./stock_data/') if f.endswith('_historical.csv')]
    
    for filename in csv_files:
        filepath = os.path.join('./stock_data/', filename)
        symbol = filename.split('_historical.csv')[0]
        
        try:
            df = pd.read_csv(filepath)
            df.columns = df.columns.str.lower()
            df['symbol'] = symbol
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
            
            # Convert all values with extreme safety
            for col in ['open', 'high', 'low', 'close']:
                df[col] = df[col].apply(lambda x: value_convert(x, 'price'))
            df['volume'] = df['volume'].apply(lambda x: value_convert(x, 'volume'))
            
            # Remove invalid rows
            df = df[~df[['open', 'high', 'low', 'close']].isnull().all(axis=1)]
            df = df[df['date'].notna()]
            
            # Process single rows individually with transaction per row
            for _, row in df.iterrows():
                with engine.begin() as conn:  # New transaction per row
                    try:
                        conn.execute(
                            text("""
                            INSERT INTO stock_data 
                            (symbol, date, open, high, low, close, volume)
                            VALUES (:symbol, :date, :open, :high, :low, :close, :volume)
                            ON CONFLICT (symbol, date) 
                            DO UPDATE SET 
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                            """),
                            {
                                'symbol': row['symbol'],
                                'date': row['date'],
                                'open': row['open'],
                                'high': row['high'],
                                'low': row['low'],
                                'close': row['close'],
                                'volume': row['volume']
                            }
                        )
                    except Exception as e:
                        print(f"Row failed: {row.to_dict()} - Error: {str(e)}")
                        continue
            
            print(f"Completed {filename} with {len(df)} rows")
            
        except Exception as e:
            print(f"File {filename} failed completely: {str(e)}")
            continue

if __name__ == '__main__':
    import_with_retry()
