from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
import pandas as pd

def import_csv_to_postgres(csv_file_path, table_name, db_connection_string):

    # Create SQLAlchemy engine
    engine = create_engine(db_connection_string)
    
    # Read CSV file into pandas DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Get column names and data types from the existing table
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    table = Table(table_name, metadata, autoload_with=engine)
    
    # Verify CSV columns match table columns
    csv_columns = set(df.columns)
    table_columns = set(table.columns.keys())
    
    if not csv_columns.issubset(table_columns):
        missing_columns = csv_columns - table_columns
        raise ValueError(f"CSV contains columns not in table: {missing_columns}")
    
    # Import data to PostgreSQL
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',  # Append to existing table
        index=False,        # Don't write row indices
        method='multi'     # Multi-row insert for efficiency
    )
    
    print(f"Successfully imported {len(df)} rows to {table_name}")

# Example usage
if __name__ == "__main__":
    # Replace these with your actual values
    CSV_PATH = './companies.csv'
    TABLE_NAME = 'companies'
    DB_CONNECTION = 'postgresql://admin:password@localhost:5432/stockmarket'
    
    import_csv_to_postgres(CSV_PATH, TABLE_NAME, DB_CONNECTION)
