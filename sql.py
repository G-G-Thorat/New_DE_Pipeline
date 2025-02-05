import pandas as pd
import sqlite3
import json
import os
from sqlalchemy import create_engine, exc

# Custom Exception Classes
class DataPipelineError(Exception):
    """Base exception class for the data pipeline."""
    pass

class APIDataError(DataPipelineError):
    """Exception raised when API data retrieval fails."""
    pass

class DatabaseConnectionError(DataPipelineError):
    """Exception raised when there is an issue connecting to the database."""
    pass

# 1. Using Generators for Efficient Data Processing

def stream_csv_data(filename):
    """Yield rows from a CSV file one by one instead of loading everything into memory."""
    with open(filename, 'r', encoding='utf-8') as file:
        next(file)  # Skip header
        for line in file:
            yield line.strip().split(',')  # Yield each row as a list

# 2. Handling Errors Gracefully with Try-Except Blocks

def safe_read_csv(filename):
    """Read a CSV file safely with error handling."""
    try:
        return pd.read_csv(filename)
    except FileNotFoundError:
        raise DataPipelineError(f"Error: The file {filename} was not found.")
    except pd.errors.EmptyDataError:
        raise DataPipelineError(f"Error: The file {filename} is empty.")
    except Exception as e:
        raise DataPipelineError(f"Unexpected error: {e}")

# 3. Store Data in Multiple Formats

def save_data_formats(df, filename):
    """Save processed stock data in CSV, JSON, and Parquet formats."""
    try:
        df.to_csv(f"{filename}.csv", index=False)
        df.to_json(f"{filename}.json", orient='records', indent=4)
        df.to_parquet(f"{filename}.parquet", index=False)
        print("Data successfully saved in CSV, JSON, and Parquet formats.")
    except Exception as e:
        raise DataPipelineError(f"Error saving data: {e}")

# 4. Store Data in an SQL Database

def store_in_database(df, db_name, table_name):
    """Store cleaned stock data into an SQLite database."""
    try:
        engine = create_engine(f"sqlite:///{db_name}")  # Create database connection
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)  # Store data in SQL
        print(f"Data successfully stored in {db_name} under table {table_name}.")
    except exc.SQLAlchemyError as e:
        raise DatabaseConnectionError(f"Database error: {e}")
    finally:
        engine.dispose()  # Close database connection

# 5. Read Data from SQL Database
def read_from_database(db_name, table_name):
    """Read data from an SQLite database efficiently."""
    try:
        engine = create_engine(f"sqlite:///{db_name}")
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
        print("Data retrieved successfully from the database.")
        return df
    except exc.SQLAlchemyError as e:
        raise DatabaseConnectionError(f"Error retrieving data: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    filename = "cleaned_stock_data.csv"  # Input file name
    db_name = "stock_data.db"  # SQLite database name
    table_name = "stocks"  # Table name
    
    # Read and clean data
    df = safe_read_csv(filename)
    
    # Store data in multiple formats
    save_data_formats(df, "processed_stock_data")
    
    # Store data in a SQL database
    store_in_database(df, db_name, table_name)
    
    # Retrieve and print data from the database
    retrieved_df = read_from_database(db_name, table_name)
    print(retrieved_df.head())
