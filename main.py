import pandas as pd
import re
from functools import reduce

def load_data(filename):
    """Load stock data from a CSV file."""
    df = pd.read_csv(filename, parse_dates=['Datetime'], index_col='Datetime')  # Load CSV, parse dates, set index
    return df

# 1. Handling Missing Data
def clean_missing_data(df):
    """Handle missing values using multiple strategies."""
    df.fillna(method='ffill', inplace=True)  # Forward fill missing values to maintain continuity
    df.fillna(method='bfill', inplace=True)  # Backward fill if any remain to avoid gaps
    df.interpolate(method='linear', inplace=True)  # Linear interpolation for smooth data handling
    return df

# 2. Filtering Relevant Columns
def filter_columns(df):
    """Select only essential stock columns."""
    return df[['Open', 'High', 'Low', 'Close', 'Volume']]  # Remove unnecessary columns for efficiency

# 3. Using Regex for Data Cleaning
def extract_ticker_from_filename(filename):
    """Extract ticker symbol from filename using regex."""
    match = re.search(r'([A-Z]+)_stock_data\.csv', filename)  # Extract ticker symbols from filenames
    return match.group(1) if match else 'Unknown'

# 4. Applying Functional Programming for Transformations
def transform_prices(df):
    """Apply transformations using map(), filter(), and reduce()."""
    df['Price Change'] = list(map(lambda x, y: round(y - x, 2), df['Open'], df['Close']))  # Compute daily price change
    df = df[df['Volume'] > 0]  # Filter out rows where no trades occurred
    df['Daily Range'] = list(map(lambda h, l: round(h - l, 2), df['High'], df['Low']))  # Calculate daily trading range
    return df

# 5. Aggregation Using Pandas Operations
def aggregate_data(df):
    """Aggregate stock data using groupby and pivot_table."""
    daily_summary = df.resample('D').agg({  # Resample to daily data for summary statistics
        'Open': 'first',  # First recorded price of the day
        'Close': 'last',  # Last recorded price of the day
        'High': 'max',  # Highest price of the day
        'Low': 'min',  # Lowest price of the day
        'Volume': 'sum'  # Total volume traded in the day
    })
    return daily_summary

if __name__ == "__main__":
    filename = "stock_data.csv"  # Example stock data file
    df = load_data(filename)  # Load stock data
    df = clean_missing_data(df)  # Handle missing values
    df = filter_columns(df)  # Keep only relevant stock columns
    df = transform_prices(df)  # Compute price changes and filter data
    aggregated_df = aggregate_data(df)  # Aggregate data to daily-level stats
    
    # Save cleaned and structured data
    df.to_csv("cleaned_stock_data.csv")  # Save cleaned stock data
    aggregated_df.to_csv("aggregated_stock_data.csv")  # Save aggregated summary data
    
    print("Data cleaning and structuring completed.")  # Notify user of completion
