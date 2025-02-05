import time
import pandas as pd 
import yfinance as yf 
import boto3 
from botocore.exceptions import NoCredentialsError, PartialCredentialsError 
from requests.exceptions import RequestException 

def fetch_stock_data(ticker, retries=3, delay=5):
    """
    Fetch real-time stock data from Yahoo Finance with retry logic.
    
    :param ticker: Stock ticker symbol (e.g., 'AAPL')
    :param retries: Number of retry attempts in case of failure
    :param delay: Delay (in seconds) between retries
    :return: DataFrame containing stock data
    """
    attempt = 0
    while attempt < retries:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period='1d', interval='1m')  # Fetch daily data with 1-min interval
            if hist.empty:
                raise ValueError("No data returned, possible invalid ticker.")
            return hist
        except (RequestException, ValueError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
            attempt += 1
    
    print("Failed to fetch data after multiple retries.")
    return None

def save_to_csv(df, filename):
    """Save stock data to a CSV file."""
    if df is not None:
        df.to_csv(filename, index=True)
        print(f"Data saved to {filename}")
    else:
        print("No data to save.")

def upload_to_s3(filename, bucket_name, s3_filename):
    """Upload a file to an AWS S3 bucket."""
    s3 = boto3.client('s3')
    try:
        s3.upload_file(filename, bucket_name, s3_filename)
        print(f"File {filename} uploaded to {bucket_name}/{s3_filename}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"S3 upload failed: {e}")

def download_from_s3(bucket_name, s3_filename, local_filename):
    """Download a file from AWS S3 to local storage."""
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, s3_filename, local_filename)
        print(f"File {s3_filename} downloaded from {bucket_name} to {local_filename}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"S3 download failed: {e}")

if __name__ == "__main__":
    ticker = "AAPL"  # Example: Apple stock
    bucket_name = "newpipeline"  # Replace with your S3 bucket name
    s3_filename = "stock_data.csv"
    local_filename = "stock_data.csv"
    
    stock_data = fetch_stock_data(ticker)
    save_to_csv(stock_data, local_filename)
    upload_to_s3(local_filename, bucket_name, s3_filename)
    download_from_s3(bucket_name, s3_filename, "downloaded_stock_data.csv")
