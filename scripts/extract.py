from pathlib import Path
import sys
import yfinance as yf

def extract(ticker: str, start: str, end: str) -> None:
    """
    Download historical price data and save to parquet file.
    """
    
    # Retrieve data
    rd = yf.download(ticker, start=start, end=end, progress=False)

    if rd.empty:
        print(f"[WARN] No data downloaded for {ticker} between {start} and {end}.")
        return
    
    # Check if output directory exists
    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    out_file = out_dir / f"{ticker}.parquet"
    
    # Add ticker as column for easier joining later
    if 'Symbol' not in rd.columns:
        rd['Symbol'] = ticker
        
    rd.to_parquet(out_file)
    print(f"[OK] Saved {len(rd)} rows of {ticker} data to {out_file}.")
    return rd



if __name__ == "__main__":
    ticker = sys.argv[1].upper()
    start = sys.argv[2]
    end = sys.argv[3]
    
    extract(ticker, start, end)
