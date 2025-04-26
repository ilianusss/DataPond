import requests
import pandas as pd
import yfinance as yf
from pathlib import Path
import json
import time
from datetime import datetime

SEC_EDGAR_BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
HEADERS = {
    "User-Agent": "MiniDataLake/1.0 (chevalinn1@gmail.com)",
    "Accept": "application/json"
}

def get_ticker_cik_mapping():
    """
    Get a mapping of ticker symbols to CIK numbers.
    """
    try:
        # Try to load from cache first
        cache_path = Path("data/cache/ticker_cik_map.json")
        if cache_path.exists():
            with open(cache_path, "r") as f:
                return json.load(f)
        
        # If not in cache, download
        mapping_url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(mapping_url, headers=HEADERS)
        data = response.json()
        
        # Convert to dictionary mapping ticker -> CIK
        ticker_to_cik = {}
        for _, company in data.items():
            ticker = company['ticker']
            # SEC stores CIK without leading zeros, but API requires 10 digits
            cik = str(company['cik_str']).zfill(10)
            ticker_to_cik[ticker] = cik
        
        # Save to cache
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(ticker_to_cik, f)
            
        return ticker_to_cik
    except Exception as e:
        print(f"Error getting CIK mapping: {e}")
        return {}

def get_fundamentals_from_sec(ticker):
    """
    Get fundamental data from SEC Edgar API.
    Returns a dictionary of fundamental metrics.
    """
    try:
        # Get CIK for ticker
        ticker_to_cik = get_ticker_cik_mapping()
        if ticker not in ticker_to_cik:
            print(f"CIK not found for {ticker}")
            return {}
            
        cik = ticker_to_cik[ticker]
        
        # SEC API has rate limits
        time.sleep(0.1)
        
        # Get company facts
        url = f"{SEC_EDGAR_BASE_URL}/CIK{cik}.json"
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code != 200:
            print(f"Error fetching SEC data: {response.status_code}")
            return {}
            
        data = response.json()
        
        # Extract key financial metrics
        metrics = {}
        try:
            if 'facts' in data and 'us-gaap' in data['facts']:
                us_gaap = data['facts']['us-gaap']
                
                # Revenue
                if 'Revenue' in us_gaap or 'SalesRevenueNet' in us_gaap:
                    revenue_key = 'Revenue' if 'Revenue' in us_gaap else 'SalesRevenueNet'
                    revenue_data = us_gaap[revenue_key]['units']['USD']
                    annual_data = [item for item in revenue_data if item.get('form') == '10-K']
                    if annual_data:
                        latest = max(annual_data, key=lambda x: x.get('filed', ''))
                        metrics['Revenue'] = {
                            'value': latest['val'],
                            'filed_date': latest['filed'],
                            'end_date': latest['end'],
                        }
                
                # Net Income
                if 'NetIncomeLoss' in us_gaap:
                    ni_data = us_gaap['NetIncomeLoss']['units']['USD']
                    annual_data = [item for item in ni_data if item.get('form') == '10-K']
                    if annual_data:
                        latest = max(annual_data, key=lambda x: x.get('filed', ''))
                        metrics['NetIncome'] = {
                            'value': latest['val'],
                            'filed_date': latest['filed'],
                            'end_date': latest['end'],
                        }
                
                # Total Assets
                if 'Assets' in us_gaap:
                    assets_data = us_gaap['Assets']['units']['USD']
                    annual_data = [item for item in assets_data if item.get('form') == '10-K']
                    if annual_data:
                        latest = max(annual_data, key=lambda x: x.get('filed', ''))
                        metrics['TotalAssets'] = {
                            'value': latest['val'],
                            'filed_date': latest['filed'],
                            'end_date': latest['end'],
                        }
                
                # Total Liabilities
                if 'Liabilities' in us_gaap:
                    liab_data = us_gaap['Liabilities']['units']['USD']
                    annual_data = [item for item in liab_data if item.get('form') == '10-K']
                    if annual_data:
                        latest = max(annual_data, key=lambda x: x.get('filed', ''))
                        metrics['TotalLiabilities'] = {
                            'value': latest['val'],
                            'filed_date': latest['filed'],
                            'end_date': latest['end'],
                        }
        except Exception as e:
            print(f"[ERR] Error parsing SEC data: {e}")
        
        return metrics
    except Exception as e:
        print(f"[ERR] Error in SEC API request: {e}")
        return {}

def get_fundamentals_from_yfinance(ticker):
    """
    Get fundamental data from Yahoo Finance.
    Returns a dictionary of fundamental metrics.
    """
    try:
        stock = yf.Ticker(ticker)
        
        metrics = {}
        
        info = stock.info
        
        # EPS and Related
        for key in ['trailingEps', 'forwardEps', 'pegRatio']:
            if key in info:
                display_names = {
                    'trailingEps': 'EPS',
                    'forwardEps': 'Forward_EPS',
                    'pegRatio': 'PEG_Ratio'
                }
                display_name = display_names.get(key, key)
                metrics[display_name] = {
                    'value': info[key],
                    'source': 'Yahoo Finance'
                }
        
        # Valuation Ratios
        for key in ['trailingPE', 'forwardPE', 'priceToBook', 'priceToSalesTrailing12Months']:
            if key in info:
                display_names = {
                    'trailingPE': 'PE_Ratio',
                    'forwardPE': 'Forward_PE',
                    'priceToBook': 'Price_to_Book',
                    'priceToSalesTrailing12Months': 'Price_to_Sales'
                }
                display_name = display_names.get(key, key)
                metrics[display_name] = {
                    'value': info[key],
                    'source': 'Yahoo Finance'
                }
        
        # Market Data
        if 'marketCap' in info:
            metrics['MarketCap'] = {
                'value': info['marketCap'],
                'source': 'Yahoo Finance'
            }
        
        # Profitability
        for key in ['profitMargins', 'returnOnAssets', 'returnOnEquity']:
            if key in info:
                display_names = {
                    'profitMargins': 'Profit_Margin',
                    'returnOnAssets': 'ROA',
                    'returnOnEquity': 'ROE'
                }
                display_name = display_names.get(key, key)
                metrics[display_name] = {
                    'value': info[key],
                    'source': 'Yahoo Finance'
                }
        
        # Dividend and Growth
        for key in ['dividendYield', 'dividendRate', 'payoutRatio', 'revenueGrowth', 'earningsGrowth']:
            if key in info:
                display_names = {
                    'dividendYield': 'Dividend_Yield',
                    'dividendRate': 'Dividend_Rate',
                    'payoutRatio': 'Payout_Ratio',
                    'revenueGrowth': 'Revenue_Growth',
                    'earningsGrowth': 'Earnings_Growth'
                }
                display_name = display_names.get(key, key)
                metrics[display_name] = {
                    'value': info[key],
                    'source': 'Yahoo Finance'
                }
        
        # Risk Metrics
        for key in ['beta', 'debtToEquity', 'currentRatio', 'quickRatio']:
            if key in info:
                display_names = {
                    'beta': 'Beta',
                    'debtToEquity': 'Debt_to_Equity',
                    'currentRatio': 'Current_Ratio',
                    'quickRatio': 'Quick_Ratio'
                }
                display_name = display_names.get(key, key)
                metrics[display_name] = {
                    'value': info[key],
                    'source': 'Yahoo Finance'
                }
                
        # 52-week range
        for key in ['fiftyTwoWeekHigh', 'fiftyTwoWeekLow']:
            if key in info:
                display_names = {
                    'fiftyTwoWeekHigh': 'Year_High',
                    'fiftyTwoWeekLow': 'Year_Low'
                }
                display_name = display_names.get(key, key)
                metrics[display_name] = {
                    'value': info[key],
                    'source': 'Yahoo Finance'
                }
        
        return metrics
    except Exception as e:
        print(f"Error getting Yahoo Finance data: {e}")
        return {}

def get_full_fundamentals(ticker):
    """
    Combine fundamental data from all sources.
    """
    fundamentals = {}
    
    yf_data = get_fundamentals_from_yfinance(ticker)
    fundamentals.update(yf_data)
    
    sec_data = get_fundamentals_from_sec(ticker)
    fundamentals.update(sec_data)
    
    return fundamentals

def save_fundamentals(ticker, fundamentals):
    """
    Save fundamental data to parquet file.
    """
    if not fundamentals:
        return
        
    out_dir = Path("data/fundamentals")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    df = pd.DataFrame([fundamentals])
    df['ticker'] = ticker
    df['timestamp'] = datetime.now().isoformat()
    
    out_file = out_dir / f"{ticker}_fundamentals.parquet"
    df.to_parquet(out_file)
    print(f"[OK] Saved fundamental data for {ticker} to {out_file}")
    
    return out_file

def load_fundamentals(ticker):
    """
    Load fundamental data from parquet file.
    """
    file_path = Path("data/fundamentals") / f"{ticker}_fundamentals.parquet"
    if not file_path.exists():
        return None
    
    try:
        data = pd.read_parquet(file_path)
        # Convert first row to dict
        fundamentals = data.iloc[0].to_dict()
        return fundamentals
    except Exception as e:
        print(f"Error loading fundamentals for {ticker}: {e}")
        return None

def get_or_update_fundamentals(ticker, force_update=False):
    """
    Get fundamental data from file or update if needed.
    Returns a dictionary of fundamental metrics.
    """
    # Try to load existing data first
    if not force_update:
        existing_data = load_fundamentals(ticker)
        if existing_data:
            # Check if data is recent (less than 24 hours old)
            timestamp = existing_data.get('timestamp')
            if timestamp:
                try:
                    data_time = datetime.fromisoformat(timestamp)
                    age = datetime.now() - data_time
                    if age.total_seconds() < 86400:  # 24 hours
                        return existing_data
                except:
                    pass
    
    # Get new data
    fundamentals = get_full_fundamentals(ticker)
    if fundamentals:
        save_fundamentals(ticker, fundamentals)
    
    return fundamentals



if __name__ == "__main__":
    ticker = sys.argv[1].upper()
    fundamentals = get_or_update_fundamentals(ticker, force_update=True) 
