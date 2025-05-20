"""
Streamlit app for US stock data visualization.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add project root to path to import scripts
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract
from scripts.transform import transform
from scripts.fundamentals import get_or_update_fundamentals

def get_us_stock_tickers():
    """Get a list of US stock tickers"""
    stocks = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'PG',
        'DIS', 'KO', 'MCD', 'INTC', 'CSCO', 'VZ', 'HD', 'CVX', 'XOM', 'JNJ',
        'BAC', 'WMT', 'UNH', 'MA', 'PFE', 'T', 'MRK'    
    ]
    return sorted(stocks)

def get_stock_data(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Get stock data, handling all scenarios.
    """
    staged_path = Path("data/staged") / f"{ticker}_{start}_{end}.parquet"
    raw_path = Path("data/raw") / f"{ticker}.parquet"
    
    # Scenario 1: Check if staged data already exists with correct range
    if staged_path.exists():
        return pd.read_parquet(staged_path)
    
    # Scenario 2: Check if raw data exists
    #if raw_path.exists():
    #    with st.spinner("Transforming data..."):
    #        transform(ticker, start, end)
    #    return pd.read_parquet(staged_path)
    
    # Scenario 3: Need to extract and transform
    with st.spinner("Downloading and processing data..."):
        extract(ticker, start, end)
        transform(ticker, start, end)
    
    return pd.read_parquet(staged_path)

def hide_anchor_links():
    # Hide disgusting anchor links 
    st.markdown("""
    <style>
        [data-testid="stHeaderActionElements"] {display: none !important}
    </style>
    """, unsafe_allow_html=True)

def main() -> None:
    hide_anchor_links()
   
    st.title("Financial Data Pond")
    
    # Get list of tickers
    tickers = get_us_stock_tickers()
    
    # UI for selection
    ticker = st.selectbox("Select Stock", tickers, index=0).upper()
    
    # Period and date selection
    st.write("Select Time Period:")
    period_col1, period_col2, period_col3, period_col4, period_col5 = st.columns(5)
    today = pd.to_datetime("today")

    if "start_date" not in st.session_state:
        st.session_state["start_date"] = today - pd.Timedelta(days=50)
    if "end_date" not in st.session_state:
        st.session_state["end_date"] = today

    if period_col1.button("1M"):
        st.session_state["start_date"] = today - pd.Timedelta(days=30)
    if period_col2.button("3M"):
        st.session_state["start_date"] = today - pd.Timedelta(days=90)
    if period_col3.button("6M"):
        st.session_state["start_date"] = today - pd.Timedelta(days=180)
    if period_col4.button("1Y"):
        st.session_state["start_date"] = today - pd.Timedelta(days=365)
    if period_col5.button("5Y"):
        st.session_state["start_date"] = today - pd.Timedelta(days=365*5)

    col1, col2 = st.columns(2)
    with col1:
        st.date_input(
            "Start date",
            key="start_date",
            min_value=pd.Timestamp("2010-01-01"),
            max_value=st.session_state["end_date"]
        )
    with col2:
        st.date_input(
            "End date",
            key="end_date",
            min_value=st.session_state["start_date"],
            max_value=today
        )

    # Format for loading data
    start_str = st.session_state["start_date"].strftime("%Y-%m-%d")
    end_str = st.session_state["end_date"].strftime("%Y-%m-%d")
    
    if st.button("Load Data", type="primary"):
        try:
            df = get_stock_data(ticker, start_str, end_str)
            
            if df is None or df.empty:
                st.error(f"[ERR] No data found for {ticker} from {start_str} to {end_str}.")
                return
            
            # Plot the data
            # Handle column name case insensitivity and tuple format
            def find_column(df, col_name):
                """Find a column case-insensitively and handle tuple format"""
                col_lower = col_name.lower()
                
                # First try direct case-insensitive match
                for col in df.columns:
                    if str(col).lower() == col_lower:
                        return col
                
                # Look for tuple format columns like "('Close', 'AMZN')"
                for col in df.columns:
                    col_str = str(col)
                    if f"('{col_name.capitalize()}'" in col_str or f"('{col_name.upper()}'" in col_str:
                        return col
                    if f'"{col_name.capitalize()}"' in col_str or f'"{col_name.upper()}"' in col_str:
                        return col
                        
                # Show available columns in error
                raise ValueError(f"Column '{col_name}' not found (case-insensitive). Available columns: {list(df.columns)}")
            
            # Find date, close and volume columns regardless of case
            date_col = find_column(df, "date")
            close_col = find_column(df, "close")
            volume_col = find_column(df, "volume")
            
            # Sort by date
            df = df.sort_values(date_col)
            
            # Find all required columns with fallbacks
            try:
                open_col = find_column(df, "open")
                high_col = find_column(df, "high")
                low_col = find_column(df, "low")
            except ValueError:
                # If we can't find all OHLC columns, just use close
                st.warning("Complete OHLC data not available. Showing basic charts only.")
                open_col = close_col
                high_col = close_col
                low_col = close_col
            
            # Interactive Trading Chart with Plotly
            st.subheader(f"{ticker} Interactive Trading Chart")
            
            # Create subplot with 2 rows (price and volume)
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                               vertical_spacing=0.03, 
                               row_heights=[0.7, 0.3])
            
            # Candlestick
            fig.add_trace(go.Candlestick(
                x=df[date_col],
                open=df[open_col],
                high=df[high_col],
                low=df[low_col],
                close=df[close_col],
                name='OHLC'
            ), row=1, col=1)
            
            # 20-day moving average
            if len(df) > 20:
                df['MA20'] = df[close_col].rolling(window=20).mean()
                fig.add_trace(go.Scatter(
                    x=df[date_col],
                    y=df['MA20'],
                    opacity=0.7,
                    line=dict(color='blue', width=2),
                    name='20-day MA'
                ), row=1, col=1)
            
            # 50-day moving average
            if len(df) > 50:
                df['MA50'] = df[close_col].rolling(window=50).mean()
                fig.add_trace(go.Scatter(
                    x=df[date_col],
                    y=df['MA50'],
                    opacity=0.7,
                    line=dict(color='red', width=2),
                    name='50-day MA'
                ), row=1, col=1)
            
            # Volume bar chart
            fig.add_trace(go.Bar(
                x=df[date_col],
                y=df[volume_col],
                marker_color='rgba(0, 150, 255, 0.6)',
                name='Volume'
            ), row=2, col=1)
            
            # Update layout
            fig.update_layout(
                title=f'{ticker} Stock Price and Volume',
                yaxis_title='Price',
                yaxis2_title='Volume',
                xaxis_rangeslider_visible=False,
                height=600,
                margin=dict(l=0, r=0, t=50, b=0),
                showlegend=True,
                hovermode='closest'
            )
            
            config = {'displayModeBar': False}

            metrics_container = st.container()
            
            # Metrics
            with metrics_container:
                st.subheader("Price Metrics")
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("First Price", f"${df.iloc[0][close_col]:.2f}")
                with col2:
                    st.metric("Last Price", f"${df.iloc[-1][close_col]:.2f}")
                with col3:
                    change = df.iloc[-1][close_col] - df.iloc[0][close_col]
                    st.metric("Change", f"${change:.2f}", f"{(change/df.iloc[0][close_col])*100:.2f}%")
                with col4:
                    high_price = df[high_col].max()
                    st.metric("Highest", f"${high_price:.2f}")
                with col5:
                    low_price = df[low_col].min()
                    st.metric("Lowest", f"${low_price:.2f}")
            
            st.plotly_chart(fig, use_container_width=True, config=config)
            

            # Fundamental data
            with st.spinner("Loading fundamental data..."):
                fundamentals = get_or_update_fundamentals(ticker)
            
            if fundamentals:
                st.subheader(f"{ticker} Fundamental Data")
                
                # Format numbers
                def format_value(value):
                    if isinstance(value, (int, float)):
                        if abs(value) >= 1_000_000_000:  # billions
                            return f"${value/1_000_000_000:.2f}B"
                        elif abs(value) >= 1_000_000:  # millions
                            return f"${value/1_000_000:.2f}M"
                        elif abs(value) >= 1_000:  # thousands
                            return f"${value/1_000:.2f}K"
                        else:
                            return f"${value:.2f}"
                    return str(value)
                
                
                # Metric display function
                def display_metric(tab, col_index, metric_key, display_name, percentage=False, prefix="$"):
                    metric_display = "N/A"
                    
                    if metric_key in fundamentals and isinstance(fundamentals[metric_key], dict):
                        value = fundamentals[metric_key].get('value')
                        if value is not None:
                            if percentage:
                                # Convert decimal to percentage if needed
                                if -1 < value < 1:
                                    value = value * 100
                                metric_display = f"{value:.2f}%"
                            elif prefix == "$":
                                metric_display = format_value(value)
                            else:
                                metric_display = f"{prefix}{value:.2f}"
                    
                    tab.metric(display_name, metric_display)


                fund_tab1, fund_tab2, fund_tab3, fund_tab4 = st.tabs(["Key Metrics", "Valuation", "Profitability", "Financial Health"])

                # Tab 1: Key financial metrics
                with fund_tab1:
                    rows = 3
                    for row in range(rows):
                        cols = st.columns(3)
                        if row == 0:
                            # Row 1: EPS metrics
                            display_metric(cols[0], 0, "EPS", "EPS")
                            display_metric(cols[1], 1, "Forward_EPS", "Forward EPS")
                            display_metric(cols[2], 2, "PEG_Ratio", "PEG Ratio", prefix="")
                        elif row == 1:
                            # Row 2: Revenue & Income
                            display_metric(cols[0], 0, "Revenue", "Revenue")
                            display_metric(cols[1], 1, "NetIncome", "Net Income")
                            display_metric(cols[2], 2, "Earnings_Growth", "Earnings Growth", percentage=True)
                        elif row == 2:
                            # Row 3: Growth metrics
                            display_metric(cols[0], 0, "Revenue_Growth", "Revenue Growth", percentage=True)
                            display_metric(cols[1], 1, "Year_High", "52-Week High")
                            display_metric(cols[2], 2, "Year_Low", "52-Week Low")
                
                # Tab 2: Valuation metrics
                with fund_tab2:
                    rows = 3
                    for row in range(rows):
                        cols = st.columns(3)
                        if row == 0:
                            # Row 1: PE metrics
                            display_metric(cols[0], 0, "PE_Ratio", "P/E Ratio", prefix="")
                            display_metric(cols[1], 1, "Forward_PE", "Forward P/E", prefix="")
                            display_metric(cols[2], 2, "Price_to_Book", "Price/Book", prefix="")
                        elif row == 1:
                            # Row 2: More valuation
                            display_metric(cols[0], 0, "Price_to_Sales", "Price/Sales", prefix="")
                            display_metric(cols[1], 1, "MarketCap", "Market Cap")
                            display_metric(cols[2], 2, "Beta", "Beta", prefix="")
                        elif row == 2:
                            # Row 3: Dividend metrics
                            display_metric(cols[0], 0, "Dividend_Yield", "Dividend Yield", percentage=True)
                            display_metric(cols[1], 1, "Dividend_Rate", "Dividend Rate")
                            display_metric(cols[2], 2, "Payout_Ratio", "Payout Ratio", percentage=True)
                
                # Tab 3: Profitability
                with fund_tab3:
                    cols = st.columns(3)
                    display_metric(cols[0], 0, "Profit_Margin", "Profit Margin", percentage=True)
                    display_metric(cols[1], 1, "ROE", "Return on Equity", percentage=True)
                    display_metric(cols[2], 2, "ROA", "Return on Assets", percentage=True)
                
                # Tab 4: Financial Health
                with fund_tab4:
                    rows = 2
                    for row in range(rows):
                        cols = st.columns(3)
                        if row == 0:
                            # Row 1: Balance sheet
                            display_metric(cols[0], 0, "TotalAssets", "Total Assets")
                            display_metric(cols[1], 1, "TotalLiabilities", "Total Liabilities")
                            display_metric(cols[2], 2, "Debt_to_Equity", "Debt/Equity", prefix="")
                        elif row == 1:
                            # Row 2: Liquidity
                            display_metric(cols[0], 0, "Current_Ratio", "Current Ratio", prefix="")
                            display_metric(cols[1], 1, "Quick_Ratio", "Quick Ratio", prefix="")
                            cols[2].write("")
            
        except Exception as e:
            st.error(f"Error loading data: {e}")

if __name__ == "__main__":
    main()
