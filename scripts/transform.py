from pathlib import Path
import sys
import duckdb

def transform(ticker: str, start: str, end: str) -> None:
    """
    Transform raw stock data into staged and analytics tables.
    """

    staged_dir = Path("data/staged")
    analytics_dir = Path("data/analytics")

    staged_dir.mkdir(parents=True, exist_ok=True)
    analytics_dir.mkdir(parents=True, exist_ok=True)

    db = duckdb.connect()
    
    # Build the staged table
    staged_path = staged_dir / f"{ticker}_{start}_{end}.parquet"
    db.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('data/raw/{ticker}.parquet')
            WHERE Date BETWEEN '{start}' AND '{end}'
        )
        TO '{staged_path}' (FORMAT PARQUET);
    """)
    print(f"[OK] Staged data written to {staged_path}")

    # Fact_price table
    fact_path = analytics_dir / f"fact_price_{ticker}_{start}_{end}.parquet"
    
    # Check available columns and handle Yahoo Finance's tuple-format column names
    columns = db.execute(f"SELECT * FROM read_parquet('{staged_path}') LIMIT 0").description
    column_names = [col[0] for col in columns]
    
    tuple_format = any("'," in str(col) for col in column_names)
    
    if tuple_format:
        # Handle Yahoo Finance's tuple-format columns
        date_col = next((col for col in column_names if col.lower() == 'date'), 'Date')
        
        column_map = {}
        for col_type in ['Open', 'High', 'Low', 'Close', 'Volume']:
            col_candidates = [col for col in column_names if f"('{col_type}'" in str(col)]
            if col_candidates:
                column_map[col_type.lower()] = col_candidates[0]
        
        select_parts = [f"date_trunc('day', {date_col}) AS date"]
        select_parts.append(f"'{ticker}' AS ticker")
        
        for col_type in ['Open', 'High', 'Low', 'Close', 'Volume']:
            col_lower = col_type.lower()
            if col_lower in column_map:
                select_parts.append(f"{column_map[col_lower]} AS {col_type}")
            else:
                select_parts.append(f"NULL AS {col_type}")
    else:
        # Standard column handling for non-tuple format
        column_map = {col.lower(): col for col in column_names}
        
        select_parts = [f"date_trunc('day', {column_map.get('date', 'Date')}) AS date"]
        
        if 'symbol' in column_map:
            select_parts.append(f"{column_map['symbol']} AS ticker")
        else:
            select_parts.append(f"'{ticker}' AS ticker")
        
        price_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in price_columns:
            if col in column_map:
                select_parts.append(f"{column_map[col]} AS {col.capitalize()}")
            elif col.capitalize() in column_names:
                select_parts.append(f"{col.capitalize()}")
            else:
                select_parts.append(f"NULL AS {col.capitalize()}")
    
    sql = f"""
        COPY (
            SELECT
                {', '.join(select_parts)}
            FROM read_parquet('{staged_path}')
        )
        TO '{fact_path}' (FORMAT PARQUET);
    """
    
    db.execute(sql)
    print(f"[OK] Fact table written to {fact_path}")

    # Dim_date table
    dim_path = analytics_dir / f"dim_date_{ticker}_{start}_{end}.parquet"
    
    fact_columns = db.execute(f"SELECT * FROM read_parquet('{fact_path}') LIMIT 0").description
    fact_column_names = [col[0] for col in fact_columns]
        
    # Find the date column
    date_col = 'date'
    for col in fact_column_names:
        if col.lower() == 'date':
            date_col = col
            break
        
    # Create dimension table
    dim_sql = f"""
        COPY (
            SELECT DISTINCT
                date_trunc('day', {date_col}) AS date,
                extract('year'  FROM {date_col})  AS year,
                extract('month' FROM {date_col})  AS month,
                extract('day'   FROM {date_col})  AS day
            FROM read_parquet('{fact_path}')
        )
        TO '{dim_path}' (FORMAT PARQUET);
    """
        
    db.execute(dim_sql)
    print(f"[OK] Dimension table written to {dim_path}")

    db.close()
    return



if __name__ == "__main__":
    ticker = sys.argv[1].upper()
    start = sys.argv[2]
    end = sys.argv[3]
    
    transform(ticker, start, end)
