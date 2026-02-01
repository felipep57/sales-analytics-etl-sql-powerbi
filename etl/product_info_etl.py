import os
import re
import hashlib
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# =========================
# CONFIG
# =========================
INPUT_PATH = os.getenv("INPUT_PATH")
SHEET_NAME = 0  # used only for Excel inputs

# Use Trusted Connection (Windows auth)
odbc_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=retail_analytics;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=Yes;"
)
ENGINE_URL = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)

# Choose which timestamp represents the "sale date" in fact_sales
FACT_DATE_SOURCE = "submitTime"  # or "createTime"

# =========================
# HELPERS
# =========================
def normalize_colname(c: str) -> str:
    c = c.strip().replace("\ufeff", "")  # remove BOM if present
    # normalize headers with spaces to snake_case (preserve original case except spaces->_)
    c = c.replace(" ", "_").replace("-", "_")
    return c

def parse_datetime_series(s: pd.Series) -> pd.Series:
    # Try a known format first to avoid pandas' "could not infer format" warning,
    # fall back to inference for heterogeneous inputs.
    if s is None:
        return s
    fmt = "%Y-%m-%d %H:%M:%S"
    try:
        return pd.to_datetime(s, format=fmt, errors="coerce")
    except Exception:
        return pd.to_datetime(s, errors="coerce", infer_datetime_format=True)

def strip_units_to_float(s: pd.Series) -> pd.Series:
    # e.g. "8.9523ft³" or "78.2641lb" or "5.2901ft3"
    return s.astype(str).str.extract(r"([0-9]+(?:\.[0-9]+)?)")[0].astype(float, errors="ignore")

def state_from_houseNo(s: pd.Series) -> pd.Series:
    s2 = s.astype(str).str.strip().str.upper()
    return s2.where(s2.str.match(r"^[A-Z]{2}$"), None)

def stable_customer_id(name, addr, postal) -> int:
    # Generate deterministic integer id from MD5.
    # MD5 first 16 hex chars => up to 64-bit unsigned value; SQL Server BIGINT is signed 64-bit.
    # To avoid pandas/sqlalchemy treating this as unsigned-64 (unsupported), mask to signed 63-bit range.
    raw = f"{name}|{addr}|{postal}".encode("utf-8", errors="ignore")
    h = hashlib.md5(raw).hexdigest()[:16]
    v = int(h, 16)
    # Keep result within signed 63-bit positive range to match BIGINT safely.
    signed63_mask = (1 << 63) - 1
    return v & signed63_mask

# Custom callable for pandas.to_sql that uses pyodbc fast_executemany
def _mssql_fast_executemany(table, conn, keys, data_iter):
    """
    table: SQLTable (pandas), conn: SQLAlchemy Connection, keys: list of cols,
    data_iter: iterable of tuples
    """
    # SQLAlchemy 2+ wraps driver connection; try recommended attribute first then fallbacks.
    raw_con = conn.connection
    # Prefer driver_connection (newer SQLAlchemy) then connection (older), then raw_con itself.
    raw_dbapi_conn = getattr(raw_con, "driver_connection", None) or getattr(raw_con, "connection", None) or raw_con

    cursor = raw_dbapi_conn.cursor()
    # Enable pyodbc fast_executemany for large batch inserts
    try:
        cursor.fast_executemany = True
    except Exception:
        # driver may not support it; continue without setting
        pass

    data = list(data_iter)
    if len(data) == 0:
        return

    # Build parameter placeholders for pyodbc (qmark style)
    placeholders = ", ".join(["?"] * len(keys))
    cols = ", ".join(f"[{k}]" for k in keys)
    insert_sql = f"INSERT INTO {table.name} ({cols}) VALUES ({placeholders})"

    cursor.executemany(insert_sql, data)
    # commit the raw connection (SQLAlchemy will manage transaction when using engine.begin,
    # but when pandas calls this method within a connection context, we still need to commit here)
    try:
        raw_dbapi_conn.commit()
    except Exception:
        # If commit is managed by SQLAlchemy transaction, ignore commit errors here
        pass

def ensure_dim_date(engine, min_date: datetime, max_date: datetime):
    if pd.isna(min_date) or pd.isna(max_date):
        return

    min_date = min_date.date()
    max_date = max_date.date()

    # Build date range
    dates = pd.date_range(min_date, max_date, freq="D")
    df = pd.DataFrame({"date_id": dates.date})
    df["year"] = dates.year
    df["quarter"] = ((dates.month - 1) // 3) + 1
    df["month"] = dates.month
    df["month_name"] = dates.strftime("%B")
    df["day"] = dates.day
    df["day_of_week"] = dates.dayofweek + 1
    df["is_weekend"] = (dates.dayofweek >= 5).astype(int)

    # Create dim_date (SQL Server compatible) if missing
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dim_date') AND type in (N'U'))
            BEGIN
                CREATE TABLE dim_date (
                    date_id DATE PRIMARY KEY,
                    year SMALLINT NOT NULL,
                    quarter TINYINT NOT NULL,
                    month TINYINT NOT NULL,
                    month_name VARCHAR(15) NOT NULL,
                    day TINYINT NOT NULL,
                    day_of_week TINYINT NOT NULL,
                    is_weekend BIT NOT NULL
                );
            END
        """))

    # Upsert rows: write staging using the SAME connection, then MERGE (no temp table visibility issue)
    df_to_load = df.copy()

    # Use a permanent staging name (dbo.stg_dim_date) and the same connection for to_sql + MERGE
    with engine.begin() as conn:
        # Write staging using the SQLAlchemy Connection so it uses the same DB session
        df_to_load.to_sql("stg_dim_date", conn, if_exists="replace", index=False, method=_mssql_fast_executemany, schema="dbo", chunksize=1000)

        conn.execute(text("""
            MERGE INTO dbo.dim_date AS target
            USING dbo.stg_dim_date AS src
            ON target.date_id = src.date_id
            WHEN NOT MATCHED THEN
              INSERT (date_id, year, quarter, month, month_name, day, day_of_week, is_weekend)
              VALUES (src.date_id, src.year, src.quarter, src.month, src.month_name, src.day, src.day_of_week, src.is_weekend);
            DROP TABLE dbo.stg_dim_date;
        """))

# =========================
# MAIN ETL
# =========================
def main():
    # Create engine and surface connection errors immediately
    try:
        engine = create_engine(ENGINE_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            pass
    except Exception as e:
        print("ERROR: Failed to create/connect SQLAlchemy engine.")
        print("Check that SQL Server is running, credentials are correct, and pyodbc + ODBC driver are installed.")
        print("Engine URL:", ENGINE_URL)
        print("Exception:", repr(e))
        raise

    # 1) Read input (CSV or Excel)
    path = INPUT_PATH
    ext = path.split(".")[-1].lower() if "." in path else ""
    if ext == "csv":
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    elif ext in ("xls", "xlsx"):
        df = pd.read_excel(path, sheet_name=SHEET_NAME, dtype=str)
    else:
        raise ValueError(f"Unsupported input file type: {path}")

    # Normalize column names
    df.columns = [normalize_colname(c) for c in df.columns]

    # DIAGNOSTICS: print file summary so you can confirm you loaded the expected CSV
    print("Loaded input file:", path)
    print("Rows:", len(df), "Columns:", len(df.columns))
    print("Columns list:", df.columns.tolist())

    # Attempt to auto-map common product-master / inventory files into the expected order-export shape:
    cols_lower = {c.lower(): c for c in df.columns}
    # map product master mainSkuCode -> masterSku
    if "mainskucode" in cols_lower and "masterSku" not in df.columns:
        df["masterSku"] = df[cols_lower["mainskucode"]]
        print("Mapped 'mainSkuCode' -> 'masterSku'")

    # map second SKU variants -> sku
    if "second_sku" in cols_lower and "sku" not in df.columns:
        df["sku"] = df[cols_lower["second_sku"]]
        print("Mapped 'Second SKU' -> 'sku'")

    # Keep your existing rename map (only applies if those exact names exist)
    rename_map = {
        "Urgent_Orders": "urgent_orders",
        "Batch_Number": "batch_number",
        "Serial_Number": "serial_number",
        "Inventory_Type": "inventory_type",
        "commercePlatform": "commercePlatform",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 2) Clean key fields
    for col in ["createTime", "submitTime", "deliveryTime", "pickingTime"]:
        if col in df.columns:
            df[col] = parse_datetime_series(df[col])

    if "volume" in df.columns:
        df["volume_num"] = strip_units_to_float(df["volume"])
    else:
        df["volume_num"] = None

    if "actualWeight" in df.columns:
        df["actualWeight_num"] = strip_units_to_float(df["actualWeight"])
    else:
        df["actualWeight_num"] = None

    if "houseNo" in df.columns:
        df["state_code"] = state_from_houseNo(df["houseNo"])
    else:
        df["state_code"] = None

    if "goodsNumber" in df.columns:
        df["goodsNumber"] = pd.to_numeric(df["goodsNumber"], errors="coerce").fillna(1).astype(int)
    else:
        df["goodsNumber"] = 1

    for required in ["name", "oneAddress", "postalCode"]:
        if required not in df.columns:
            df[required] = None

    df["customer_id"] = df.apply(
        lambda r: stable_customer_id(r.get("name"), r.get("oneAddress"), r.get("postalCode")),
        axis=1
    )
    # Ensure dtype is signed 64-bit integer for SQL Server BIGINT
    df["customer_id"] = df["customer_id"].astype("int64")

    # Pick product key: prefer masterSku else sku
    if "masterSku" not in df.columns:
        df["masterSku"] = None
    if "sku" not in df.columns:
        df["sku"] = None

    df["product_key"] = df["masterSku"].fillna("").astype(str).str.strip()
    df.loc[df["product_key"] == "", "product_key"] = df.loc[df["product_key"] == "", "sku"].fillna("").astype(str).str.strip()
    df.loc[df["product_key"] == "", "product_key"] = None

    # Extract product attributes from the DataFrame for updating dim_product
    # Map column names to dim_product fields
    product_cols_map = {
        "english_name": None,
        "chinese_name": None,
        "customer_code": None
    }

    # Find matching columns (case-insensitive)
    for col in df.columns:
        col_lower = col.lower()
        if "english" in col_lower and "name" in col_lower:
            product_cols_map["english_name"] = col
        elif "chinese" in col_lower and "name" in col_lower:
            product_cols_map["chinese_name"] = col
        elif "customer" in col_lower and "code" in col_lower:
            product_cols_map["customer_code"] = col

    print(f"Product attribute mapping: {product_cols_map}")

    # 3) Create product staging table for this ETL
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'stg_product_master') AND type in (N'U'))
            BEGIN
                CREATE TABLE stg_product_master (
                  main_sku_code VARCHAR(120) NOT NULL,
                  english_name VARCHAR(255) NULL,
                  chinese_name VARCHAR(255) NULL,
                  customer_code VARCHAR(100) NULL,
                  category VARCHAR(100) NULL
                );
            END
        """))

        conn.execute(text("TRUNCATE TABLE stg_product_master;"))

    # Prepare product staging data
    product_staging = pd.DataFrame()
    product_staging["main_sku_code"] = df["product_key"]
    product_staging["english_name"] = df[product_cols_map["english_name"]] if product_cols_map["english_name"] else None
    product_staging["chinese_name"] = df[product_cols_map["chinese_name"]] if product_cols_map["chinese_name"] else None
    product_staging["customer_code"] = df[product_cols_map["customer_code"]] if product_cols_map["customer_code"] else None
    product_staging["category"] = None  # Add category logic if needed

    # Remove rows with null main_sku_code
    product_staging = product_staging[product_staging["main_sku_code"].notna()]

    # Clean tabs and whitespace from all string columns
    for col in product_staging.columns:
        if product_staging[col].dtype == "object" and product_staging[col].notna().any():
            product_staging[col] = product_staging[col].str.rstrip('\t\r\n ').str.lstrip()

    # Debug: Show sample data before loading
    print("Sample product staging data (first 3 rows):")
    print(product_staging.head(3).to_string())
    print(f"Non-null counts: english_name={product_staging['english_name'].notna().sum()}, chinese_name={product_staging['chinese_name'].notna().sum()}, customer_code={product_staging['customer_code'].notna().sum()}")

    # Truncate string columns
    for col, max_len in [("main_sku_code", 120), ("english_name", 255), ("chinese_name", 255), ("customer_code", 100), ("category", 100)]:
        if col in product_staging.columns and product_staging[col].notna().any():
            product_staging[col] = product_staging[col].astype(str).str.slice(0, max_len)

    print(f"Loading {len(product_staging)} products into staging...")

    # Load product staging data
    product_staging.to_sql("stg_product_master", engine, if_exists="append", index=False, method=_mssql_fast_executemany, chunksize=1000)

    # Verify staging data was loaded correctly
    with engine.begin() as conn:
        result = conn.execute(text("SELECT TOP 3 * FROM stg_product_master")).fetchall()
        print(f"\nStaging table verification (first 3 rows from SQL Server):")
        for row in result:
            print(f"  {row}")

        # Check counts
        count_result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(english_name) as eng_count,
                COUNT(chinese_name) as chi_count,
                COUNT(customer_code) as cust_count
            FROM stg_product_master
        """)).fetchone()
        print(f"Staging counts in SQL Server: total={count_result[0]}, english_name={count_result[1]}, chinese_name={count_result[2]}, customer_code={count_result[3]}")


    # 4) Ensure dim_product table exists
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dim_product') AND type in (N'U'))
            BEGIN
                CREATE TABLE dim_product (
                    product_id INT IDENTITY(1,1) PRIMARY KEY,
                    main_sku_code VARCHAR(120) NOT NULL UNIQUE,
                    english_name VARCHAR(255) NULL,
                    chinese_name VARCHAR(255) NULL,
                    customer_code VARCHAR(100) NULL,
                    category VARCHAR(100) NULL
                );
            END
        """))

    # 5) Upsert dim_product from staging - Insert new products
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO dim_product (main_sku_code, english_name, chinese_name, customer_code, category)
            SELECT DISTINCT s.main_sku_code, s.english_name, s.chinese_name, s.customer_code, s.category
            FROM stg_product_master s
            WHERE s.main_sku_code IS NOT NULL AND s.main_sku_code <> ''
              AND NOT EXISTS (SELECT 1 FROM dim_product p WHERE p.main_sku_code = s.main_sku_code);
        """))
        print(f"Inserted {result.rowcount} new products into dim_product")

        # Update existing products with new information
        result = conn.execute(text("""
            UPDATE p
            SET p.english_name = COALESCE(s.english_name, p.english_name),
                p.chinese_name = COALESCE(s.chinese_name, p.chinese_name),
                p.customer_code = COALESCE(s.customer_code, p.customer_code),
                p.category = COALESCE(s.category, p.category)
            FROM dim_product p
            INNER JOIN stg_product_master s ON p.main_sku_code = s.main_sku_code
            WHERE s.english_name IS NOT NULL OR s.chinese_name IS NOT NULL 
               OR s.customer_code IS NOT NULL OR s.category IS NOT NULL;
        """))
        print(f"Updated {result.rowcount} existing products in dim_product")

        # Verify the update worked
        verify_result = conn.execute(text("""
            SELECT TOP 3 product_id, main_sku_code, english_name, chinese_name, customer_code 
            FROM dim_product 
            WHERE main_sku_code IN ('NBTKWJ-BCOK076A', 'HIFINE-PJHW-004WH', 'HIFINE-PJHW-004SE')
        """)).fetchall()
        print(f"\nVerification - dim_product after UPDATE:")
        for row in verify_result:
            print(f"  {row}")

    print("\nETL completed successfully.")

if __name__ == "__main__":
    main()

