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
    # normalize headers with spaces to snake_case
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

    # Create dim_date (SQL Server compatible)
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

    # Upsert rows: use temporary staging and MERGE for idempotence
    df_to_load = df.copy()
    # Use the fast executemany method when pushing the temp rows
    df_to_load.to_sql("#tmp_dim_date", engine, if_exists="replace", index=False, method=_mssql_fast_executemany, chunksize=1000)

    with engine.begin() as conn:
        conn.execute(text("""
            MERGE INTO dim_date AS target
            USING #tmp_dim_date AS src
            ON target.date_id = src.date_id
            WHEN NOT MATCHED THEN
              INSERT (date_id, year, quarter, month, month_name, day, day_of_week, is_weekend)
              VALUES (src.date_id, src.year, src.quarter, src.month, src.month_name, src.day, src.day_of_week, src.is_weekend);
            DROP TABLE #tmp_dim_date;
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

    df.columns = [normalize_colname(c) for c in df.columns]

    rename_map = {
        "Urgent_Orders": "urgent_orders",
        "Batch_Number": "batch_number",
        "Serial_Number": "serial_number",
        "Inventory_Type": "inventory_type",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 2) Clean key fields
    for col in ["createTime", "submitTime", "deliveryTime", "pickingTime"]:
        if col in df.columns:
            # Clean tabs/whitespace before parsing datetime
            df[col] = df[col].str.rstrip('\t\r\n ').str.lstrip()
            df[col] = parse_datetime_series(df[col])

    if "volume" in df.columns:
        df["volume_num"] = strip_units_to_float(df["volume"])
    else:
        df["volume_num"] = None

    if "actualWeight" in df.columns:
        df["actualWeight_num"] = strip_units_to_float(df["actualWeight"])
    else:
        df["actualWeight_num"] = None

    # Use State column directly if available, otherwise try to extract from houseNo
    if "State" in df.columns:
        df["state_code"] = df["State"].astype(str).str.strip().str.upper()
        df.loc[~df["state_code"].str.match(r"^[A-Z]{2}$", na=False), "state_code"] = None
    elif "houseNo" in df.columns:
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

    # Debug: Check what's in the sku column
    print(f"\nDEBUG - SKU column analysis:")
    print(f"  'sku' column exists: {'sku' in df.columns}")
    print(f"  'masterSku' column exists: {'masterSku' in df.columns}")
    if 'sku' in df.columns:
        print(f"  Sample sku values (first 10): {df['sku'].head(10).tolist()}")
        print(f"  SKU null count: {df['sku'].isna().sum()}")
    if 'masterSku' in df.columns:
        print(f"  Sample masterSku values (first 10): {df['masterSku'].head(10).tolist()}")
    print(f"  Sample product_key values (first 10): {df['product_key'].head(10).tolist()}")
    print(f"  product_key null count: {df['product_key'].isna().sum()}")
    print()

    print(f"Loaded {len(df)} rows from CSV")

    # 3) Create staging table (SQL Server compatible)
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'stg_order_export_raw') AND type in (N'U'))
            BEGIN
                CREATE TABLE stg_order_export_raw (
                  orderNo VARCHAR(100) NULL,
                  orderType VARCHAR(100) NULL,
                  orderRealStatus VARCHAR(50) NULL,
                  platformOrderNo VARCHAR(100) NULL,
                  commercePlatform VARCHAR(50) NULL,
                  name VARCHAR(255) NULL,
                  country VARCHAR(50) NULL,
                  city VARCHAR(120) NULL,
                  postalCode VARCHAR(20) NULL,
                  contactNo VARCHAR(60) NULL,
                  urgent_orders VARCHAR(50) NULL,
                  oneReference VARCHAR(120) NULL,
                  twoReference VARCHAR(120) NULL,
                  oneAddress VARCHAR(255) NULL,
                  twoAddress VARCHAR(255) NULL,
                  houseNo VARCHAR(50) NULL,
                  masterSku VARCHAR(100) NULL,
                  email VARCHAR(255) NULL,
                  companyName VARCHAR(255) NULL,
                  volume VARCHAR(50) NULL,
                  actualWeight VARCHAR(50) NULL,
                  serviceProvider VARCHAR(50) NULL,
                  trackNo VARCHAR(60) NULL,
                  expressDeliveryService VARCHAR(100) NULL,
                  outBoundSource VARCHAR(100) NULL,
                  logisticsStatus VARCHAR(120) NULL,
                  trackNoReason VARCHAR(255) NULL,
                  createTime DATETIME2 NULL,
                  submitTime DATETIME2 NULL,
                  deliveryTime DATETIME2 NULL,
                  pickingTime DATETIME2 NULL,
                  sku VARCHAR(120) NULL,
                  batch_number VARCHAR(120) NULL,
                  serial_number VARCHAR(120) NULL,
                  goodsNumber INT NULL,
                  inventory_type VARCHAR(120) NULL,
                  length VARCHAR(50) NULL,
                  width VARCHAR(50) NULL,
                  high VARCHAR(50) NULL,
                  remarks VARCHAR(255) NULL,
                  customer_id BIGINT NULL,
                  state_code CHAR(2) NULL,
                  volume_num FLOAT NULL,
                  actualWeight_num FLOAT NULL,
                  product_key VARCHAR(120) NULL,
                  load_ts DATETIME2 DEFAULT GETDATE()
                );
            END
        """))

        # Optional: truncate staging each run
        conn.execute(text("TRUNCATE TABLE stg_order_export_raw;"))

    # Load into staging
    staging_cols = [
        "orderNo","orderType","orderRealStatus","platformOrderNo","commercePlatform","name","country","city","postalCode",
        "contactNo","urgent_orders","oneReference","twoReference","oneAddress","twoAddress","houseNo","masterSku","email",
        "companyName","volume","actualWeight","serviceProvider","trackNo","expressDeliveryService","outBoundSource",
        "logisticsStatus","trackNoReason","createTime","submitTime","deliveryTime","pickingTime","sku","batch_number",
        "serial_number","goodsNumber","inventory_type","length","width","high","remarks",
        "customer_id","state_code","volume_num","actualWeight_num","product_key"
    ]
    for c in staging_cols:
        if c not in df.columns:
            df[c] = None

    # Truncate string columns to match staging schema lengths to avoid "right truncation" errors.
    col_max_map = {
        "orderNo": 100, "orderType": 100, "orderRealStatus": 50, "platformOrderNo": 100,
        "commercePlatform": 50, "name": 255, "country": 50, "city": 120, "postalCode": 20,
        "contactNo": 60, "urgent_orders": 50, "oneReference": 120, "twoReference": 120,
        "oneAddress": 255, "twoAddress": 255, "houseNo": 50, "masterSku": 100, "email": 255,
        "companyName": 255, "volume": 50, "actualWeight": 50, "serviceProvider": 50,
        "trackNo": 60, "expressDeliveryService": 100, "outBoundSource": 100,
        "logisticsStatus": 120, "trackNoReason": 255, "sku": 120, "batch_number": 120,
        "serial_number": 120, "inventory_type": 120, "length": 50, "width": 50, "high": 50,
        "remarks": 255, "state_code": 2, "product_key": 120
    }
    for col, max_len in col_max_map.items():
        if col in df.columns:
            mask = df[col].notna()
            # convert and slice only non-null values to preserve None/NaN
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].astype(str).str.slice(0, max_len)

    # Use pyodbc fast_executemany insert method to avoid parameter marker / batching issues
    df[staging_cols].to_sql("stg_order_export_raw", engine, if_exists="append", index=False, method=_mssql_fast_executemany, chunksize=1000)

    # Ensure dimension tables and fact table exist before upserts/joins
    with engine.begin() as conn:
        # dim_platform
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dim_platform') AND type in (N'U'))
            BEGIN
                CREATE TABLE dim_platform (
                    platform_id INT IDENTITY(1,1) PRIMARY KEY,
                    platform_name VARCHAR(50) NOT NULL UNIQUE
                );
            END
        """))
        # dim_product
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
        # dim_customer
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dim_customer') AND type in (N'U'))
            BEGIN
                CREATE TABLE dim_customer (
                    customer_id BIGINT PRIMARY KEY,
                    gender VARCHAR(20) NULL,
                    state_code CHAR(2) NULL,
                    postal_code VARCHAR(20) NULL
                );
            END
        """))
        # fact_sales - create a minimal table to match the ETL insert statement
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'fact_sales') AND type in (N'U'))
            BEGIN
                CREATE TABLE fact_sales (
                    fact_id INT IDENTITY(1,1) PRIMARY KEY,
                    order_id VARCHAR(100) NULL,
                    date_id DATE NULL,
                    product_id INT NULL,
                    customer_id BIGINT NULL,
                    platform_id INT NULL,
                    units INT NOT NULL DEFAULT 1,
                    revenue DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                    state_code CHAR(2) NULL
                );
            END
        """))

    # 4) Upsert dim_platform (SQL Server)
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO dim_platform (platform_name)
            SELECT DISTINCT s.commercePlatform
            FROM stg_order_export_raw s
            WHERE s.commercePlatform IS NOT NULL AND s.commercePlatform <> ''
              AND NOT EXISTS (SELECT 1 FROM dim_platform dp WHERE dp.platform_name = s.commercePlatform);
        """))
        print(f"Inserted {result.rowcount} new platforms into dim_platform")

    # 5) Insert dim_product (basic, SQL Server)
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO dim_product (main_sku_code, english_name, chinese_name, customer_code, category)
            SELECT DISTINCT s.product_key, NULL, NULL, NULL, NULL
            FROM stg_order_export_raw s
            WHERE s.product_key IS NOT NULL AND s.product_key <> ''
              AND NOT EXISTS (SELECT 1 FROM dim_product p WHERE p.main_sku_code = s.product_key);
        """))
        print(f"Inserted {result.rowcount} new products into dim_product")

    # 6) Insert dim_customer (SQL Server)
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO dim_customer (customer_id, gender, state_code, postal_code)
            SELECT DISTINCT s.customer_id, 'Unknown', s.state_code, s.postalCode
            FROM stg_order_export_raw s
            WHERE s.customer_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM dim_customer c WHERE c.customer_id = s.customer_id);
        """))
        print(f"Inserted {result.rowcount} new customers into dim_customer")

        # Update existing customers with state_code and postal_code from staging
        result = conn.execute(text("""
            UPDATE c
            SET c.state_code = s.state_code,
                c.postal_code = s.postalCode
            FROM dim_customer c
            INNER JOIN (
                SELECT DISTINCT customer_id, state_code, postalCode
                FROM stg_order_export_raw
                WHERE customer_id IS NOT NULL
            ) s ON c.customer_id = s.customer_id
            WHERE c.state_code IS NULL OR c.postal_code IS NULL;
        """))
        print(f"Updated {result.rowcount} existing customers with state/postal info")

    # 7) Ensure dim_date covers date range
    dt_col = FACT_DATE_SOURCE
    min_dt = df[dt_col].min() if dt_col in df.columns else None
    max_dt = df[dt_col].max() if dt_col in df.columns else None
    ensure_dim_date(engine, min_dt, max_dt)

    # 8) Insert fact_sales (SQL Server DATE conversion)
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            INSERT INTO fact_sales (
                order_id, date_id, product_id, customer_id, platform_id,
                units, revenue, state_code
            )
            SELECT
                r.orderNo AS order_id,
                CONVERT(date, r.{dt_col}) AS date_id,
                p.product_id,
                r.customer_id,
                pl.platform_id,
                COALESCE(r.goodsNumber, 1) AS units,
                0.00 AS revenue,
                r.state_code
            FROM stg_order_export_raw r
            LEFT JOIN dim_platform pl
                ON pl.platform_name = r.commercePlatform
            LEFT JOIN dim_product p
                ON p.main_sku_code = r.product_key
            WHERE r.{dt_col} IS NOT NULL
                AND r.commercePlatform IS NOT NULL
                AND r.product_key IS NOT NULL
                AND pl.platform_id IS NOT NULL
                AND p.product_id IS NOT NULL;
        """))
        print(f"Inserted {result.rowcount} rows into fact_sales")

    print("\nETL completed successfully.")

if __name__ == "__main__":
    main()

