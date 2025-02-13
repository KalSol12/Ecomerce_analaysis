import pandas as pd
import psycopg2

# Load dataset (en/sure correct file path)
df = pd.read_csv("Data/kz.csv")

# Check dataset structure
print(f"Total Rows: {df.shape[0]}")
print(df.head())
print(df.info())

# Check missing values
print("Missing Values Before Handling:\n", df.isnull().sum())
# Remove duplicate rows and reset index
print(f"Duplicate Rows Before: {df.duplicated().sum()}")
df = df.drop_duplicates().reset_index(drop=True)
print(f"Duplicate Rows After: {df.duplicated().sum()}")
# Convert event_time to datetime
df["event_time"] = pd.to_datetime(df["event_time"])
# Convert user_id safely to integer
df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce").fillna(-1).astype("Int64")
# Handle missing values
df = df.assign(
    category_code=df["category_code"].fillna("Unknown"),
    brand=df["brand"].fillna("Unknown"),
    price=df["price"].fillna(df["price"].median()),
    category_id=df["category_id"].fillna(df["category_id"].mode().iloc[0] if not df["category_id"].mode().empty else -1),
    user_id=df["user_id"].fillna(-1),
) 

# Verify missing values are handled
print("Missing Values After Handling:\n", df.isnull().sum())
# Save cleaned dataset to the specified folder
output_path = "/Data/cleaned_dataset.csvcleaned_dataset.csv"
df.to_csv(output_path, index=False)
print(f"✅ Cleaned dataset saved successfully at {output_path}!")
# Save cleaned dataset
df.to_csv("cleaned_dataset.csv", index=False)
print("✅ Dataset cleaned and saved successfully!")

# ------------------------------ #
# ✅ Connect to PostgreSQL
# ------------------------------ #

# Database connection parameters
DB_NAME = "ecommerce_db"
DB_USER = "postgres"  # Default username
DB_PASSWORD = "kal@12"  # Replace with your actual password
DB_HOST = "localhost"  # Use "127.0.0.1" if needed
DB_PORT = "5432"  # Default PostgreSQL port

try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL successfully!")

except Exception as e:
    print("❌ Connection failed:", e)

# ------------------------------ #
# ✅ Create Table in PostgreSQL
# ------------------------------ #

create_table_query = """
CREATE TABLE IF NOT EXISTS ecommerce_data (
    event_time TIMESTAMP,
    order_id BIGINT PRIMARY KEY,
    product_id BIGINT,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price NUMERIC(10,2),
    user_id BIGINT
);
"""
# Execute create table query
cursor.execute(create_table_query)
conn.commit()
print("✅ Table 'ecommerce_data' created successfully!")

# ------------------------------ #
# ✅ Insert Data into PostgreSQL
# ------------------------------ #

# Convert data types to ensure compatibility
df["event_time"] = pd.to_datetime(df["event_time"])  # Convert to datetime
df["order_id"] = df["order_id"].astype("Int64")  # Convert to integer
df["product_id"] = df["product_id"].astype("Int64")
df["category_id"] = df["category_id"].fillna(-1).astype("Int64")  # Fill NaN with -1
df["category_code"] = df["category_code"].astype(str)  # Ensure it's a string
df["brand"] = df["brand"].astype(str)  # Ensure it's a string
df["price"] = df["price"].astype(float)  # Ensure numeric type
df["user_id"] = df["user_id"].fillna(-1).astype("Int64")  # Fill NaN with -1

# Insert data row by row (Handling NULL values)
for _, row in df.iterrows():
    try:
        cursor.execute("""
            INSERT INTO ecommerce_data (event_time, order_id, product_id, category_id, category_code, brand, price, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (
            row["event_time"].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row["event_time"]) else None,
            int(row["order_id"]) if pd.notna(row["order_id"]) else None,
            int(row["product_id"]) if pd.notna(row["product_id"]) else None,
            int(row["category_id"]) if pd.notna(row["category_id"]) else None,
            row["category_code"] if pd.notna(row["category_code"]) else "Unknown",
            row["brand"] if pd.notna(row["brand"]) else "Unknown",
            float(row["price"]) if pd.notna(row["price"]) else 0.0,
            int(row["user_id"]) if pd.notna(row["user_id"]) else None
        ))
    except Exception as e:
        print(f"❌ Error inserting row: {row}")
        print("Error:", e)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()
print("✅ Data successfully loaded into PostgreSQL!")
