import pandas as pd
import psycopg2
import logging
from pathlib import Path
from typing import Dict, Final, Any
from decouple import config


logging.basicConfig(level=logging.INFO, format="%(message)s")

DB_CONFIG: Final[dict[str, Any]] = {
    "host": config("LOCALHOST"),
    "port": config("PORT"),
    "dbname": config("DBNAME"),
    "user": config("USER"),
    "password": config("PASSWORD"),
}

DATA_DIR: Final[Path] = Path("../data/dwh")
TABLES_MAP: Final[Dict[str, str]] = {
    "prod.dim_customers (customer_id, region, registration_date)": "dim_customers.csv",
    "prod.dim_orders (order_id, order_date, customer_id, product_id, quantity)": "dim_orders.csv",
    "prod.dim_products (product_id, category, price, cost_price)": "dim_products.csv",
    "prod.fact_order (order_id, order_date, customer_id, product_id, quantity, category, price, cost_price, region, registration_date)": "fact_order.csv",
}


def upload_data():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for structure, file_name in TABLES_MAP.items():
                file_path = DATA_DIR / file_name
                df = pd.read_csv(file_path)
                
                total_rows = df.shape[0]
                step = max(1, total_rows // 100)
                
                logging.info(f"Load in {structure.split(' ')[0]}...")
                
                i = 0
                while i < total_rows:
                    print(i, end='\r')
                    values = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                    query = f"insert into {structure} values {values};"
                    
                    cur.execute(query)
                    conn.commit()                        
                    i += step + 1
                
                logging.info("Done")


if __name__ == "__main__":
    upload_data()