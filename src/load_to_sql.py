
import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname("download_data.ipynb")

RAW_DATA_PATH = os.path.join(BASE_DIR, "..", "data", "raw", "all_prices.csv")
DB_PATH = os.path.join(BASE_DIR, "..", "db", "investments.db")

os.makedirs("db", exist_ok=True)


def main():
    # Leer datos CSV
    df = pd.read_csv(RAW_DATA_PATH, parse_dates=["Date"])

    # Conectar a SQLite
    conn = sqlite3.connect(DB_PATH)

    # Guardar en una tabla llamada 'prices'
    df.to_sql("prices", conn, if_exists="replace", index=False)

    # Crear índice para mejorar consultas
    with conn:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices (Ticker, Date);")

    conn.close()
    print(f"Datos cargados en la base de datos {DB_PATH}")


if __name__ == "__main__":
    main()