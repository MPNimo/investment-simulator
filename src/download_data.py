#%%

import os
from datetime import datetime
import pandas as pd
import yfinance


#%%

# Parámetros básicos del proyecto
TICKERS = ["AAPL", "MSFT", "^GSPC"]  # Puedes cambiar/añadir
START_DATE = "2015-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

DATA_DIR = os.path.join("data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)


#%%

def download_ticker(ticker: str) -> pd.DataFrame:
    """
    Descarga datos diarios de un ticker usando yfinance.
    Devuelve un DataFrame con las columnas estándar de OHLCV.
    """
    print(f"Descargando datos de {ticker}...")
    df = yfinance.download(ticker, start=START_DATE, end=END_DATE)
    df.reset_index(inplace=True)
    df["Ticker"] = ticker
    return df


#%%

def main():
    all_data = []

    for ticker in TICKERS:
        df_ticker = download_ticker(ticker)
        all_data.append(df_ticker)

        # Guardar cada ticker por separado (opcional)
        path_ticker = os.path.join(DATA_DIR, f"{ticker.replace('^','')}.csv")
        df_ticker.to_csv(path_ticker, index=False)
        print(f"Guardado {path_ticker}")

    # Unir todo en un solo CSV
    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)
        all_path = os.path.join(DATA_DIR, "all_prices.csv")
        df_all.to_csv(all_path, index=False)
        print(f"Datos combinados guardados en {all_path}")


if __name__ == "__main__":
    main()