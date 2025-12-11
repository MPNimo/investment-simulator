import os
import sqlite3
from dataclasses import dataclass
from typing import List
import pandas as pd

BASE_DIR = os.path.dirname("load_to_sql.ipynb")
DB_PATH = os.path.join(BASE_DIR, "..", "db", "investments.db")
PROCESSED_DIR = os.path.join(BASE_DIR, "..", "data", "processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# Parámetros de simulación
INITIAL_CAPITAL = 10000.0 # Capital inicial lump sum
MONTHLY_CONTRIBUTION = 300.0 # aporte mensual para DCA (Dollar-Cost Averaging)
INVESTMENT_DAY = 5  # Intentaremos invertir el día 5 de cada mes


@dataclass
class StrategyResult:
    df_equity: pd.DataFrame
    name: str
    ticker: str


def get_price_series(ticker: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Obtiene la serie de precios ajustados para un ticker desde la base de datos.
    """
    query = """
    SELECT Date, [Adj Close] AS adj_close
    FROM prices
    WHERE Ticker = ?
    ORDER BY Date
    """
    df = pd.read_sql_query(query, conn, params=(ticker,), parse_dates=["Date"])
    return df


def simulate_lump_sum(prices: pd.DataFrame, ticker: str) -> StrategyResult:
    """
    Estrategia Lump Sum: se invierte todo el capital inicial el primer día.
    """
    df = prices.copy().reset_index(drop=True)

    # Comprar todo el primer día
    first_price = df.loc[0, "adj_close"]
    units = INITIAL_CAPITAL / first_price

    df["strategy"] = "Lump Sum"
    df["ticker"] = ticker
    df["units"] = units
    df["invested_amount"] = INITIAL_CAPITAL
    df["portfolio_value"] = df["units"] * df["adj_close"]

    return StrategyResult(df_equity=df[["Date", "ticker", "strategy", "units", "invested_amount", "portfolio_value"]],
                          name="Lump Sum",
                          ticker=ticker)


def _get_monthly_investment_dates(prices: pd.DataFrame) -> List[pd.Timestamp]:
    """
    Dado un DataFrame de precios diarios, determina las fechas de inversión mensuales.
    Se intenta invertir el INVESTMENT_DAY; si no existe (festivo/fin de semana),
    se usa el siguiente día disponible.
    """
    prices = prices.copy()
    prices["year"] = prices["Date"].dt.year
    prices["month"] = prices["Date"].dt.month

    invest_dates = []

    for (year, month), group in prices.groupby(["year", "month"]):
        # Ordenamos por fecha
        group = group.sort_values("Date")
        # Filtrar fechas >= día objetivo
        cand = group[group["Date"].dt.day >= INVESTMENT_DAY]
        if cand.empty:
            # Si no hay fecha >= día objetivo, usamos el último día hábil del mes
            invest_date = group["Date"].max()
        else:
            invest_date = cand["Date"].min()

        invest_dates.append(invest_date)

    return sorted(invest_dates)


def simulate_dca(prices: pd.DataFrame, ticker: str) -> StrategyResult:
    """
    Estrategia DCA: se invierte una cantidad fija cada mes.
    """
    df = prices.copy().reset_index(drop=True)
    df["strategy"] = "DCA"
    df["ticker"] = ticker

    # Fechas en las que vamos a invertir
    invest_dates = _get_monthly_investment_dates(df)

    total_units = 0.0
    total_invested = 0.0

    units_list = []
    invested_list = []
    value_list = []

    for idx, row in df.iterrows():
        current_date = row["Date"]
        price = row["adj_close"]

        # Si la fecha actual es una fecha de inversión, compramos
        if current_date in invest_dates:
            buy_units = MONTHLY_CONTRIBUTION / price
            total_units += buy_units
            total_invested += MONTHLY_CONTRIBUTION

        portfolio_value = total_units * price

        units_list.append(total_units)
        invested_list.append(total_invested)
        value_list.append(portfolio_value)

    df["units"] = units_list
    df["invested_amount"] = invested_list
    df["portfolio_value"] = value_list

    return StrategyResult(df_equity=df[["Date", "ticker", "strategy", "units", "invested_amount", "portfolio_value"]],
                          name="DCA",
                          ticker=ticker)


def run_simulations(tickers: List[str]) -> pd.DataFrame:
    """
    Ejecuta las simulaciones para una lista de tickers y devuelve
    un DataFrame combinado listo para guardar.
    """
    conn = sqlite3.connect(DB_PATH)

    all_results = []

    for ticker in tickers:
        print(f"Simulando estrategias para {ticker}...")

        prices = get_price_series(ticker, conn)

        lump = simulate_lump_sum(prices, ticker)
        dca = simulate_dca(prices, ticker)

        all_results.append(lump.df_equity)
        all_results.append(dca.df_equity)

    conn.close()

    df_all = pd.concat(all_results, ignore_index=True)
    return df_all


def main():
    # Puedes fijar aquí los tickers que quieres simular.
    # Opcional: leerlos dinámicamente de la BBDD.
    tickers = ["AAPL", "MSFT", "^GSPC"]

    df_equity_curves = run_simulations(tickers)

    # Guardar curvas de equity
    output_path = os.path.join(PROCESSED_DIR, "equity_curves.csv")
    df_equity_curves.to_csv(output_path, index=False)
    print(f"Curvas de equity guardadas en {output_path}")


if __name__ == "__main__":
    main()