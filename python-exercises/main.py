# main.py
from source.db import get_connection, write_df_to_db
from source.extract import read_csv, read_csv_with_pd
from source.logging import logger
from source.transform import convert_to_numeric
from source.utils import drop_duplicates, to_lowercase
import pandas as pd
 
csv_file = "source/sales.csv"
 
def main():
    # ---- Tu código original comentado ----
    # print(read_csv(csv_file))
    # sales_df = read_csv_with_pd(csv_file)
    # sales_df["Price"] = convert_to_numeric(sales_df["Price"])
    # sales_df["Product"] = to_lowercase(sales_df["Product"])
    # sales_df["Total"] = sales_df["Price"] * sales_df["Quantity"]
    # clean_sales_df = drop_duplicates (sales_df)
    # print(clean_sales_df.head())
 
    # ---- Nuevo código para insertar en la BD ----
    # Crear DataFrame simple para probar
    df = pd.read_csv(csv_file)
    df = df.drop_duplicates()
    df["Product"] = df["Product"].str.strip().str.lower()
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Total"] = df["Price"] * df["Quantity"]
 
    # Guardar el DataFrame en la base de datos
    write_df_to_db(df, "sales")
 
    # Conexión normal (ya la tenías)
    print(get_connection())
 
if __name__ == "__main__":
    logger.info("Application started.")
    main()
    logger.info("Application finished.")
 