from datetime import timedelta

import pandas as pd
import psycopg2
import yaml
from sqlalchemy import create_engine

# Получим данные о подключению к БД из конфигурационного файла
with open("../.config.yml", "r") as file:
    config = yaml.safe_load(file)

TARGET_DB_HOST = config["target_db"]["host"]
TARGET_DB_PORT = config["target_db"]["port"]
TARGET_DB_USER = config["target_db"]["user"]
TARGET_DB_PASSWORD = config["target_db"]["password"]
TARGET_DB_NAME = config["target_db"]["dbname"]


def get_loaded_date() -> str:
    """Функция для получения даты текущей загрузки в базе данных"""
    try:
        connection = psycopg2.connect(host=TARGET_DB_HOST, port=TARGET_DB_PORT, user=TARGET_DB_USER, password=TARGET_DB_PASSWORD, dbname=TARGET_DB_NAME)
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(calday) FROM store_sales.bills_head;")
        last_date = cursor.fetchone()[0]
    except Exception as e:
        print(f"Ошибка: {e}")
    connection.close()

    if last_date:
        load_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        load_date = "2021-01-01"

    return load_date


def loader(load_date: str) -> None:
    """Функция загрузки таблиц на текущую дату"""

    tables = ["bills_item", "bills_head", "traffic", "coupons", "promos"]
    dfs = {}  # example {'name_df': pd.DataFrame}
    connection_string = f"postgresql+psycopg2://{TARGET_DB_USER}:{TARGET_DB_PASSWORD}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"
    engine = create_engine(connection_string)

    for table_name in tables:
        if table_name == "traffic":
            dfs[table_name] = pd.read_csv(f"../datasets/{table_name}.csv", parse_dates=["calday"], date_format="%d.%m.%Y")
            dfs[table_name] = dfs[table_name][dfs[table_name]["calday"] == load_date]
            dfs[table_name].to_sql(name=table_name, schema="store_sales", con=engine, if_exists="append", index=False)
        elif table_name == "promos":
            dfs[table_name] = pd.read_csv(f"../datasets/{table_name}.csv")
            # Выбираем только актуальные акции на сегодняшний день. Могут быть удалены или добавлены
            actual_promos_coupons = dfs[table_name].groupby("promo_id", as_index=False).first()
            actual_promos_idx = dfs[table_name].merge(actual_promos_coupons, on="promo_id", how="left").dropna().index
            dfs[table_name] = dfs[table_name].iloc[actual_promos_idx]
            dfs[table_name].to_sql(name=table_name, schema="store_sales", con=engine, if_exists="replace", index=False)
        else:
            dfs[table_name] = pd.read_csv(f"../datasets/{table_name}.csv", parse_dates=["calday"])
            dfs[table_name] = dfs[table_name][dfs[table_name]["calday"] == load_date]
            dfs[table_name].to_sql(name=table_name, schema="store_sales", con=engine, if_exists="append", index=False)


if __name__ == "__main__":
    load_date = get_loaded_date()
    print(f"Дата загрузки: {load_date}")
    loader(load_date)
