from datetime import timedelta

import psycopg2
import yaml

# Загрузка конфигурации из файла config.yml
with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

# Получение параметров подключения к базе данных
TARGET_DB_HOST = config["target_db"]["host"]
TARGET_DB_PORT = config["target_db"]["port"]
TARGET_DB_USER = config["target_db"]["user"]
TARGET_DB_PASSWORD = config["target_db"]["password"]
TARGET_DB_NAME = config["target_db"]["dbname"]


def get_loaded_date() -> str:
    """Функция для получения даты текущей загрузки"""
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


if __name__ == "__main__":
    LOAD_DATE = get_loaded_date()
    print(LOAD_DATE)
