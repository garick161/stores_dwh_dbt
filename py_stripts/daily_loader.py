from datetime import timedelta

import psycopg2
import yaml

# Загрузка конфигурации из файла config.yml
with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

# Получение параметров подключения к базе данных
SOURCE_DB_HOST = config["source_db"]["host"]
SOURCE_DB_PORT = config["source_db"]["port"]
SOURCE_DB_USER = config["source_db"]["user"]
SOURCE_DB_PASSWORD = config["source_db"]["password"]
SOURCE_DB_NAME = config["source_db"]["dbname"]


def get_last_loaded_date():
    try:
        connection = psycopg2.connect(host=SOURCE_DB_HOST, port=SOURCE_DB_PORT, user=SOURCE_DB_USER, password=SOURCE_DB_PASSWORD, dbname=SOURCE_DB_NAME)
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(calday) FROM public.bills_head;")
        res = cursor.fetchone()[0]

    except Exception as e:
        print(f"Ошибка: {e}")
    connection.close()
    return res


if __name__ == "__main__":
    LOAD_DATE = get_last_loaded_date()
    if LOAD_DATE:
        LOAD_DATE = (LOAD_DATE + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        LOAD_DATE = "2021-01-01"
    print(LOAD_DATE)
