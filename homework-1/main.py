"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2  # Библиотека для подключения к базе данных в Postgres
import csv
import os

# Константы содержащие имена файлов с данными.
EMP = 'employees_data.csv'
CUST = 'customers_data.csv'
ORD = 'orders_data.csv'

# Константа с параметрами для подключения к базе данных 'north'.
CONNECT = psycopg2.connect(host="localhost", database="north", user="postgres", password="12345")


def reader_csv(path):
    """Возвращает список в виде словарей из данных 'csv' файлов"""
    result = []
    data = os.path.join(os.path.dirname(__file__), f"north_data/{path}")
    with open(data, 'r', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for rowe in reader:
            result.append(rowe)
    return result


if __name__ == "__main__":

    empl_csv = reader_csv(EMP)
    cust_csv = reader_csv(CUST)
    ord_csv = reader_csv(ORD)

# Скрипт, который заполняет созданные таблицы данными из 'north_data'.
    with CONNECT as conn:
        with conn.cursor() as cur:
            for row in empl_csv:
                cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                            (row["employee_id"],
                             row["first_name"],
                             row["last_name"],
                             row["title"],
                             row["birth_date"],
                             row["notes"],))
            for row in cust_csv:
                cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                            (row["customer_id"],
                             row["company_name"],
                             row["contact_name"],))
            for row in ord_csv:
                cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                            (row["order_id"],
                             row["customer_id"],
                             row["employee_id"],
                             row["order_date"],
                             row["ship_city"],))
    conn.close()
