from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
import requests
from bs4 import BeautifulSoup
import csv
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, String, MetaData

@dag(
    params={
        "url": Param("https://sport.detik.com/sepakbola/", description="URL untuk scraping berita"),
        "filename": Param("", description="Nama file CSV untuk menyimpan hasil scrapping"),
        "user": Param("", description="The user or person triggering the DAG"),
    },
)
def assignment_airflow():

    # Web scraping
    @task()
    def extract_web(param1, filename):
        print(param1, type(param1))
        
        response = requests.get(param1)
        soup = BeautifulSoup(response.content, "html.parser")

        source = [
            {"title": h3.find("a")["dtr-ttl"], "url": h3.find("a")["href"] if h3.find("a") else None}
            for h3 in soup.find_all("h3")
        ]

        df = pd.DataFrame(source)

        folder_path = "/opt/airflow/data"
        os.makedirs(folder_path, exist_ok=True)

        # Menyimpan file
        file_name = os.path.join(folder_path, f"{filename}.csv")
        df.to_csv(file_name, index=False)

        print(f"Data berhasil disimpan ke {file_name}")
        return file_name

    # Membaca CSV
    @task()
    def extract_from_csv(filename):
        print(f"Reading from CSV: {filename}")
        with open(filename, "r", newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
        return data

    # Load ke SQLite
    @task()
    def load_sqlite(data, table_name):
        df = pd.DataFrame(data)

        engine = sa.create_engine(f"sqlite:///data/{table_name}.sqlite")

        with engine.begin() as conn:
            df.to_sql(table_name, conn, index=False, if_exists="replace")

        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name} LIMIT 10"
            result_df = pd.read_sql(query, conn)

        print(f"Menampilkan table {table_name}:")
        print(result_df)

    # Branch_operator
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")
    extract_from_json = EmptyOperator(task_id="extract_from_json")

    web_data = extract_web(param1="{{params.url}}", filename="{{params.filename}}")
    csv_data = extract_from_csv(filename=web_data)
    load_data = load_sqlite(data=csv_data, table_name="{{params.filename}}")

    # Task order
    start_task >> web_data >> [extract_from_json, csv_data] >> load_data >> end_task

assignment_airflow = assignment_airflow()