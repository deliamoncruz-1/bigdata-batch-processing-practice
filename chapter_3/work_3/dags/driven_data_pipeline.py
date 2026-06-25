import random
import csv
import logging
import uuid
import polars as pl

from faker import Faker
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Configuración logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)


# Función para creación de datos
def create_data(locale: str) -> Faker:
    logging.info(
        f"Created synthetic data for {locale.split('_')[-1]} country code."
    )
    return Faker(locale)


# Función para generar un registro
def generate_record(fake: Faker) -> list:

    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = fake.phone_number()
    mac_address = fake.mac_address()
    ip_address = fake.ipv4()
    clabe = fake.iban()
    accessed_at = fake.date_time_between("-1y")

    session_duration = random.randint(0, 36_000)
    download_speed = random.randint(0, 1_000)
    upload_speed = random.randint(0, 800)
    consumed_traffic = random.randint(0, 2_000_000)

    return [
        person_name,
        user_name,
        email,
        personal_number,
        birth_date,
        address,
        phone_number,
        mac_address,
        ip_address,
        clabe,
        accessed_at,
        session_duration,
        download_speed,
        upload_speed,
        consumed_traffic
    ]


def write_to_csv() -> None:

    fake = create_data("es_MX")

    headers = [
        "person_name",
        "user_name",
        "email",
        "personal_number",
        "birth_date",
        "address",
        "phone_number",
        "mac_address",
        "ip_address",
        "clabe",
        "accessed_at",
        "session_duration",
        "download_speed",
        "upload_speed",
        "consumed_traffic"
    ]

    if str(date.today()) == "2026-06-09":
        rows = 100_372
    else:
        rows = random.randint(0, 1_101)

    with open(
        "/opt/airflow/data/raw_data.csv",
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.writer(file)

        writer.writerow(headers)

        for _ in range(rows):
            writer.writerow(generate_record(fake))

    logging.info(f"Written {rows} records to the CSV file.")


def add_id() -> None:

    df = pl.read_csv("/opt/airflow/data/raw_data.csv")

    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]

    df = df.with_columns(
        pl.Series("unique_id", uuid_list)
    )

    df.write_csv("/opt/airflow/data/raw_data.csv")

    logging.info("Added UUID to the dataset.")


def update_datetime(run_type: str) -> None:

    if run_type == "next":

        current_time = datetime.now().replace(
            microsecond=0
        )

        yesterday_time = str(
            current_time - timedelta(days=1)
        )

        df = pl.read_csv("/opt/airflow/data/raw_data.csv")

        df = df.with_columns(
            pl.lit(yesterday_time).alias("accessed_at")
        )

        df.write_csv("/opt/airflow/data/raw_data.csv")

        logging.info(
            "Updated accessed timestamp."
        )


def save_raw_data():

    logging.info(
        f"Started batch processing for {date.today()}."
    )

    if str(date.today()) == "2026-05-20":
        run_type = "first"
    else:
        run_type = "next"

    write_to_csv()

    add_id()

    update_datetime(run_type)

    logging.info(
        f"Finished batch processing {date.today()}."
    )

# Define the default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Define the DAG.
dag = DAG(
    'extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval='* 7 * * *',
    start_date=datetime(2024, 9, 22),
    catchup=False,
)

# Define extract raw data task.
extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

# Define create raw schema task.
create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
)

# Define create raw table task
create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            person_name VARCHAR(100),
            user_name VARCHAR(100),
            email VARCHAR(100),
            personal_number NUMERIC,
            birth_date VARCHAR(100),
            address VARCHAR(500),
            phone_number VARCHAR(100),
            mac_address VARCHAR(200),
            ip_address VARCHAR(100),
            clabe VARCHAR(100),
            accessed_at TIMESTAMP,
            session_duration INT,
            download_speed INT,
            upload_speed INT,
            consumed_traffic INT,
            unique_id VARCHAR(100)
        );
    """,
    dag=dag
)

# Define load CSV data into the table task.
load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
        COPY driven_raw.raw_batch_data(
            person_name,
            user_name,
            email,
            personal_number,
            birth_date,
            address,
            phone_number,
            mac_address,
            ip_address,
            clabe,
            accessed_at,
            session_duration,
            download_speed,
            upload_speed,
            consumed_traffic,
            unique_id
        )
        FROM '/opt/airflow/data/raw_data.csv'
        DELIMITER ','
        CSV HEADER;
    """
)

# Define staging dbt models run task.
run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
)

# Define trusted dbt models run task.
run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
)

# Set the task in the DAG
[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task

create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task

run_dbt_staging_task >> run_dbt_trusted_task