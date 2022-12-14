import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sql import SQLThresholdCheckOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain

from tools.move_data import create_temp_file, get_df_from_file


def extract_rates_from_db(
        conn_id_hook: str,
        end_date_minus_10_days: str, 
        out_file_key: str, 
        **context
) -> None:
    """Извлекает курсы валют из базы данных."""
    pg_hook = PostgresHook(conn_id_hook)
    sql = (
        f"""
         SELECT event_date, currency, rate 
         FROM exchange_rates 
         WHERE event_date >= date('{end_date_minus_10_days}')
         """
    )

    data = pg_hook.get_pandas_df(sql,  parse_dates=['event_date'])

    create_temp_file(
        data=data, key_xcoms=out_file_key, type_file='parquet', **context
    )


def _download_blob(bucket: str, hook: GCSHook, blob_name: str) -> pd.DataFrame:
    """Выгружает blob, сохраняет данные в DataFrame."""
    from io import BytesIO
    columns = [
        'Order Charged Date',
        'Order Charged Timestamp',
        'Financial Status',
        'Product ID',
        'Product Type',
        'SKU ID',
        'Currency of Sale',
        'Item Price',
        'Charged Amount',
        'City of Buyer',
        'State of Buyer',
        'Postal Code of Buyer',
        'Country of Buyer'
    ]
    blob_content = hook.download(bucket_name=bucket, object_name=blob_name)
    data = pd.read_csv(
        BytesIO(blob_content),
        usecols=columns,
        encoding='utf-8',
        compression='zip'
    )
    return data


def extract_from_store_magic_land(
        conn_id_hook: str,
        start_date: str,
        end_date: str,
        end_date_minus_10_days: str,
        bucket_name: str,
        out_file_key: str,
        **context
) -> None:
    """Извлекает данные из google cloud."""
    import pandas as pd
    gcp_hook = GCSHook(conn_id_hook)
    blobs_names = gcp_hook.list(
        bucket_name=bucket_name,
        prefix='sales/'
    )
    data = pd.DataFrame()
    for blob_name in blobs_names:
        if start_date in blob_name or end_date in blob_name:
            data_temp = _download_blob(bucket_name, gcp_hook, blob_name)
            data = pd.concat([data, data_temp], ignore_index=True)

    data = data.loc[data['Order Charged Date'] >= end_date_minus_10_days]

    create_temp_file(
        data=data, key_xcoms=out_file_key, type_file='parquet', **context
    )


def transform_and_convert_data(
        in_file_key_rates: str,
        in_file_key_store: str,
        out_file_key: str,
        **context
) -> None:
    """Приводит данные к стандартному виду."""
    rates = get_df_from_file(in_file_key_rates, type_file='parquet', **context)
    store = get_df_from_file(in_file_key_store, type_file='parquet', **context)

    store.rename(
        columns={
            'Order Charged Date': 'event_date',
            'Order Charged Timestamp': 'event_timestamp',
            'Financial Status': 'fin_status',
            'Product ID': 'product_id',
            'Product Type': 'product_type',
            'SKU ID': 'sku_id',
            'Currency of Sale': 'currency',
            'Item Price': 'item_price_usd',
            'Charged Amount': 'charged_amount_usd',
            'City of Buyer': 'city_b',
            'State of Buyer': 'state_b',
            'Postal Code of Buyer': 'postal_code_b',
            'Country of Buyer': 'country_b'
        },
        inplace=True
    )
    store['product_id'] = store['product_id'].str.replace('com.dominigames.', '')
    store['sku_id'] = store['sku_id'].str.replace('com.dominigames.', '')
    store['sku_id'] = store['sku_id'].str.replace('.unlock', '')
    store['charged_amount_usd'] = store['charged_amount_usd'].str.replace(',', '')
    store['charged_amount_usd'] = store['charged_amount_usd'].astype('float')
    store['item_price_usd'] = store['item_price_usd'].str.replace(',', '')
    store['item_price_usd'] = store['item_price_usd'].astype('float')
    data = pd.merge(
        store,
        rates,
        on=['event_date', 'currency'],
        how='left'
    )
    data['item_price_usd'] = data['item_price_usd'] / data['rate']
    data['charged_amount_usd'] = data['charged_amount_usd'] / data['rate']
    data.drop('rate', axis=1, inplace=True)

    create_temp_file(data=data, key_xcoms=out_file_key, **context)


def load_in_store_magic_land(**context) -> None:
    """Загружает данные в базу dominigames таблицу gp_store_magic_land."""
    ti = context['ti']
    report_date = context['10_days_ago']
    data_store_file = ti.xcom_pull(key='transform_convert_currency')
    pg_hook = context['params']['hook']

    sql = (
        f"""
         DELETE FROM gp_sales_store_magic_land 
         WHERE event_date >= date('{report_date}');
         COPY gp_sales_store_magic_land FROM STDIN CSV HEADER DELIMITER ';'
         """
    )

    pg_hook.copy_expert(sql, data_store_file)


with DAG(
        dag_id='gp_sales_store_magic_land',
        start_date=datetime(2022, 5, 1),
        schedule_interval='@once',
        catchup=False
) as dag:
    start_date = "{{ (execution_date - macros.timedelta(days=30)).strftime('%Y%m') }}"
    end_date = "{{ execution_date.strftime('%Y%m') }}"
    end_date_minus_10_days = "{{ (execution_date - macros.timedelta(days=10)).strftime('%Y-%m-%d') }}"

    pg_hook = PostgresHook('postgres_sources')

    extract_rates_from_db_task = PythonOperator(
        task_id='extract_rates_from_db',
        python_callable=extract_rates_from_db,
        params={'hook': pg_hook},
        op_kwargs={
            'conn_id_hook': 'postgres_sources',
            'end_date_minus_10_days': end_date_minus_10_days,
            'out_file_key': 'extract_rates_from_db'
        }
    )

    extract_from_store_magic_land_task = PythonOperator(
        task_id='extract_from_store_magic_land',
        python_callable=extract_from_store_magic_land,
        op_kwargs={
            'conn_id_hook': 'gp_sales_store_magic_land',
            'start_date': start_date,
            'end_date': end_date,
            'end_date_minus_10_days': end_date_minus_10_days,
            'bucket_name': 'pubsite_prod_5083592531648467308',
            'out_file_key': 'extract_from_store_magic_land'
        }
    )

    transform_and_convert_data_task = PythonOperator(
        task_id='transform_and_convert_data',
        python_callable=transform_and_convert_data,
        op_kwargs={
            'in_file_key_rates': "extract_rates_from_db",
            'in_file_key_store': "extract_from_store_magic_land",
            'out_file_key': 'transform_and_convert_data'
        }
    )

    load_in_postgres_task = CopyPostgresOperator(
        task_id='load_in_postgres_for',
        xcom_key_file_path='transform_and_convert_data',
        postgres_conn_id='postgres_dominigames',
        sql=f"""
             DELETE FROM gp_sales_store_magic_land 
             WHERE event_date >= '{end_date_minus_10_days}';
             COPY gp_sales_store_magic_land FROM STDIN CSV HEADER DELIMITER ';'
            """
    )

    load_in_postgres = PythonOperator(
        task_id='load_in_store_magic_land',
        python_callable=load_in_store_magic_land,
        params={'hook': pg_hook},
        op_kwargs=op_kwargs
    )

    chain(
        extract_rates_from_db_task,
        extract_magic_land,
        transform_unification,
        transform_convert,
        load_in_postgres
    )
