from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from google.api_core.exceptions import NotFound
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
import logging
from time import sleep
from typing import Dict
import json

from custom_operators.upgrade_check_operators import ClickHouseSQLThresholdCheckOperator
from tools.move_data import create_temp_file, get_df_from_file


def extract_data_from_big_query(
        report_date: str,
        table_name: str,
        bg_conn_id: str,
        location: str,
        out_file_key: str,
        **context
) -> None:
    """Извлекает данные из bigQuery."""
    bq_hook = BigQueryHook(bg_conn_id, location=location)
    report_date = report_date.replace('-', '')
    sql = f"""
           SELECT event_date, 
                  event_timestamp, 
                  event_name, 
                  event_params,
                  event_previous_timestamp, 
                  event_value_in_usd, 
                  event_bundle_sequence_id, 
                  event_server_timestamp_offset,
                  user_id, 
                  user_pseudo_id, 
                  user_properties, 
                  user_first_touch_timestamp,
                  user_ltv, 
                  device, 
                  geo, 
                  app_info, 
                  traffic_source,
                  stream_id, 
                  platform
           FROM (SELECT *, 
                        row_number() over(partition by 
                                                    event_date,
                                                    event_timestamp,
                                                    event_name,
                                                    event_previous_timestamp,
                                                    event_bundle_sequence_id,
                                                    user_id,
                                                    user_pseudo_id,
                                                    user_first_touch_timestamp,
                                                    stream_id 
                                          ) as num
                 FROM {table_name}{report_date}
            ) as sub
           WHERE num = 1
           """

    client = bq_hook.get_client()
    data = client.query(sql).to_dataframe()

    create_temp_file(
        data=data, key_xcoms=out_file_key, type_file='parquet', **context
    )


def _to_normal_json(cell):
    params = dict()
    for dict_i in cell:
        key = dict_i['key']
        if key == 'AB_TESTS_VALUES':
            try:
                params[dict_i['value']['string_value'].split('"')[
                    1]] = int(
                    dict_i['value']['string_value'].split('"')[-2])
            except Exception as error:
                params[dict_i['key']] = np.NAN
                logging.info(error)
        elif 'firebase_exp_' in key:
            try:
                params[dict_i['key']] = int(
                    dict_i['value']['string_value'])
            except Exception as error:
                logging.info(error)
        elif key == 'ga_session_id':
            params[dict_i['key']] = int(dict_i['value']['int_value'])
        elif key == 'ga_session_number':
            params[dict_i['key']] = int(dict_i['value']['int_value'])
        elif key == 'first_open_time':
            params[dict_i['key']] = int(dict_i['value']['int_value'])
        else:
            for value in dict_i['value'].values():
                if value != None:
                    params[key] = value
    params = json.dumps(params)
    return params


def transform_unification_data(
        in_file_key: str,
        out_file_key: str,
        **context
) -> None:
    """Преобразует данные к стандартному виду."""
    data = get_df_from_file(in_file_key, type_file='parquet', **context)

    data = data[[
        'event_date',
        'event_timestamp',
        'event_name',
        'event_params',
        'event_previous_timestamp',
        'event_value_in_usd',
        'event_bundle_sequence_id',
        'event_server_timestamp_offset',
        'user_id',
        'user_pseudo_id',
        'user_properties',
        'user_first_touch_timestamp',
        'user_ltv',
        'device',
        'geo',
        'app_info',
        'traffic_source',
        'stream_id',
        'platform'
    ]]

    data['event_date'] = pd.to_datetime(data['event_date'])
    data['event_timestamp'] = pd.to_datetime(data['event_timestamp'], unit='us')

    data['user_first_touch_timestamp'] = pd.to_datetime(
        data['user_first_touch_timestamp'], unit='us'
    )

    data_app_info = data['app_info'].apply(lambda row: row['id'])
    data.insert(1, 'app_info__id', data_app_info)

    data['device'] = data['device'].apply(json.dumps)
    data['geo'] = data['geo'].apply(json.dumps)
    data['app_info'] = data['app_info'].apply(json.dumps)
    data['traffic_source'] = data['traffic_source'].apply(json.dumps)
    data['user_ltv'] = data['user_ltv'].apply(json.dumps)

    data['event_params'] = data['event_params'].apply(_to_normal_json)
    data['user_properties'] = data['user_properties'].apply(_to_normal_json)

    data = data.replace({pd.NaT: None})
    data['event_previous_timestamp'] = data[
        'event_previous_timestamp'].fillna(0).astype('Int64')
    data['stream_id'] = data['stream_id'].fillna(0).astype('int64')
    data['event_bundle_sequence_id'] = data[
        'event_bundle_sequence_id'].fillna(0).astype('int')
    data['event_previous_timestamp'] = data['event_previous_timestamp'].astype('int')

    create_temp_file(
        data=data, key_xcoms=out_file_key, type_file='parquet', **context
    )


def load_in_clickhouse(
        report_date: str,
        account: str,
        ch_conn_id: str,
        in_file_key: str,
        **context
) -> None:
    """Загружает данные в базу firebase_events."""
    ch_hook = ClickHouseHook(ch_conn_id)
    data = get_df_from_file(in_file_key, type_file='parquet', **context)
    sql_delete = (
        f"ALTER TABLE {account} DELETE WHERE event_date = '{report_date}';"
    )
    sql_insert = (
        f"INSERT INTO {account} VALUES"
    )

    data = data.replace({pd.NaT: None})
    data['event_value_in_usd'] = data['event_value_in_usd'].replace({np.nan: None})

    ch_hook.run(sql_delete)
    sleep(30)
    ch_hook.get_conn().execute(sql_insert, [list(x) for x in data.to_numpy()])


default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
        default_args=default_args,
        dag_id='firebase_events_old_archive_v0.1',
        start_date=datetime(2022, 9, 2),
        schedule_interval='00 05 * * *',
        catchup=False,
        max_active_tasks=15,
        max_active_runs=1
) as dag:
    accounts = {
        'android1': {
            'table_name': 'analytics_198494171',
            'conn_id': 'bigquery_android_1',
            'location': 'us'
        },
        'android2': {
            'table_name': 'analytics_229971028',
            'conn_id': 'bigquery_android_2',
            'location': 'us'
        },
        'android3': {
            'table_name': 'analytics_248790095',
            'conn_id': 'bigquery_android_3',
            'location': 'us'
        },
        'android4': {
            'table_name': 'analytics_261156647',
            'conn_id': 'bigquery_android_4',
            'location': 'us'
        },
        'android5': {
            'table_name': 'analytics_277761306',
            'conn_id': 'bigquery_android_5',
            'location': 'us'
        },
        'android6': {
            'table_name': 'analytics_294103784',
            'conn_id': 'bigquery_android_6',
            'location': 'us'
        },
        'android7': {
            'table_name': 'analytics_324213159',
            'conn_id': 'bigquery_android_7',
            'location': 'us'
        },
        'ios1': {
            'table_name': 'analytics_200011464',
            'conn_id': 'bigquery_ios_1',
            'location': 'us'
        },
        'ios2': {
            'table_name': 'analytics_235010119',
            'conn_id': 'bigquery_ios_2',
            'location': 'us'
        },
        'ios3': {
            'table_name': 'analytics_259695304',
            'conn_id': 'bigquery_ios_3',
            'location': 'us'
        },
        'ios4': {
            'table_name': 'analytics_273815186',
            'conn_id': 'bigquery_ios_4',
            'location': 'us'
        },
        'ios5': {
            'table_name': 'analytics_294094496',
            'conn_id': 'bigquery_ios_5',
            'location': 'us'
        },
        'ios6': {
            'table_name': 'analytics_325570311',
            'conn_id': 'bigquery_ios_6',
            'location': 'us'
        },
        # 'ml': {
        #     'table_name': 'analytics_243579814',
        #     'conn_id': 'bigquery_ml',
        #     'location': 'us'
        # },
        'dominishell_android': {
            'table_name': 'analytics_301477343',
            'conn_id': 'bigquery_dominishell_android',
            'location': 'eu'
        },
        'dominishell_ios': {
            'table_name': 'analytics_302581404',
            'conn_id': 'bigquery_dominishell_ios',
            'location': 'us'
        },
    }
    report_date = '{{ (execution_date - macros.timedelta(days=5)).strftime("%Y-%m-%d") }}'

    for account, options in accounts.items():
        task_archive = 'extract_' + account + '_archive'

        extract_from_big_query_archive = PythonOperator(
            task_id=task_archive,
            python_callable=extract_data_from_big_query,
            op_kwargs={
                'report_date': report_date,
                'account': account,
                'table_name': options['table_name'] + '.events_',
                'bg_conn_id': options['conn_id'],
                'location': options['location'],
                'out_file_key': 'extract_data_from_big_query_' + account
            },
        )

        transform_unification = PythonOperator(
            task_id='transform_' + account,
            python_callable=transform_unification_data,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            op_kwargs={
                'in_file_key': 'extract_data_from_big_query_' + account,
                'out_file_key': 'transform_unification_data_' + account
            },
        )

        load_in_database = PythonOperator(
            task_id='load_' + account,
            python_callable=load_in_clickhouse,
            op_kwargs={
                'report_date': report_date,
                'account': account,
                'ch_conn_id': 'clickhouse_firebase_events',
                'in_file_key': 'transform_unification_data_' + account
            },
        )

        check_correct_load = ClickHouseSQLThresholdCheckOperator(
            task_id='check_correct_load_' + account,
            conn_id='clickhouse_firebase_events',
            database='firebase_events',
            sql=(
                 f"""
                 SELECT count(event_date)
                 FROM {account}
                 WHERE event_date = '{report_date}'
                 """
            ),
            min_threshold=1000,
            max_threshold=10000000
        )

        chain(
            extract_from_big_query_archive,
            transform_unification,
            load_in_database,
            check_correct_load
        )
