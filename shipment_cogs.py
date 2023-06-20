import io
import os
import json
import pandas as pd
import requests
from google.cloud import bigquery
import dask.dataframe as dd
import time
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()

retries = Retry(total=15,
                connect=10,
                read=10,
                status=10,
                backoff_factor=0.3,
                status_forcelist=[500, 501, 502, 503, 504, 505, 400, 401, 402, 403, 404, 405, 429],
                raise_on_status=True)

max_retries = requests.adapters.DEFAULT_RETRIES = retries
time_out_factor = requests.adapters.DEFAULT_POOL_TIMEOUT = None
adapter = requests.adapters.HTTPAdapter(pool_connections=15,
                                        pool_maxsize=15,
                                        pool_block=False,
                                        max_retries=max_retries)

session.mount('https://', adapter)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = 'shipment_cogs.csv'
# file_path = f'/tmp/{path}'
# path = os.path.join(BASE_DIR, file_path)

from settings import CLIENT_ID, CLIENT_SECRET


def get_looker_access_token(client_id: str, client_secret: str) -> str:
    uri = "https://nuts.looker.com/api/4.0/login"
    payload = f'client_id={client_id}&client_secret={client_secret}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        response = session.post(url=uri,
                                headers=headers,
                                data=payload,
                                verify=True,
                                timeout=600,
                                stream=True).json()['access_token']

        print('Access token granted!')
        return response

    except Exception as error:
        print(f'There was an error in getting Access token!: {str(error)}')
        if error:
            raise Exception('There was an error in getting Access token!\\n') from error


def get_looker_data(access_token: str, look_id: int, limit: int = 500):
    uri = f'https://nuts.looker.com/api/4.0/looks/{look_id}/run/csv?limit={limit}'
    payload = {}
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    try:
        response = session.get(url=uri,
                               headers=headers,
                               data=payload,
                               verify=True,
                               timeout=600 * 60,
                               stream=True)
        print('Success! Data was received.')

        return response.text

    except Exception as error:
        print(f'There was an error in getting Data! {str(error)}')


"""
 Get bq data in order to get the latest date
"""
client = bigquery.Client("ex-nuts")


def delete_from_table_if_exists():
    # delete all rows from temp table
    query_delete_job = client.query(
        """
        if exists(

        select * from `staging.INFORMATION_SCHEMA.TABLES` where table_name='looker_daily_shipment_data_staging'

        )
        then
        delete `ex-nuts.staging.looker_daily_shipment_data_staging` where true;
        else
        select 'there is not table';
        end if;

        """
    )

    # get the results
    results = query_delete_job.result()  # Waits for job to complete.
    print('Table was deleted. ', results)
    return 'True'


def run_task(request):
    if request:
        print(request)
    else:
        print('there is no request')

    project_id = "ex-nuts"
    dataset_name = 'staging'
    table_name = 'looker_daily_shipment_data'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=0,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'order_id',
        'shipment_id',
        'shipped_time',
        'shipped_date',
        'orders_discount_fee_amount',
        'orders_is_reshipment',
        'shipments_is_shipment_canceled',
        'shipments_canceled_date',
        'orders_placed_date',
        'credit_card_processing_fee',
        'direct_labor_fulfillment_cost',
        'direct_labor_support_cost',
        'direct_labor_others_cost',
        'indirect_labor_cost',
        'others_cost',
        'total_heat_res_packaging_cost',
        'total_shipment_revenue',
        'total_shipment_actual_carrier_cost'

    ]

    data_types = {
        'order_id': 'string',
        'shipment_id': 'string',
        'shipped_time': 'string',
        'shipped_date': 'string',
        'orders_discount_fee_amount': 'float',
        'orders_is_reshipment': 'string',
        'shipments_is_shipment_canceled': 'string',
        'shipments_canceled_date': 'string',
        'orders_placed_date': 'string',
        'credit_card_processing_fee': 'float',
        'direct_labor_fulfillment_cost': 'float',
        'direct_labor_support_cost': 'float',
        'direct_labor_others_cost': 'float',
        'indirect_labor_cost': 'float',
        'others_cost': 'float',
        'total_heat_res_packaging_cost': 'float',
        'total_shipment_revenue': 'float',
        'total_shipment_actual_carrier_cost': 'float'

    }

    rows = 50000000
    rows_to_skip = 0
    row_limit = 50000000

    delete_from_table_if_exists()

    dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET),
                                look_id=2332,
                                limit=row_limit)

    with open(path, 'w', encoding='utf-8') as file:
        file.write(dask_data)

        file.close()

    rows_to_insert = dd.read_csv(path, names=column_names, header=0, dtype=data_types, sep=',', blocksize=None).head(
        n=rows)

    print('started to push data into bq')

    job = client.load_table_from_dataframe(
        rows_to_insert, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id} and done!")
    return 'True'


run_task('a')
