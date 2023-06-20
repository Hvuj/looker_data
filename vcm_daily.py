import io
import os
import requests
from google.cloud import bigquery
import dask.dataframe as dd
from settings import CLIENT_ID, CLIENT_SECRET
from requests.exceptions import HTTPError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = 'vcm_daily.csv'
# file_path = f'/tmp/{path}'
# path = os.path.join(BASE_DIR, file_path)


def get_looker_access_token(client_id: str, client_secret: str) -> str:
    uri = "https://nuts.looker.com/api/3.1/login"
    payload = f'client_id={client_id}&client_secret={client_secret}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        response = requests.post(url=uri,
                                 headers=headers,
                                 data=payload,
                                 verify=True,
                                 timeout=600 * 1800,
                                 stream=True).json()['access_token']

        print('Access token granted!')
        return response

    except Exception as error:
        print(f'There was an error in getting Access token!: {str(error)}')
        if error:
            raise HTTPError(f'There was an error in getting Access token!\\n') from error


def get_looker_data(access_token: str, look_id: int, limit: int = 500):
    uri = f'https://nuts.looker.com/api/3.1/looks/{look_id}/run/csv?limit={limit}'

    payload = {}
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    try:

        response = requests.get(url=uri,
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
client = bigquery.Client('ex-nuts')


def delete_from_table_if_exists():
    # delete all rows from temp table
    query_delete_job = client.query(
        """
        if exists(

        select * from `xplenty.INFORMATION_SCHEMA.TABLES` where table_name='looker_daily_data_vcm_staging'

        )
        then
        delete `ex-nuts.xplenty.looker_daily_data_vcm_staging` where true;
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

    project_id = "ex-nuts"
    dataset_name = 'xplenty'
    table_name = 'looker_daily_data_vcm'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'orders_placed_date',
        'accrued_credit_card_processing_fee',
        'accrued_direct_labor_fulfillment_cost',
        'accrued_direct_labor_others_cost',
        'accrued_direct_labor_support_cost',
        'accrued_freight_out_cost',
        'accrued_indirect_labor_cost',
        'accrued_others_cost',
        'accrued_product_cost',
        'discounts_total_discount_fee',
        'order_skus_discount',
        'order_skus_total_filling_labor_cost',
        'order_skus_total_packaging_cost',
        'order_skus_total_product_after_discount_revenue',
        'order_skus_total_product_before_discount_revenue',
        'order_total_order_net_revenue',
        'shipments_total_shipping_revenue',
        'shipments_total_heat_resistant_packaging_cost',
        'gm',
        'gross_margin'
    ]

    data_types = {
        'orders_placed_date': 'string',
        'accrued_credit_card_processing_fee': 'string',
        'accrued_direct_labor_fulfillment_cost': 'string',
        'accrued_direct_labor_others_cost': 'string',
        'accrued_direct_labor_support_cost': 'string',
        'accrued_freight_out_cost': 'string',
        'accrued_indirect_labor_cost': 'string',
        'accrued_others_cost': 'string',
        'accrued_product_cost': 'string',
        'discounts_total_discount_fee': 'string',
        'order_skus_discount': 'string',
        'order_skus_total_filling_labor_cost': 'string',
        'order_skus_total_packaging_cost': 'string',
        'order_skus_total_product_after_discount_revenue': 'string',
        'order_skus_total_product_before_discount_revenue': 'string',
        'order_total_order_net_revenue': 'string',
        'shipments_total_shipping_revenue': 'string',
        'shipments_total_heat_resistant_packaging_cost': 'string',
        'gm': 'string',
        'gross_margin': 'string'
    }

    rows = 30000001
    row_limit = 100000

    delete_from_table_if_exists()

    dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET), look_id=2382,
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
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    return 'True'

run_task('yes')