import os
import dask.dataframe as dd
import requests
from google.cloud import bigquery
import time
import json
from settings import CLIENT_ID, CLIENT_SECRET, DBT_ACCOUNT_ID, DBT_JOB_ID, DBT_TOKEN
from requests.exceptions import HTTPError
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.3,
                status_forcelist=[500, 501, 502, 503, 504, 505, 400, 401, 402, 403, 404, 405, 429],
                raise_on_status=False)

adapter = requests.adapters.HTTPAdapter(pool_connections=15,
                                        pool_maxsize=15,
                                        pool_block=False,
                                        max_retries=retries)

session.mount('https://', adapter)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = 'data_raw.csv'
# file_path = f'/tmp/{path}'
# path = os.path.join(BASE_DIR, file_path)
print(path)


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
            raise HTTPError(f'There was an error in getting Access token!\\n') from error


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
client = bigquery.Client('ex-nuts')


def delete_from_table_if_exists():
    # delete all rows from temp table
    query_delete_job = client.query(
        """
        if exists(

        select * from `staging.INFORMATION_SCHEMA.TABLES` where table_name='looker_daily_data'

        )
        then
        delete `ex-nuts.staging.looker_daily_data` where true;
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
    table_name = 'looker_daily_data'

    table_id = f"{project_id}.{dataset_name}.{table_name}"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'ga_attribution_campaign',
        'ga_attribution_channel',
        'ga_attribution_medium',
        'orders_order_id',
        'orders_user_id',
        'orders_placed_date',
        'order_products_product_id',
        'order_skus_id',
        'contacts_contact_id',
        'users_is_user_account_created',
        'orders_net_revenue',
        'contacts_email_domain',
        'order_products_product_name',
        'order_products_merchandising_category',
        'order_products_first_tier_department_name',
        'order_products_primary_department_name',
        'order_products_show_on_product_page_tag_names',
        'order_products_show_on_search_page_tag_names',
        'order_products_is_product_active',
        'order_skus_packaging_type',
        'ga_attribution_source',
        'orders_shipping_fee_amount',
        'orders_unique_merchandising_category_name',
        'orders_billed_to_city',
        'orders_billed_to_state_prov',
        'orders_billed_to_country_code',
        'order_skus_list_price',
        'order_skus_after_discount_price',
        'order_skus_before_discount_price',
        'contacts_first_subscribed_date',
        'contacts_first_subscribed_time',
        'orders_placed_time',
        'orders_is_placed_day_holiday',
        'order_skus_discount',
        'orders_canceled_date',
        'order_skus_weight',
        'kw_skus_data_net_weight',
        'order_skus_packaging_type_code',
        'gifting_is_gift',
        'orders_discount_fee_amount',
        'kw_skus_data_meltable',
        'orders_total_new_order_revenue',
        'order_skus_count_of_pieces',
        'order_skus_gross_profit',
        'accrued_gross_margin',
        'shipments_total_shipping_revenue',
        'order_skus_total_raw_material_cost',
        'order_skus_total_packaging_cost',
        'shipments_total_heat_resistant_packaging_cost',
        'order_skus_total_filling_labor_cost',
        'accrued_direct_labor_fulfillment_cost',
        'accrued_direct_labor_support_cost',
        'accrued_direct_labor_others_cost',
        'accrued_indirect_labor_cost',
        'accrued_freight_out_cost',
        'accrued_credit_card_processing_fee',
        'orders_total_shipping_carrier_fees',
        'accrued_others_cost',
        'accrued_product_cost'
    ]

    data_types = {
        'ga_attribution_campaign': 'string',
        'ga_attribution_channel': 'string',
        'ga_attribution_medium': 'string',
        'orders_order_id': 'string',
        'orders_user_id': 'string',
        'orders_placed_date': 'string',
        'order_products_product_id': 'string',
        'order_skus_id': 'string',
        'contacts_contact_id': 'string',
        'users_is_user_account_created': 'string',
        'orders_net_revenue': 'string',
        'contacts_email_domain': 'string',
        'order_products_product_name': 'string',
        'order_products_merchandising_category': 'string',
        'order_products_first_tier_department_name': 'string',
        'order_products_primary_department_name': 'string',
        'order_products_show_on_product_page_tag_names': 'string',
        'order_products_show_on_search_page_tag_names': 'string',
        'order_products_is_product_active': 'string',
        'order_skus_packaging_type': 'string',
        'ga_attribution_source': 'string',
        'orders_shipping_fee_amount': 'string',
        'orders_unique_merchandising_category_name': 'string',
        'orders_billed_to_city': 'string',
        'orders_billed_to_state_prov': 'string',
        'orders_billed_to_country_code': 'string',
        'order_skus_list_price': 'string',
        'order_skus_after_discount_price': 'string',
        'order_skus_before_discount_price': 'string',
        'contacts_first_subscribed_date': 'string',
        'contacts_first_subscribed_time': 'string',
        'orders_placed_time': 'string',
        'orders_is_placed_day_holiday': 'string',
        'order_skus_discount': 'string',
        'orders_canceled_date': 'string',
        'order_skus_weight': 'string',
        'kw_skus_data_net_weight': 'string',
        'order_skus_packaging_type_code': 'string',
        'gifting_is_gift': 'string',
        'orders_discount_fee_amount': 'string',
        'kw_skus_data_meltable': 'string',
        'orders_total_new_order_revenue': 'string',
        'order_skus_count_of_pieces': 'string',
        'order_skus_gross_profit': 'string',
        'accrued_gross_margin': 'string',
        'shipments_total_shipping_revenue': 'string',
        'order_skus_total_raw_material_cost': 'string',
        'order_skus_total_packaging_cost': 'string',
        'shipments_total_heat_resistant_packaging_cost': 'string',
        'order_skus_total_filling_labor_cost': 'string',
        'accrued_direct_labor_fulfillment_cost': 'string',
        'accrued_direct_labor_support_cost': 'string',
        'accrued_direct_labor_others_cost': 'string',
        'accrued_indirect_labor_cost': 'string',
        'accrued_freight_out_cost': 'string',
        'accrued_credit_card_processing_fee': 'string',
        'orders_total_shipping_carrier_fees': 'string',
        'accrued_others_cost': 'string',
        'accrued_product_cost': 'string'
    }

    rows = 50000000
    row_limit = 50000000

    delete_from_table_if_exists()

    dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET), look_id=2248,
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


run_task('a')
