import requests
from google.cloud import bigquery
import dask.dataframe as dd
from settings import CLIENT_ID, CLIENT_SECRET
import os
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
path = 'looker_orders_level_data.csv'
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

        select * from `staging.INFORMATION_SCHEMA.TABLES` where table_name='looker_orders_level_data_staging'

        )
        then
        delete `ex-nuts.staging.looker_orders_level_data_staging` where true;
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
        print('There is not request')

    project_id = "ex-nuts"
    dataset_name = 'staging'
    table_name = 'looker_orders_level_data'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=0,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=False,
        schema=[
            bigquery.SchemaField("orders_order_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("discount_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("orders_placed_time", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("orders_postal_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("orders_is_auto_delivery", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("holdout", bigquery.enums.SqlTypeNames.STRING)
        ]
    )

    column_names = [
        'orders_order_id',
        'discount_code',
        'orders_placed_time',
        'orders_postal_code',
        'orders_is_auto_delivery',
        'holdout'

    ]

    data_types = {
        'orders_order_id': 'string',
        'discount_code': 'string',
        'orders_placed_time': 'string',
        'orders_postal_code': 'string',
        'orders_is_auto_delivery': 'string',
        'holdout': 'string'

    }

    rows = 30000001
    row_limit = 50000000

    # delete_from_table_if_exists()

    dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET), look_id=2483,
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
