import requests
from google.cloud import bigquery
import dask.dataframe as dd
from settings import CLIENT_ID, CLIENT_SECRET
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = 'answer_choices.csv'
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
        response = requests.post(url=uri,
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
client = bigquery.Client("ex-nuts")


def delete_from_table_if_exists():
    # delete all rows from temp table
    query_delete_job = client.query(
        """
        if exists(

        select * from `xplenty.INFORMATION_SCHEMA.TABLES` where table_name='looker_daily_answer_choices_staging'

        )
        then
        delete `ex-nuts.xplenty.looker_daily_answer_choices_staging` where true;
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
    dataset_name = 'xplenty'
    table_name = 'looker_daily_answer_choices'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=0,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'contact_id',
        'survey_answer_choices',
        'orders_placed_time',
        'orders_order_id',
        'zipcode',
        'is_b2b'

    ]

    data_types = {
        'contact_id': 'string',
        'survey_answer_choices': 'string',
        'orders_placed_time': 'string',
        'orders_order_id': 'string',
        'zipcode': 'string',
        'is_b2b': 'string'

    }

    rows = 30000000001
    rows_to_skip = 0
    row_limit = 50000000000

    delete_from_table_if_exists()

    # dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
    #                                                                  client_secret=CLIENT_SECRET), look_id=2432,
    #                             limit=row_limit)
    #
    # with open(path, 'w', encoding='utf-8') as file:
    #     file.write(dask_data)
    #
    #     file.close()

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
