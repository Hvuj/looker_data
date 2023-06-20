import csv
import os
import dask.dataframe as dd
import requests
import re
import phonenumbers
from google.cloud import bigquery
from settings import CLIENT_ID, CLIENT_SECRET
from hashlib import sha256
from email_validator import validate_email, EmailNotValidError
from requests.exceptions import HTTPError
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
path = 'pii_data_temp.csv'
csv_path = 'pii_data.csv'

# file_path_temp = f'/tmp/{path}'
# path = os.path.join(BASE_DIR, file_path_temp)
# file_path = f'/tmp/{csv_path}'
# csv_path = os.path.join(BASE_DIR, file_path)


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
        raise error


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
                                timeout=600 * 60)
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

        select * from `staging.INFORMATION_SCHEMA.TABLES` where table_name='looker_pii_data_staging'

        )
        then
        delete `ex-nuts.staging.looker_pii_data_staging` where true;
        else
        select 'there is not table';
        end if;

        """
    )

    # get the results
    results = query_delete_job.result()  # Waits for job to complete.
    print('Table was deleted. ', results)
    return 'True'


def run(request):
    project_id = "ex-nuts"
    dataset_name = 'staging'
    table_name = 'looker_pii_data'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=0,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'order_order_id',
        'orders_placed_date',
        'first_name',
        'email',
        'fb_phone',
        'gg_phone',
        'fb_zipcode',
        'gg_zipcode',
        'fb_city',
        'gg_city',
        'fb_state',
        'gg_state',
        'fb_country_code',
        'gg_country_code'
    ]

    data_types = {
        'order_order_id': 'string',
        'orders_placed_date': 'string',
        'first_name': 'string',
        'email': 'string',
        'fb_phone': 'string',
        'gg_phone': 'string',
        'fb_zipcode': 'string',
        'gg_zipcode': 'string',
        'fb_city': 'string',
        'gg_city': 'string',
        'fb_state': 'string',
        'gg_state': 'string',
        'fb_country_code': 'string',
        'gg_country_code': 'string'
    }

    rows = 30000001
    rows_to_skip = 0
    row_limit = 50000000

    delete_from_table_if_exists()

    dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET), look_id=2556,
                                limit=row_limit)
    with open(path, 'w', encoding='utf-8') as file:
        file.write(dask_data)

        file.close()

    r = csv.reader(open(path, encoding="UTF-8"))
    lines = list(r)

    with open(csv_path, 'w', encoding='utf-8') as file:
        writer = csv.writer(file)
        for i, item in enumerate(lines):
            if i == 0:
                header = [item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8]]
                writer.writerow(header)
            else:
                order_order_id = item[0]
                orders_placed_date = item[1]
                first_name = re.sub('[+() -+\']', '', str(sha256(re.sub(' ', '', str(item[2])).
                                                                 strip().
                                                                 lower().
                                                                 encode('utf-8'))
                                                          .hexdigest())) if item[2] != '' else 'null'

                try:
                    email = validate_email(item[3], allow_empty_local=True, ).email
                except EmailNotValidError as email_error:
                    print(email_error)

                email = re.sub('[+() -+\']', '', str(sha256(re.sub(' ', '', email)
                                                            .lower()
                                                            .strip()
                                                            .encode('utf-8'))
                                                     .hexdigest()))

                if item[4] != '':
                    try:
                        fb_phone = phonenumbers.parse(item[4], item[8])

                        if phonenumbers.is_valid_number(fb_phone):
                            fb_phone = phonenumbers.format_number(fb_phone,
                                                                  phonenumbers.PhoneNumberFormat.E164)
                            fb_phone = ''.join(e for e in fb_phone if e.isnumeric())
                            fb_phone = re.sub('[+() -+\']', '', str(sha256(fb_phone
                                                                           .strip()
                                                                           .lower()
                                                                           .encode('utf-8'))
                                                                    .hexdigest()))

                    except Exception as error:
                        print(error)

                else:
                    fb_phone = 'null'

                if item[4] != '':
                    try:
                        gg_phone = phonenumbers.parse(item[4], item[8])

                        if phonenumbers.is_valid_number(gg_phone):
                            gg_phone = phonenumbers.format_number(gg_phone, phonenumbers.PhoneNumberFormat.E164)
                            gg_phone = re.sub('[+() -+\']', '', str(sha256(gg_phone
                                                                           .strip()
                                                                           .lower()
                                                                           .encode('utf-8'))
                                                                    .hexdigest()))

                    except Exception as error:
                        print(error)

                else:
                    gg_phone = 'null'

                if item[8] == 'US':
                    fb_zipcode = str(item[5])[:5]
                    fb_zipcode = re.sub('[+() -+\']', '', str(re.sub('[- ]', '', fb_zipcode)
                                                              .lower()
                                                              .strip())) if fb_zipcode != '' else 'null'
                else:
                    fb_zipcode = re.sub('[+() -+\']', '', str(re.sub('[- ]', '', str(item[5]))
                                                              .lower()
                                                              .strip())) if item[
                                                                                5] != '' else 'null'

                gg_zipcode = item[5]

                fb_city = ''.join(e for e in str(item[6]) if e.isalnum())
                fb_city = re.sub('[+() -+\']', '', sha256(fb_city
                                                          .lower()
                                                          .strip()
                                                          .encode('utf-8'))
                                 .hexdigest() if fb_city != '' else 'null')

                gg_city = item[6]

                fb_state = ''.join(e for e in str(item[7]) if e.isalpha())

                fb_state = re.sub('[+() -+\']', '', str(sha256(fb_state
                                                               .lower()
                                                               .strip()
                                                               .encode('utf-8'))
                                                        .hexdigest())) if item[7] != '' else 'null'
                gg_state = item[7]
                fb_country_code = re.sub('[+() -+\']', '', str(sha256(str(item[8])
                                                                      .strip()
                                                                      .lower()
                                                                      .encode('utf-8'))
                                                               .hexdigest())) if item[8] != '' else 'null'
                gg_country_code = item[8]

                data_to_write_as_csv = [
                    order_order_id,
                    orders_placed_date,
                    first_name,
                    email,
                    fb_phone,
                    gg_phone,
                    fb_zipcode,
                    gg_zipcode,
                    fb_city,
                    gg_city,
                    fb_state,
                    gg_state,
                    fb_country_code,
                    gg_country_code
                ]
                writer.writerow(data_to_write_as_csv)

    rows_to_insert = dd.read_csv(csv_path, names=column_names, header=0, dtype=data_types, sep=',',
                                 blocksize="20MB").head(
        n=rows)

    print('started to push data into bq')

    job = client.load_table_from_dataframe(
        rows_to_insert, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    return 'True'


run('a')
