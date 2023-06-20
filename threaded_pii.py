import csv
import os
import dask.dataframe as dd
import requests
import re
import phonenumbers
import time
from google.cloud import bigquery
from settings import CLIENT_ID, CLIENT_SECRET
from hashlib import sha256
from email_validator import validate_email, EmailNotValidError
import concurrent.futures

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = 'pii_data_temp.csv'
csv_path = 'pii_data_threaded.csv'

file_path_temp = f'/tmp/{path}'
path = os.path.join(BASE_DIR, file_path_temp)
file_path = f'/tmp/{csv_path}'
csv_path = os.path.join(BASE_DIR, file_path)


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
    print('Table was deleted. ex-nuts.xplenty.looker_pii_data_staging', results)
    return 'True'


def run_task(request):
    project_id = "ex-nuts"
    dataset_name = 'staging'
    table_name = 'looker_pii_data'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
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

    rows = 30000000001
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

    def get_next_line():
        with open(csv_path, 'w', encoding='utf-8') as file:
            writer = csv.writer(file)
            for i, item in enumerate(lines):
                if i == 0:
                    header = [item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8]]
                    writer.writerow(header)
                else:
                    yield item

    def process_line(item):
        with open(csv_path, 'a', encoding='utf-8') as file:
            writer = csv.writer(file)
            order_order_id = item[0]
            orders_placed_date = item[1]
            first_name = re.sub('[+() -+\']', '', str(sha256(re.sub(' ', '', str(item[2])).
                                                             strip().
                                                             lower().
                                                             encode('utf-8'))
                                                      .hexdigest())) if item[2] != '' else 'null'
            email = item[3]
            try:
                email = validate_email(item[3], allow_empty_local=True, ).email
                email = re.sub('[+() -+\']', '', str(sha256(re.sub(' ', '', email)
                                                            .lower()
                                                            .strip()
                                                            .encode("utf-8"))
                                                     .hexdigest()))
            except EmailNotValidError as email_error:
                print(email_error)
                email = re.sub('[+() -+\']', '', str(sha256(re.sub(' ', '', item[3])
                                                            .lower()
                                                            .strip()
                                                            .encode("utf-8"))
                                                     .hexdigest()))

            fb_phone = item[4]
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
                                                                       .encode())
                                                                .hexdigest()))

                except Exception as error:
                    print(error)

            else:
                fb_phone = re.sub('[+() -+\']', '', str(sha256(item[4]
                                                               .strip()
                                                               .lower()
                                                               .encode())
                                                        .hexdigest()))

            gg_phone = item[4]
            if item[4] != '':
                try:
                    gg_phone = phonenumbers.parse(item[4], item[8])

                    if phonenumbers.is_valid_number(gg_phone):
                        gg_phone = phonenumbers.format_number(gg_phone, phonenumbers.PhoneNumberFormat.E164)
                        gg_phone = re.sub('[+() -+\']', '', str(sha256(gg_phone
                                                                       .strip()
                                                                       .lower()
                                                                       .encode())
                                                                .hexdigest()))

                except Exception as error:
                    print(error)

            else:
                gg_phone = re.sub('[+() -+\']', '', str(sha256(item[4]
                                                               .strip()
                                                               .lower()
                                                               .encode())
                                                        .hexdigest()))

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
                                                           .encode())
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

    f = get_next_line()
    jobs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
        for i, line in enumerate(lines):
            if i > 0:
                results = executor.submit(process_line, line)
                # this will clean memory
                results.result()
                jobs.append(results)
                if len(jobs) > 20:
                    jobs.clear()

    print('ready')
    rows_to_insert = dd.read_csv(csv_path, names=column_names, header=0, dtype=data_types, sep=',',
                                 blocksize=None).head(n=rows)

    print('started to push data into bq')

    job = client.load_table_from_dataframe(
        rows_to_insert, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    return 'True'


if __name__ == '__main__':
    start = time.perf_counter()

    run_task('yes')
    end = time.perf_counter()
    print(f'Done tasks and it took in total {round(end - start, 2)} second(s) to finish')
