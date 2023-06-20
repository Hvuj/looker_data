from imports import logging, bigquery, BigQueryError, Final, LoadJobConfig, LoadJob, Dict, dd, Retry, dask, io, pd, \
    Response, requests, dask_bigquery
from workers import create_session


def make_request(session: requests.Session,
                 url: str,
                 headers: Dict[str, str] = None,
                 timeout: int = 0) -> dd.DataFrame:
    try:
        payload: Final[Dict] = {}
        print('Fetching data...')
        response: Response = session.get(url=url,
                                         headers=headers,
                                         data=payload,
                                         verify=True,
                                         timeout=timeout)
        if not 200 <= response.status_code < 300:
            raise f"Failed to fetch data: {response.text}"
        print('Success! Data was received.')
        chunk_size = 100000
        chunks = list(
            pd.read_csv(
                io.BytesIO(response.content),
                sep=',',
                header=0,
                chunksize=chunk_size,
                iterator=True,
            )
        )
        return dd.from_pandas(pd.concat(chunks), npartitions=25)
    except Exception as e:
        print(f'There has been an error while fetching the data: {e}')
        raise


def delete_temp_table(client: bigquery.Client,
                      project_id: str,
                      dataset_name: str,
                      table_name: str) -> None:
    table_id: str = f'{project_id}.{dataset_name}.{table_name}_staging'

    try:
        query_to_run: Final[str] = f"""drop table if exists `{table_id}`;"""
        query: bigquery.QueryJob = client.query(query_to_run)
        print(f'Table deleted successfully: {table_id}')

    except Exception as error:
        logging.warning(f'There was an error: {error}')
        raise BigQueryError(f'An error occurred while delete data from BigQuery from table {table_id}') from error


def upload_data_to_bq(ddf, **kwargs) -> str:
    """
    Uploads data from a Pandas DataFrame to Google BigQuery.

    Args:
        kwargs[project_id]: A string representing the project ID where the target dataset resides.
        kwargs[dataset_name]: A string representing the name of the target dataset.
        kwargs[table_name]: A string representing the name of the target table.
        ddf: A Pandas DataFrame containing the data to upload.

    Returns:
        A string indicating whether the job completed successfully or not.
    """
    try:
        kwargs = kwargs['kwargs']
        project_id = kwargs['project_id']
        dataset_name = kwargs['dataset_name']
        table_name = kwargs['table_name']
        table_id: Final[str] = f"{table_name}_staging"
        # ddf = dask.datasets.timeseries(freq="1min")

        res = dask_bigquery.to_gbq(
            ddf,
            project_id=project_id,
            dataset_id=dataset_name,
            table_id=table_id,
        )
        logging.info('Pushing data from BigQuery successfully')
        return res
    except Exception as e:
        # Log the error message and raise the exception
        logging.error(f'An error occurred while uploading data to BigQuery: {e}')
        raise e


def run_job(client: bigquery.Client, table_id: str, data_to_send: dd.DataFrame) -> str:
    """
    Uploads a given pandas DataFrame to a specified BigQuery table using the provided BigQuery client object.

    Args:
        client (bigquery.Client): A client object used to interact with BigQuery.
        table_id (str): A string representing the ID of the BigQuery table to upload the data to.
        data_to_send (pd.DataFrame): A pandas DataFrame containing the data to upload to BigQuery.

    Returns:
        str: A string 'True' if the upload was successful.

    Raises:
        This function does not raise any exceptions.

    """
    logging.info(f'Pushing data to table: {table_id}')
    job_config: Final[LoadJobConfig] = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=0,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=False,
    )
    job: Final[LoadJob] = client.load_table_from_dataframe(
        data_to_send, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    logging.info(f'Upload to BigQuery completed, job status: {job.state}')

    return 'True'


def api_call(access_token: str, look_id: int = None, limit: int = 500, timeout: int = 60):
    try:
        retries = Retry(
            total=5,
            backoff_factor=0.2,
            status_forcelist=list(range(400, 600)),
            allowed_methods=frozenset(['POST', 'GET']),
            raise_on_status=False
        )
        session, headers = create_session(access_token, retries)

        url: Final[str] = f'https://nuts.looker.com/api/4.0/looks/{look_id}/run/csv?limit={limit}&cache=true'
        return make_request(session=session, url=url, headers=headers, timeout=timeout)

    except Exception as e:
        print(e)
        raise e
