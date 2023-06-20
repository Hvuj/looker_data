from imports import Client, bigquery, Final, dask, Optional, Any, time
from settings import CLIENT_ID, CLIENT_SECRET
from access_token_module import get_access_token
from data_utils import api_call, upload_data_to_bq
from transformations import hash_email, hash_phone, parse_zipcode, parse_state, hash_city, apply_func_on_df, hash_names
from data_utils import delete_temp_table


def main(request: Optional[Any]) -> str:
    project_id = "ex-nuts"
    dataset_name = 'staging'
    table_name = 'looker_pii_data'
    bq_client: Final[Client] = bigquery.Client(project_id)
    access_token = get_access_token(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    data = api_call(
        access_token=access_token, look_id=2556, limit=1000000, timeout=720
    )
    return process_data(data, project_id, dataset_name, table_name, bq_client)


def process_data(data, project_id, dataset_name, table_name, bq_client):
    delete_temp_table(client=bq_client,
                      project_id=project_id,
                      dataset_name=dataset_name,
                      table_name=table_name)
    data = data.astype(str)
    data = data.rename(columns={
        'Orders Order ID': "orders_order_id",
        'Orders Placed Date': "orders_place_date",
        'Contacts First Name': "first_name",
        'Contacts Contact Email': "email",
        'Orders Billed to Phone Number': "phone",
        'Orders Billed to Postal Code': "zipcode",
        'Orders Billed to City': "city",
        'Orders Billed to State Prov': "state",
        'Orders Billed to Country Code': "country_code"
    })

    start = time.time()
    print(data.columns)

    data['first_name'] = data[['first_name']].map_partitions(apply_func_on_df,
                                                             func_ton_run=hash_names,
                                                             type='first_name',
                                                             meta=('first_name', 'object'))
    data['last_name'] = data[['last_name']].map_partitions(apply_func_on_df,
                                                           func_ton_run=hash_names,
                                                           type='last_name',
                                                           meta=('last_name', 'object'))

    data['email'] = data[['email']].map_partitions(apply_func_on_df,
                                                   func_ton_run=hash_email,
                                                   meta=('email', 'object'))

    data['fb_phone'] = data[['phone', 'country_code']].map_partitions(apply_func_on_df,
                                                                      func_ton_run=hash_phone,
                                                                      source='facebook',
                                                                      meta=('fb_phone', 'object'))

    data['gg_phone'] = data[['phone', 'country_code']].map_partitions(apply_func_on_df,
                                                                      func_ton_run=hash_phone,
                                                                      source='google',
                                                                      meta=('gg_phone', 'object'))

    data['fb_zipcode'] = data[['zipcode', 'country_code']].map_partitions(apply_func_on_df,
                                                                          func_ton_run=parse_zipcode,
                                                                          source='facebook',
                                                                          meta=('fb_zipcode', 'object'))

    data['gg_zipcode'] = data[['zipcode', 'country_code']].map_partitions(apply_func_on_df,
                                                                          func_ton_run=parse_zipcode,
                                                                          source='google',
                                                                          meta=('gg_zipcode', 'object'))

    data['fb_state'] = data[['state', 'country_code']].map_partitions(apply_func_on_df,
                                                                      func_ton_run=parse_state,
                                                                      source='facebook',
                                                                      meta=('fb_state', 'object'))

    data['gg_state'] = data[['state', 'country_code']].map_partitions(apply_func_on_df,
                                                                      func_ton_run=parse_state,
                                                                      source='google',
                                                                      meta=('gg_state', 'object'))

    data['fb_city'] = data[['city']].map_partitions(apply_func_on_df,
                                                    func_ton_run=hash_city,
                                                    source='facebook',
                                                    meta=('fb_city', 'object'))

    data['gg_city'] = data[['city']].map_partitions(apply_func_on_df,
                                                    func_ton_run=hash_city,
                                                    source='google',
                                                    meta=('gg_city', 'object'))

    data['fb_country_code'] = data[['country_code']].map_partitions(apply_func_on_df,
                                                                    func_ton_run=hash_city,
                                                                    source='facebook',
                                                                    meta=('fb_country_code', 'object'))

    data['gg_country_code'] = data[['country_code']].map_partitions(apply_func_on_df,
                                                                    func_ton_run=hash_city,
                                                                    source='google',
                                                                    meta=('gg_country_code', 'object'))

    data = data.drop(columns=['phone'])
    if __name__ == '__main__':
        return run(data, project_id, dataset_name, table_name, bq_client, start)


def run(data, project_id, dataset_name, table_name, bq_client, start):
    if __name__ == '__main__':
        with dask.config.set(scheduler='processes'):
            ddf = data.compute()
            upload_res = upload_data_to_bq(client=bq_client,
                                           project_id=project_id,
                                           dataset_name=dataset_name,
                                           table_name=table_name,
                                           data_to_send=ddf)
            end = time.time()
            print(end - start)
            return upload_res


main('a')
