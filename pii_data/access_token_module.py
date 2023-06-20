from imports import Optional, requests, HTTPAdapter, Retry, Response, Final, Dict, ttl_cache


def access_token_api_call(api_call_url: str,
                          api_call_payload: str,
                          api_call_headers: dict,
                          api_call_seconds: int,
                          client_id: str,
                          client_secret: str) -> Optional[str]:
    """
    Fetches Google Ads api Access Token

    Parameters
    ----------
    api_call_url : str
        The url for the current API call.
    api_call_payload : str
        The payload for the current API call.
    api_call_headers : str
        The headers for the current API call.
    api_call_seconds : int
        The seconds for the timeout window for the current API call.
    client_id : str
        The client id for the current API call.
    client_secret : str
        The client secret for the current API call.
    Returns
    -------
    access_token: Union[str,bool]
        On success - Access token is granted.
        On failure - None
    """
    try:
        with requests.Session() as session:
            retries = Retry(total=5,
                            backoff_factor=0.2,
                            status_forcelist=list(range(400, 600)),
                            allowed_methods=frozenset(['POST']),
                            raise_on_status=False)

            adapter = HTTPAdapter(pool_connections=15,
                                  pool_maxsize=15,
                                  pool_block=False,
                                  max_retries=retries)

            session.mount('http://', adapter)
            session.mount('https://', adapter)
            response: Response = session.post(url=api_call_url,
                                              data=api_call_payload,
                                              headers=api_call_headers,
                                              verify=True,
                                              timeout=api_call_seconds,
                                              auth=(client_id, client_secret))
            if 200 <= response.status_code < 300:
                print("Success! Access token has been retrieved")
                return response.json()['access_token']
    except Exception as e:
        print(e)
        raise e


@ttl_cache(maxsize=1)
def get_access_token(client_id: str, client_secret: str) -> Optional[str]:
    """
    Fetches Looker api Access Token

    Parameters
    ----------
    client_id : str
        The client id of the account.
    client_secret : str
        The client secret of the account.

    Returns
    -------
    access_token: Union[str,bool]
        On success - Access token is granted.
        On failure - None

    Raises
    ------
    HTTPError
        If url is not correct parameter.
    ConnectTimeout
        If connection is timeout.
    ConnectionError
        If there is a built-in connection error.
    """

    url: Final[str] = "https://nuts.looker.com/api/3.1/login"
    payload: Final[str] = f'client_id={client_id}&client_secret={client_secret}'
    headers: Final[dict[str]] = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    access_token = access_token_api_call(api_call_url=url,
                                         api_call_payload=payload,
                                         api_call_headers=headers,
                                         api_call_seconds=60,
                                         client_id=client_id,
                                         client_secret=client_secret)
    if access_token is None:
        try:
            print("Retrying request with double timeout.")
            access_token = access_token_api_call(api_call_url=url,
                                                 api_call_payload=payload,
                                                 api_call_headers=headers,
                                                 api_call_seconds=120,
                                                 client_id=client_id,
                                                 client_secret=client_secret)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as error:
            print(f"Retried request with double timeout and an error occurred: {error}")
    return access_token
