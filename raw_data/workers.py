from imports import List, Final, Any, Retry, Tuple, requests, Dict, HTTPAdapter, Optional


def create_session(access_token: str, retries: Retry) -> Tuple[requests.Session, Dict[str, str]]:
    """Create a session with necessary configuration"""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=15,
        pool_maxsize=15,
        pool_block=False,
        max_retries=retries
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    headers: Final[Dict[str, str]] = {'Authorization': f'Bearer {access_token}'}
    return session, headers


def make_request(session: requests.Session,
                 url: str,
                 params: Optional[Dict[str, Any]] = None,
                 headers: Dict[str, str] = None,
                 timeout: int = 60) -> requests.Response:
    """Make a GET request to the given URL with the given parameters and headers."""
    return session.get(url=url, params=params, headers=headers, verify=True, timeout=timeout)
