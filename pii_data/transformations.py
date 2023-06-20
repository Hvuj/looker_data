from imports import List, dd, sha256, validate_email, EmailNotValidError, phonenumbers, re, pycountry, GeonamesCache, \
    LRUCache


def apply_func_on_df(df, func_ton_run, **kwargs):
    """
    Apply a given function to each row of a DataFrame.

    Args:
        df (pandas.DataFrame or dask.DataFrame): The DataFrame to apply the function on.
        func_ton_run (callable): The function to apply on each row.
        **kwargs: Additional keyword arguments to pass to the function.

    Returns:
        pandas.Series or dask.Series: The result of applying the function to each row.

    """
    if kwargs:
        return df.apply(func_ton_run, axis=1, kwargs=kwargs)
    return df.apply(func_ton_run, axis=1)


def transform_column_names(report_data: dd.DataFrame) -> List[str]:
    """
    Transform column names of a DataFrame to a specific format.

    Args:
        report_data (dask.DataFrame): The DataFrame containing the report data.

    Returns:
        List[str]: A list of transformed column names.

    Raises:
        ValueError: If an error occurs during the transformation.

    """
    try:
        bq_schema: List[str] = [
            ''.join(
                f'_{char.lower()}' if char.isupper() else char for char in word
            )
            for word in report_data.columns.tolist()
        ]
        return bq_schema
    except ValueError as value_error:
        print(f"There is no:{value_error}")
        raise value_error


def hash_names(element, **kwargs):
    """
    Hashes an element using the SHA256 algorithm.

    Parameters:
        element (pd.DataFrame): The element containing the first_name and last_name attributes.

    Returns:
        str: The hexadecimal representation of the hashed element.

    Raises:
        AttributeError: If the first_name attribute is not present.
        Exception: If there is an error while fetching the first or last names.
    """
    try:
        kwargs = kwargs['kwargs']
        if kwargs['type'] == 'first_name':
            first_name = re.sub(r'[^a-z]', '', element.first_name.lower(), flags=re.IGNORECASE).replace(" ", "")
            sha256_hash = sha256()
            sha256_hash.update(first_name.strip().encode('utf-8'))
        else:
            last_name = re.sub(r'[^a-z]', '', element.last_name.lower(), flags=re.IGNORECASE).replace(" ", "")
            sha256_hash = sha256()
            sha256_hash.update(last_name.strip().encode('utf-8'))
        return sha256_hash.hexdigest().strip()
    except Exception as error:
        print(error)
        raise Exception('There was some error while fetching the first or last names') from error


def hash_email(element):
    """
    Hashes an email using the SHA256 algorithm.

    Parameters:
        element (pd.DataFrame): The element containing the email attribute.

    Returns:
        str: The hexadecimal representation of the hashed email.

    Raises:
        EmailNotValidError: If the email is not valid.
    """
    try:
        item = validate_email(element.email, allow_empty_local=True).email
        sha256_hash = sha256()
        sha256_hash.update(item.lower().strip().encode('utf-8'))
        return sha256_hash.hexdigest().strip()

    except EmailNotValidError as email_error:
        print(email_error)
        sha256_hash = sha256()
        sha256_hash.update(element.email.lower().strip().encode('utf-8'))
        return sha256_hash.hexdigest().strip()


def hash_phone(element, **kwargs):
    """
    Hashes a phone number using the SHA256 algorithm.

    Parameters:
        element (pd.DataFrame): The element containing the phone and country_code attributes.
        **kwargs: Additional keyword arguments. The 'source' keyword is expected to be present in kwargs.

    Returns:
        str: The hexadecimal representation of the hashed phone number.

    Raises:
        Exception: If there is an error while hashing the phone number.
    """
    try:
        sha256_hash = sha256()

        phone = element.phone.split('.')[0].replace(" ", "").replace("-", "")
        if phone.startswith("+"):
            phone = phone[1:]

        phone_number = phonenumbers.parse(phone, element.country_code)

        if phonenumbers.is_valid_number(phone_number):
            phone_number = phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.E164)

            kwargs = kwargs['kwargs']
            if kwargs['source'] == 'facebook':
                phone_number = re.sub(r"[^0-9]", "", phone_number)
            else:
                phone_number = "+" + re.sub(r"[^0-9]", "", phone_number)

            sha256_hash.update(phone_number.lower().strip().encode('utf-8'))
            return sha256_hash.hexdigest().strip()

    except Exception as phone_error:
        print(phone_error)
        sha256_hash = sha256()
        sha256_hash.update(element.phone.split('.')[0].lower().strip().encode('utf-8'))
        return sha256_hash.hexdigest().strip()


def parse_zipcode(element, **kwargs):
    """
    Parses a zipcode based on the source and country code.

    Parameters:
        element (pd.DataFrame): The element containing the zipcode and country_code attributes.
        **kwargs: Additional keyword arguments. The 'source' keyword is expected to be present in kwargs.

    Returns:
        str: The parsed zipcode.

    Raises:
        Exception: If there is an error while parsing the zipcode.
    """
    try:
        kwargs = kwargs['kwargs']
        if kwargs['source'] == 'facebook':
            return (
                re.sub(r"[^0-9]", "", element.zipcode[:5])
                if element.country_code.lower() == "us"
                else re.sub(r"[^0-9a-z]", "", element.zipcode, flags=re.IGNORECASE)
                .strip()
                .lower()
            )
        if element.country_code.lower() in ["us", "ar", "at", "be", "br", "hr", "cr", "do", "ec", "eg", "ee", "fi",
                                            "gt", "hu", "in", "id", "it", "jo", "kw", "lt", "lu", "my", "mt", "mx",
                                            "me", "no", "om", "pk", "py", "pl", "pt", "ro", "ru", "sa", "rs", "sg",
                                            "sk", "si", "th", "tn", "tr", "ua", "uy", "ve"]:
            return re.sub(r"[^0-9]", "", element.zipcode[:5])
        else:
            return re.sub(r"[^0-9a-z]", "", element.zipcode, flags=re.IGNORECASE).strip().lower()

    except Exception as zipcode_error:
        print(zipcode_error)
        return re.sub(r"[^0-9a-z]", "", element.zipcode, flags=re.IGNORECASE).strip().lower()


def parse_state(element, **kwargs):
    """
    Parses a state based on the source and country code.

    Parameters:
        element (pd.DataFrame): The element containing the state and country_code attributes.
        **kwargs: Additional keyword arguments. The 'source' keyword is expected to be present in kwargs.

    Returns:
        str: The parsed state code or hashed representation.

    Raises:
        Exception: If there is an error while parsing the state.
    """
    try:
        sha256_hash = sha256()
        state = element.state
        country_code = element.country_code.upper()
        kwargs = kwargs['kwargs']

        if len(state) != 2 or not state.isalpha():
            country = pycountry.countries.get(alpha_2=country_code)
            if country and hasattr(country, 'subdivisions'):
                subdivisions = country.subdivisions
                if state_subdivision := subdivisions.get(name=state.lower()):
                    if kwargs['source'] == 'facebook':
                        sha256_hash.update(state_subdivision.code.lower().strip().encode('utf-8'))
                        return sha256_hash.hexdigest().strip()
                    return state_subdivision.code

            if kwargs['source'] == 'facebook':
                sha256_hash.update(state[:2].lower().strip().encode('utf-8'))
                return sha256_hash.hexdigest().strip()
            return state[:2]

        if kwargs['source'] == 'facebook':
            sha256_hash.update(state.lower().strip().encode('utf-8'))
            return sha256_hash.hexdigest().strip()
        return state.upper().strip()  # Return the state code in uppercase

    except Exception as state_error:
        print(state_error)
        if kwargs['source'] == 'facebook':
            sha256_hash = sha256()
            sha256_hash.update(element.state.lower().strip().encode('utf-8'))
            return sha256_hash.hexdigest().strip()
        return element.state.upper().strip()


gc = GeonamesCache()
cities = gc.get_cities()
city_names = {city['name'].lower(): city for city in cities.values()}
cache = LRUCache(maxsize=10000)


def hash_city(element, **kwargs):
    """
    Hashes a city name based on the source and checks for caching.

    Parameters:
        element (pd.DataFrame): The element containing the city attribute.
        **kwargs: Additional keyword arguments. The 'source' keyword is expected to be present in kwargs.

    Returns:
        str: The hashed or transformed city name.

    Raises:
        None.
    """
    kwargs = kwargs['kwargs']
    source = kwargs['source']
    city = element.city.lower()
    sha256_hash = sha256()

    if city in cache:
        return cache[city]

    if city in city_names:
        if source == 'facebook':
            res = city_names[city]['name'].replace(" ", "")
        else:
            res = city_names[city]['name'].lower().strip()
        return return_hashed_city(sha256_hash, res, city, source)
    res = city.replace(" ", "") if source == 'facebook' else city.lower().strip()
    return return_hashed_city(sha256_hash, res, city, source)


def return_hashed_city(sha256_hash, value, city, source):
    """
    Returns the hashed value of a city name based on the source.

    Parameters:
        sha256_hash (hash object): The SHA256 hash object.
        value (str): The value to be hashed.
        city (str): The original city name.
        source (str): The source of the city name.

    Returns:
        str: The hashed value of the city name.

    Raises:
        None.
    """
    if source == 'facebook':
        sha256_hash.update(value.lower().strip().encode('utf-8'))
        hashed_value = sha256_hash.hexdigest().strip()
    else:
        hashed_value = value

    cache[city] = hashed_value
    return hashed_value


def parse_country_code(element, **kwargs):
    """
    Parses a zipcode based on the source and country code.

    Parameters:
        element (pd.DataFrame): The element containing the country_code attribute.
        **kwargs: Additional keyword arguments. The 'source' keyword is expected to be present in kwargs.

    Returns:
        str: The parsed zipcode.

    Raises:
        Exception: If there is an error while parsing the zipcode.
    """
    try:
        kwargs = kwargs['kwargs']
        if kwargs['source'] == 'facebook':
            sha256_hash = sha256()
            cc = re.sub(r"[^a-z]", "", element.country_code, flags=re.IGNORECASE).replace(" ", "")
            sha256_hash.update(cc.lower().strip().encode('utf-8'))
            return sha256_hash.hexdigest().strip()

        cc = re.sub(r"[^a-z]", "", element.country_code, flags=re.IGNORECASE).strip().replace(" ", "").upper()




    except Exception as country_code_error:
        print(country_code_error)
        return re.sub(r"[^0-9a-z]", "", element.zipcode, flags=re.IGNORECASE).strip().lower()
