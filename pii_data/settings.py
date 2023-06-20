from __future__ import annotations
from typing import Final
from dotenv import load_dotenv
from os.path import join, dirname
from google.cloud import secretmanager
import os
import google_crc32c
import json

try:
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    PROJECT_ID: Final[str] = os.environ.get("PROJECT_ID")
    SECRET_ID: Final[str] = os.environ.get("SECRET_ID")
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"

    response = client.access_secret_version(request={"name": name})
    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        print(response)
    payload = json.loads(str(response.payload.data.decode('utf-8')).replace('\'', '\"'))

    CLIENT_ID = payload['CLIENT_ID']
    CLIENT_SECRET = payload['CLIENT_SECRET']
except (KeyError, ValueError, TypeError) as base_errors:
    raise print(f'There is an error: {base_errors}') from base_errors
