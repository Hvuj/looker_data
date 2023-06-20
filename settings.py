import os
from os.path import join, dirname

from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
CLIENT_ID = os.environ.get("CLIENT_ID")
DBT_ACCOUNT_ID = os.environ.get("DBT_ACCOUNT_ID")
DBT_TOKEN = os.environ.get("DBT_TOKEN")
DBT_JOB_ID = os.environ.get("DBT_JOB_ID")
