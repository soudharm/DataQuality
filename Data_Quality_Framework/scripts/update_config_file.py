import pandas as pd
import snowflake.connector
import pyodbc
import pathlib
import boto3
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from django.conf import settings


def updateconfig():
    connection_string = "DefaultEndpointsProtocol=https;AccountName=diadifstorage001;AccountKey=kmNicxR5p69wOAHgIj8zqgA5Ky3Ej0xDU9haRELi0zG9aZv45VlHUoqPU9Wi7V6IOu8zDT1UgZ97+AStwFUxWw==;EndpointSuffix=core.windows.net"
    file_path = "fca/config/Config_Rule.xlsx"
    try:
        rule_config_file = pd.read_excel(
            f"abfs://{file_path}",
            storage_options={
                "connection_string": connection_string
            }, na_values = "Missing",sheet_name="DQ_RULE_CONFIG")
        now = datetime.now()
        date_today = now.strftime("%d%m%Y")
        #count=rule_config_file.index
        for i in range(len(rule_config_file)):
            start = rule_config_file['source_file'][i].rindex('/')
            end = rule_config_file['source_file'][i].rindex('.')
            rule_config_file.source_file[i]=rule_config_file['source_file'][i].replace(rule_config_file['source_file'][i][start + 1:end], date_today)
            print(rule_config_file.source_file[i])

    except Exception as e:
        print(e)