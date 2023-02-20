import os

import pandas as pd
from datetime import date
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import argparse
import configparser as cp
import csv
from openpyxl import load_workbook
# import pyarrow.orc as orc
import pathlib

#from DQF.main import read_args


#from DQF.main import folder_paths

def upload_config():
    project_path = folder_paths()
    conf_obj = cp.ConfigParser()
    full_path = os.path.join(project_path, "config") + r"\framework_config.ini"
    conf_obj.read(full_path)

    connection_string = conf_obj["Config_Upload"]["connection_string"]
    my_container = conf_obj["Config_Upload"]["my_container"]
    cofig_file_path=conf_obj["Config_Upload"]["cofig_file_path"]
    my_blob = "Config_Rule.xlsx"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(my_container)
    blob_client = container_client.get_blob_client(my_blob)

    # full_path=os.path.join("C:/Users/soupatil/Downloads/Data_Quality_Framework_Tool (1)/Data_Quality_Framework_Tool/DQF/Projects/Salesforce/DQ_output/", "Projects")

    with open(cofig_file_path, "rb") as data:
        blob_client.upload_blob(data)



def azureupload(output_file_path,project_path):
    #conf_obj, output_folder_path, output_file_path, project_path, rule_config_file, admin_file = folder_paths()
    # args = read_args()
    # project = args.project
    # main_project_path = os.getcwd()
    # project_path = os.path.join(main_project_path, "Projects", project)
    conf_obj = cp.ConfigParser()
    full_path = os.path.join(project_path, "config") + r"\framework_config.ini"
    conf_obj.read(full_path)

    now = datetime.now()
    date_today = now.strftime("%Y-%m-%d")
    #output = conf_obj["File_Configuration"]["Upload_Azure"]
    connection_string = conf_obj["Upload_Azure"]["connection_string"]
    my_container = conf_obj["Upload_Azure"]["my_container"]+"/"+ date_today


    timeString = str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    my_blob = r"\ValidationResult_{}.xlsx".format(timeString)
    #blob = BlobClient.from_connection_string(conn_str=connection_string,
                                             #container_name=my_container,
                                             #blob_name=my_blob)
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(my_container)
    blob_client = container_client.get_blob_client(my_blob)

    #full_path=os.path.join("C:/Users/soupatil/Downloads/Data_Quality_Framework_Tool (1)/Data_Quality_Framework_Tool/DQF/Projects/Salesforce/DQ_output/", "Projects")

    with open(output_file_path,"rb") as data:
        blob_client.upload_blob(data)
# import os, uuid, sys
# from azure.storage.filedatalake import DataLakeServiceClient
# from azure.core._match_conditions import MatchConditions
# from azure.storage.filedatalake._models import ContentSettings
#
#
# service_client = ""
# storage_account_name = "deloittedqflake"
# storage_account_key = "3Jr83MBU8tNP0EFGNnat23NScsj97R32wilCrlRg2m9dKWri7CqnQ8+IwPDA7OeUAHK3tReccAB8+AStSFhtkA=="
# file_path = r"C:\Users\soupatil\Downloads\Data_Quality_Framework_Tool (1)\Data_Quality_Framework_Tool\DQF\Interfaces\Salesforce\config\Config_Rule.xlsx"
#
#
# def initialize_storage_account(storage_account_name, storage_account_key):
#     try:
#         global service_client
#
#         service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
#             "https", storage_account_name), credential=storage_account_key)
#         print(service_client)
#     except Exception as e:
#         print(e)
#
#
# def upload_file_to_directory():
#     try:
#
#         file_system_client = service_client.get_file_system_client(file_system="input")
#
#         directory_client = file_system_client.get_directory_client("config")
#
#         file_client = directory_client.create_file("Config_Rule.xlsx")
#         local_file = open(file_path, 'rb')
#
#         file_contents = local_file.read()
#
#         file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
#
#         file_client.flush_data(len(file_contents))
#
#     except Exception as e:
#         print(e)
#
# initialize_storage_account(storage_account_name, storage_account_key)
# upload_file_to_directory()
