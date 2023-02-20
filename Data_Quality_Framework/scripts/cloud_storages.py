import pandas as pd
import snowflake.connector
import pyodbc
import pathlib
import boto3
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from django.conf import settings


def get_credentials(secretname):
    client_id = settings.AZURE_CLIENT_ID
    client_secret = settings.AZURE_CLIENT_SECRET
    keyvault_name = settings.AZURE_KEYVAULT_NAME
    tenant_id = settings.AZURE_TENANT_ID
    KVUri = f"https://{keyvault_name}.vault.azure.net"
    _credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    _sc = SecretClient(vault_url=KVUri, credential=_credential)

    retrieved_secret = _sc.get_secret(secretname).value
    return retrieved_secret

def establish_db_conn(conf_obj, table_name, platform_name):
    """
    Connects with cloud database and returns dataframe
    """
    conn = ""
    try:
        if platform_name == 'snowflake':
            account_name = conf_obj["DB_Connection"]["snowflake_account"]
            # user_name = get_credentials("snowflakeusername")
            # password = get_credentials("snowflakepassword")
            user_name = "DATAQUALITY"
            password = "Admin@123"
            database = conf_obj["DB_Connection"]["snowflake_database"]
            warehouse = conf_obj["DB_Connection"]["snowflake_warehouse"]
            conn = snowflake.connector.connect(
                account=account_name,
                user=user_name,
                password=password,
                database=database,
                warehouse=warehouse
            )
        elif platform_name == 'synapse':
            server = conf_obj["DB_Connection"]["synapse_server"]
            database = conf_obj["DB_Connection"]["synapse_database"]
            username = get_credentials("synapseusername")
            password = get_credentials("synapsepassword")
            # username = "sqladminuser"
            # password = "Admin@123"
            driver = conf_obj["DB_Connection"]["synapse_driver"]
            conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=tcp:' + server +
                                  ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

        with conn.cursor() as cursor:
            cursor.execute(("SELECT * FROM " + table_name))
            col_names = [desc[0] for desc in cursor.description]
            rows = []
            for row in cursor:
                row_to_list = [elem for elem in row]
                rows.append(row_to_list)
            df = pd.DataFrame(rows, columns=col_names)
            print(df, 111111111111111111111)
            return df
    except:
        return False

def azure_datalake_storage(conf_obj):
    connection_string = conf_obj["ADLS_Connection_Details"]["connection_string"]
    file_path = conf_obj["ADLS_Connection_Details"]["file_path"]
    rule_config_file = ""
    admin_file = ""
    try:
        # admin_file = pd.read_excel(
        #     f"abfs://{file_path}",
        #     storage_options={
        #         "connection_string": connection_string
        #     }, sheet_name=["DQ_RULE_ADMIN"])
        rule_config_file = pd.read_excel(
            f"abfs://{file_path}",
            storage_options={
                "connection_string": connection_string
            }, sheet_name=["DQ_RULE_CONFIG"])
    except Exception as e:
        print(e)
    return rule_config_file, admin_file

def read_file_adls(conf_obj, file_path):
    connection_string = conf_obj["ADLS_Connection_Details"]["connection_string"]
    dataset = ""
    try:
        file_extension = pathlib.Path(file_path).suffix
        if file_extension == '.csv':
            dataset = pd.read_csv(f"abfs://{file_path}",storage_options={"connection_string": connection_string})
        elif file_extension in ['.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            dataset = pd.read_excel(f"abfs://{file_path}",storage_options={"connection_string": connection_string})
        elif file_extension == ".json":
            dataset = pd.read_json(f"abfs://{file_path}",storage_options={"connection_string": connection_string})
        elif file_extension == ".xml":
            dataset = pd.read_xml(f"abfs://{file_path}",storage_options={"connection_string": connection_string})
        elif file_extension == ".parquet":
            dataset = pd.read_parquet(f"abfs://{file_path}",storage_options={"connection_string": connection_string}, engine="pyarrow")
    except Exception as e:
        print(e)
    return dataset

def connect_aws_storage(conf_obj):
    rule_config_file = ""
    admin_file = ""
    aws_access_key_id = conf_obj["AWS_Connection_Details"]["aws_access_key_id"]
    aws_secret_access_key = conf_obj["AWS_Connection_Details"]["aws_secret_access_key"]
    s3_bucket_name = conf_obj['AWS_Connection_Details']['s3_bucket_name']
    file_path = conf_obj["AWS_Connection_Details"]["file_path"]
    s3 = boto3.resource('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
    try:
        obj = s3.Object(s3_bucket_name, file_path)
        data = obj.get()['Body'].read()
        admin_file = pd.read_excel(data, sheet_name=["DQ_RULE_ADMIN"])
        rule_config_file = pd.read_excel(data, sheet_name=["DQ_RULE_CONFIG"])
        return rule_config_file, admin_file

    except Exception as e:
        print(e)
    return rule_config_file, admin_file

def establish_db_conn1(table_name, platform_name):
    """
    Connects with cloud database and returns dataframe
    """
    conn = ""
    if platform_name == 'snowflake':
        print("2222222222222222222222222222222")
        account_name = "jq62272.central-india.azure"
        # user_name = get_credentials("snowflakeusername")
        # password = get_credentials("snowflakepassword")
        user_name = "DATAQUALITYCHECK"
        password = "Admin@123"
        database = "SNOWFLAKE_SAMPLE_DATA"
        warehouse = "TEST"
        print("4444444444444444444444444")
        conn = snowflake.connector.connect(
            account=account_name,
            user=user_name,
            password=password,
            database=database,
            warehouse=warehouse,
            schema="TPCDS_SF100TCL"
        )
        print("33333333333333333333333")
    with conn.cursor() as cursor:
        print("11111111111111111111111111111111111111111111111111")
        cursor.execute(("SELECT * FROM " + table_name))
        col_names = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor:
            row_to_list = [elem for elem in row]
            rows.append(row_to_list)
        df = pd.DataFrame(rows, columns=col_names)
        print(df, 111111111111111111111)
        return df
    # except:
    #     return False

establish_db_conn1("CALL_CENTER", "snowflake")