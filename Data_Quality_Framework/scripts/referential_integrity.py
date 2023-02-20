# import pandas as pd
# import pyodbc
# from azure.keyvault.secrets import SecretClient
# from azure.identity import ClientSecretCredential
#
# def test(table_name):
#     server = "dataqualitysynapseaa.sql.azuresynapse.net"
#     database = "dataframeworksqlpool"
#     username = "sqladminuser"
#     password = "Octo@2022"
#     driver = "{ODBC Driver 13 for SQL Server}"
#     conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=tcp:' + server +
#                           ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
#
#     table_names = [a.strip() for a in table_name.split(',')]
#     df_dicts = {}
#     for table_name in table_names:
#         with conn.cursor() as cursor:
#             cursor.execute(("SELECT * FROM " + table_name))
#             col_names = [desc[0] for desc in cursor.description]
#             rows = []
#             for row in cursor:
#                 row_to_list = [elem for elem in row]
#                 rows.append(row_to_list)
#             df = pd.DataFrame(rows, columns=col_names)
#             df_dicts[table_name+"_df"] = df
#     col = "CustomerID"
#     x = df_dicts[table_names[0]+"_df"][~df_dicts[table_names[0]+"_df"][col].isin(df_dicts[table_names[1]+"_df"][col])]
#     print(x.shape[0])
#     return True
#
# print(test("OrderDetails,CustomerDetails"))
#
#
import os
source_folder_path = r"C:\Users\soupatil\Videos\DQF\Data_Quality_Framework_Tool\DQF"
print(len([entry for entry in os.listdir(source_folder_path) if os.path.isfile(os.path.join(source_folder_path, entry))]))
