# # # # # import pandas as pd
# # # # # import os
# # # # #
# # # # # df = pd.read_csv(r"C:\Users\supshinde\PycharmProjects\Data_Quality_Framework_Tool\DQF\Interfaces\Salesforce\config\email_sender_list.csv")
# # # # # print(df)
# # # # # lst = df['email_id'].tolist()
# # # # # print(lst)
# # # #
# # # #
# # # # import win32com.client as win32
# # # # from datetime import datetime
# # # # import os
# # # #
# # # #
# # # # outlook = win32.Dispatch('outlook.application')
# # # # mail = outlook.CreateItem(0)
# # # # mail.Subject = 'Currencies Exchange Prices as of ' + datetime.now().strftime('%#d %b %Y %H:%M')
# # # # attachment = mail.Attachments.Add(r"C:\Users\supshinde\PycharmProjects\Data_Quality_Framework_Tool\DQF\static\admin\img\dqf1.png")
# # # # attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", "dqf_logo")
# # # # lst = ["supriya.shinde30@gmail.com", "supriyashinde3096@gmail.com"]
# # # # mail.HTMLBody = r"""
# # # # Hi,<br><br>
# # # # For more details, you can check the tabs in the Excel file attached.<br><br>
# # # # Best regards,<br>
# # # # Team Deloitte<br><br>
# # # # <img src="cid:dqf_logo"><br><br>
# # # # """
# # # # mail.Attachments.Add(r"C:\Users\supshinde\PycharmProjects\Data_Quality_Framework_Tool\DQF\Interfaces\Salesforce\config\email_sender_list.csv")
# # # # for i in lst:
# # # #     mail.To = i
# # # #     mail.Send()
# # # ###########################################################################
# # #
# # # import os, uuid, sys
# # # from azure.storage.filedatalake import DataLakeServiceClient
# # # from azure.core._match_conditions import MatchConditions
# # # from azure.storage.filedatalake._models import ContentSettings
# # #
# # #
# # # service_client = ""
# # # file_path = r"C:\Users\supshinde\PycharmProjects\Data_Quality_Framework_Tool\DQF\Interfaces\Salesforce\config\Config_Rule.xlsx"
# # # storage_account_name = "dataqfvalidation"
# # # storage_account_key = "DyQmqEmb3XSc+VBYib/iRcPPrAfdFN39NAUXefRlrhqg/cdrc86Y9vtBPd/HkEfI3to7COSA1PRc+ASt8g+ffQ=="
# # #
# # #
# # # def initialize_storage_account(storage_account_name, storage_account_key):
# # #     try:
# # #         global service_client
# # #
# # #         service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
# # #             "https", storage_account_name), credential=storage_account_key)
# # #         print(service_client)
# # #     except Exception as e:
# # #         print(e)
# # #
# # #
# # # def upload_file_to_directory():
# # #     try:
# # #
# # #         file_system_client = service_client.get_file_system_client(file_system="input")
# # #
# # #         directory_client = file_system_client.get_directory_client("config")
# # #
# # #         file_client = directory_client.create_file("Config_Rule.xlsx")
# # #         local_file = open(file_path, 'rb')
# # #
# # #         file_contents = local_file.read()
# # #
# # #         file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
# # #
# # #         file_client.flush_data(len(file_contents))
# # #
# # #     except Exception as e:
# # #         print(e)
# # #
# # # initialize_storage_account(storage_account_name, storage_account_key)
# # # upload_file_to_directory()
# #
# # ##########################################################################
# # import os
# # #
# # path = r"C:\Users\supshinde\PycharmProjects\Data_Quality_Framework_Tool\DQF\Projects\Salesforce\DQ_input"
# # # # if "*" in source_file:
# # # for (root, dirs, file) in os.walk(path):
# # #   print("Directories and Files in output folder => ", dirs, file)
# # #   for f in file:
# # #     # print(f, root)
# # #     print(root + "\\" + f)
# # #     # dataset = read_source_file(source_file)
# #
# # path = path.replace('*', '')
# # root, dirs, files = os.walk(path).__next__()
# # print(files)
# # # for f in file:
# # #     # print(f, root)
# # #     print(root + "/" + f)
# # #     # dataset = read_source_file(source_file)
# # ########################################################################
# #
# # from pyspark.sql.functions import lit
# #
# # config_file_path = "dbfs:/mnt/dataqfvalidation/input/config/Config_Rule (3).xlsx"
# # new_file_path = "dbfs:/mnt/dataqfvalidation/input/config/sample_data.avro"
# #
# # df = spark.read.format("com.crealytics.spark.excel").\
# #      option("header", "true").\
# #      option("path", config_file_path).load()
# # new_df = df.withColumn('source_file', lit(new_file_path))
# # print(new_df)
# # # dbfs:/FileStore/shared_uploads/dataqualitywithaa@outlook.com/Config_Rule__3_.xlsx
# # # /dbfs/FileStore/shared_uploads/dataqualitywithaa@outlook.com/Config_Rule__3_.xlsx
# #
# #
# # #####################################################################################3
# #
# #
# # def check_rules(admin_config, i, rule_id, validation_column, execution_parameter, out_file_path):
# #      source_file = ""
# #      table = ""
# #      source_path = ""
# #      conf_obj, output_folder_path, output_file_path, project_path, rule_config_file, admin_file = folder_paths()
# #      if admin_config['rule_action_type'][i] == "file":
# #           source_file = admin_config["source_file"][i]
# #           if "*" in source_file:
# #                source_file = source_file.replace('*', '')
# #                root, dirs, files = os.walk(source_file).__next__()
# #                for f in files:
# #                     dataset = read_source_file(root + "\\" + f)
# #                     call_checks(admin_config, i, rule_id, validation_column, execution_parameter, out_file_path, dataset, source_file,source_path, table)
# #           else:
# #                source_path = project_path + conf_obj["File_Configuration"]["input_folder_path"]
# #                source_file_path = source_path + str(source_file)
# #                dataset = read_source_file(source_file_path)
# #                call_checks(admin_config, i, rule_id, validation_column, execution_parameter, out_file_path, dataset, source_file,source_path, table)
# #      elif admin_config['rule_action_type'][i] == "table":
# #           table = admin_config["validation_table"][i]
# #           platform_name = admin_config['Platform'][i]
# #           dataset = establish_db_conn(conf_obj, table, platform_name)
# #           call_checks(admin_config, i, rule_id, validation_column, execution_parameter, out_file_path, dataset, source_file,source_path, table)
# #
# #
# # def call_checks(admin_config, i, rule_id, validation_column, execution_parameter, out_file_path, dataset, source_file,source_path, table):
# #      detailed_output_check = admin_config["detailed_output"][i]
# #
# #      # Column Nullability Check
# #      if admin_config["rule_name"][i] == 'not_null':
# #           output_message = column_nullability_check(admin_config["config_id"][i], rule_id,
# #                                                     dataset,
# #                                                     validation_column, source_file, table,
# #                                                     execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #
# #      # Checking Duplicate
# #      elif admin_config["rule_name"][i] == 'unique':
# #
# #           output_message = duplicate_check(admin_config["config_id"][i], rule_id, dataset,
# #                                            validation_column,
# #                                            source_file, table, execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check integer datatype
# #      elif admin_config["rule_name"][i] == 'int_check':
# #           output_message = integer_datatype_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                   validation_column,
# #                                                   source_file, table, execution_parameter,
# #                                                   admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check char datatype
# #      elif admin_config["rule_name"][i] == 'char_check':
# #           output_message = char_datatype_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                validation_column,
# #                                                source_file, table, execution_parameter,
# #                                                admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check varchar datatype
# #      elif admin_config["rule_name"][i] == 'varchar_check':
# #           output_message = varchar_datatype_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                   validation_column,
# #                                                   source_file, table, execution_parameter,
# #                                                   admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check boolean datatype
# #      elif admin_config["rule_name"][i] == 'boolean_check':
# #           output_message = boolean_datatype_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                   validation_column,
# #                                                   source_file, table, execution_parameter,
# #                                                   admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check values in specified decimal points
# #      elif admin_config["rule_name"][i] == 'decimal_check':
# #           output_message = decimal_check(admin_config["config_id"][i], rule_id, dataset,
# #                                          validation_column,
# #                                          source_file, table, execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check values in specified list
# #      elif admin_config["rule_name"][i] == 'lst_values_check':
# #           output_message = specified_list_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                 validation_column,
# #                                                 source_file, table, execution_parameter,
# #                                                 admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check values in specified dateformat type
# #      elif admin_config["rule_name"][i] == 'date_format_check':
# #           output_message = dateformat_check(admin_config["config_id"][i], rule_id, dataset,
# #                                             validation_column,
# #                                             source_file, table, execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # Check values in specified time type
# #      elif admin_config["rule_name"][i] == 'timestamp_check':
# #           output_message = time_check(admin_config["config_id"][i], rule_id, dataset,
# #                                       validation_column,
# #                                       source_file, table, execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #           if detailed_output_check == "Y":
# #                write_detailed_output(output_message, out_file_path)
# #
# #      # DataSet size check
# #      elif admin_config["rule_name"][i] == 'file_size':
# #           output_message = dataset_size_check(admin_config["config_id"][i], rule_id, dataset,
# #                                               source_file_path, source_file, table,
# #                                               execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #
# #      # dataset file extension check
# #      elif admin_config["rule_name"][i] == 'file_extension':
# #           output_message = file_extension_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                 source_file, table,
# #                                                 execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #
# #      # count dataset column
# #      elif admin_config["rule_name"][i] == 'file_col_count_check':
# #           output_message = dataset_column_count(admin_config["config_id"][i], rule_id, dataset,
# #                                                 source_file, table,
# #                                                 execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #
# #      elif admin_config["rule_name"][i] == 'header_pattern_check':
# #           output_message = header_pattern_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                 validation_column,
# #                                                 source_file, table, execution_parameter,
# #                                                 admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #
# #      elif admin_config["rule_name"][i] == 'pattern_check':
# #           output_message = pattern_check(admin_config["config_id"][i], rule_id, dataset,
# #                                          validation_column,
# #                                          source_file, table, execution_parameter, admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #
# #      elif admin_config["rule_name"][i] == 'file_availability_check':
# #           output_message = file_availability_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                    validation_column,
# #                                                    source_path, table, execution_parameter,
# #                                                    admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
# #
# #      elif admin_config["rule_name"][i] == 'file_folder_availability_check':
# #           output_message = file_folder_availability_check(admin_config["config_id"][i], rule_id, dataset,
# #                                                           validation_column,
# #                                                           source_path, table, execution_parameter,
# #                                                           admin_config["DQ_CHECK_STAGE"][i])
# #           write_output_data(output_message, out_file_path)
#
# validation_column = "OrderDetails.CustomerID,CustomerDetails.CustomerID"
# lst1 = [a.strip() for a in validation_column.split(',')]
# col_dict = {}
# for i in lst1:
#     lst2 = i.split('.')
#     col_dict[lst2[0]] = lst2[1]


import pandas as pd
#
# df1 = pd.DataFrame({'a':[1,2,3], 'b':[4,5,6]})
# df2 = pd.DataFrame({'a':[14,24,34], 'b':[444,445,446]})
#
# dict1 = {"df1":df1, "df2": df2}
# print(type(dict1['df1']))

rule_config_file = pd.read_excel(r"C:\Users\soupatil\Videos\DQF\Data_Quality_Framework_Tool\DQF\templates\Config_Rule_with_records.xlsx",storage_options={})