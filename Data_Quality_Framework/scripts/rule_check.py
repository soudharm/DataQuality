# import required libraries
import pathlib
import os
import operator
import ast
import decimal
from datetime import datetime
import time
import pandas as pd
from os.path import exists
from scripts.cloud_storages import establish_db_conn
import warnings
warnings.filterwarnings("ignore")


def string_to_dict(string):
    """
    Converts string to dictionary datatype
    """
    return ast.literal_eval(string)


def convert_to_bytes(size_str):
    """
    Converts torrent sizes to a common count in bytes.
    """
    size_data = size_str.split()

    multipliers = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']

    size_magnitude = float(size_data[0])
    multiplier_exp = multipliers.index(size_data[1])
    size_multiplier = 1024 ** multiplier_exp if multiplier_exp > 0 else 1
    return size_magnitude * size_multiplier


def check_execution_parameter(execution_parameter, value, func_name):
    """
    Checks execution parameters
    """
    ops = {'+': operator.add, '-': operator.sub, '*': operator.mul, '/': operator.truediv,
           '%': operator.mod, '^': operator.xor, '<': operator.lt, '<=': operator.le,
           '=': operator.eq, '!=': operator.ne, '>=': operator.ge, '>': operator.gt}
    if execution_parameter != "nan" and len(execution_parameter) != 0:
        if func_name == "file_size_check":
            lst = execution_parameter.split(',')
            comparison_value = lst[0].strip()
            comparison_unit = lst[1].strip()
            comparison_operator = lst[2].strip()
            size = comparison_value + " " + comparison_unit
            size_in_bytes = convert_to_bytes(size)
            output = ops[comparison_operator](value, size_in_bytes)
            if output:
                message = "Size of file is " + comparison_operator + " " + \
                          comparison_value + comparison_unit
            else:
                message = "Size of file is NOT " + comparison_operator + " " + \
                          comparison_value + comparison_unit
            return message
        elif func_name == "column_count_check":
            lst = execution_parameter.split(',')
            comparison_value = lst[0].strip()
            comparison_operator = lst[1].strip()
            output = ops[comparison_operator](value, int(comparison_value))
            if output:
                message = "Number of columns in dataset are " + comparison_operator + " " + \
                          comparison_value
            else:
                message = "Number of columns in dataset are NOT " + comparison_operator + " " + \
                          comparison_value
            return message
        elif func_name == "decimal_check":
            lst = execution_parameter.split(',')
            comparison_value = lst[0].strip()
            comparison_operator = lst[1].strip()
            output = ops[comparison_operator](value, int(comparison_value))
            return output
        elif func_name == "column_length_check":
            lst = execution_parameter.split(',')
            comparison_value = lst[0].strip()
            comparison_operator = lst[1].strip()
            output = ops[comparison_operator](value, int(comparison_value))
            return output

def read_source_file(source_file_path):
    file_extension = pathlib.Path(source_file_path).suffix
    dataset = ""
    if file_extension == '.csv':
        dataset = pd.read_csv(source_file_path)
    elif file_extension in ['.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
        dataset = pd.read_excel(source_file_path)
    elif file_extension == ".json":
        dataset = pd.read_json(source_file_path)
    elif file_extension == ".xml":
        dataset = pd.read_xml(source_file_path)
    elif file_extension == ".parquet":
        dataset = pd.read_parquet(source_file_path, engine="pyarrow")
    return dataset


def column_nullability_check(config_id, rule_id, dataset, validation_column, source_file, table,
                             execution_parameter, dq_check_action):
    """
    This Function is checking the number of Null value in a requested column
    """
    detailed_list = []
    dq_check_action_flag = False
    sum_null = dataset.loc[dataset[validation_column] == ''].count().iloc[0]
    if dataset[validation_column].isnull().any():
        null_rows = dataset[dataset[validation_column].isnull()]
        for i in null_rows.index:
            detailed_list.append(dict(dataset.loc[i]))
        message = validation_column + " has null value"
        null_count = dataset[validation_column].isnull().sum(axis=0)
        if dq_check_action == 'error':
            dq_check_action_flag = True
    elif sum_null != 0:
        null_rows = dataset[dataset[validation_column].values == '']
        for i in null_rows.index:
            detailed_list.append(dict(dataset.loc[i]))
        message = validation_column + " has null value"
        null_count = sum_null
        if dq_check_action == 'error':
            dq_check_action_flag = True
    else:
        message = validation_column + " has no null value"
        null_count = 0
    result = config_id, rule_id, 'not_null', source_file, table, validation_column, message, null_count, detailed_list, dq_check_action_flag
    return result


def duplicate_check(config_id, rule_id, dataset, validation_column, source_file, table, execution_parameter, dq_check_action):
    """
    This function is checking the number of duplicate value in a requested column
    """

    detailed_list = []
    dq_check_action_flag = False
    if dataset[validation_column].duplicated().any():
        duplicate_rows = dataset[dataset.duplicated(validation_column, keep=False)]
        for i in duplicate_rows.index:
            detailed_list.append(dict(dataset.loc[i]))
        message = validation_column + " has duplicate value"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    else:
        message = validation_column + " has no duplicate value"
    duplicate_count = dataset[validation_column].duplicated().sum()
    result = config_id, rule_id, "unique", source_file, table, validation_column, message, duplicate_count, detailed_list, dq_check_action_flag
    return result


def integer_datatype_check(config_id, rule_id, dataset, validation_column, source_file, table,
                           execution_parameter, dq_check_action):
    """
    Checks datatype of column is integer or not
    """
    print(dataset, type(dataset), "!!!!!!!!!!!!!!!!!!!!!!!!!!!!11111")
    int_count = 0
    detailed_list = []
    dq_check_action_flag = False
    if dataset[validation_column].dtype == 'int64':
        message = validation_column + " is int datatype"
    else:
        for i in dataset.index:
            val = dataset[validation_column][i]
            try:
                if not isinstance(val, int):
                    int_val = int(val)
            except ValueError:
                int_count += 1
                detailed_list.append(dict(dataset.loc[i]))
        message = validation_column + " is not int datatype"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "int_check", source_file, table, validation_column, message, int_count, detailed_list, dq_check_action_flag
    return result


def char_datatype_check(config_id, rule_id, dataset, validation_column, source_file, table,
                        execution_parameter, dq_check_action):
    """
    Checks datatype of column is char or not
    """
    char_count = 0
    detailed_list = []
    dq_check_action_flag = False
    for i in dataset:
        val = dataset[validation_column][i]
        if isinstance(val, str) and len(val) == 1:
            try:
                x = int(val)
                char_count += 1
                detailed_list.append(dict(dataset.loc[i]))
            except ValueError:
                pass
        else:
            char_count += 1
            detailed_list.append(dict(dataset.loc[i]))
    if char_count == 0:
        message = validation_column + " is char datatype"
    else:
        message = validation_column + " is not char datatype"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "char_check", source_file, table, validation_column, message, char_count, detailed_list, dq_check_action_flag
    return result


def varchar_datatype_check(config_id, rule_id, dataset, validation_column, source_file, table,
                        execution_parameter, dq_check_action):
    """
    Checks datatype of column is char or not
    """
    varchar_count = 0
    detailed_list = []
    dq_check_action_flag = False
    for i in dataset.index:
        val = dataset[validation_column][i]
        if isinstance(val, str):
            try:
                if "." in val:
                    x = float(val)
                else:
                    x = int(val)
                varchar_count += 1
                detailed_list.append(dict(dataset.loc[i]))
            except ValueError:
                pass
        else:
            varchar_count += 1
            detailed_list.append(dict(dataset.loc[i]))
    if varchar_count == 0:
        message = validation_column + " is varchar datatype"
    else:
        message = validation_column + " is not varchar datatype"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "varchar_check", source_file, table, validation_column, message, varchar_count, detailed_list, dq_check_action_flag
    return result


def boolean_datatype_check(config_id, rule_id, dataset, validation_column, source_file, table,
                           execution_parameter, dq_check_action):
    """
    Checks datatype of column is boolean or not
    """
    boolean_count = 0
    detailed_list = []
    dq_check_action_flag = False
    if dataset[validation_column].dtype == 'bool':
        message = validation_column + " is boolean datatype"
    else:
        for i in dataset.index:
            val = dataset[validation_column][i]
            if isinstance(val, bool) or (val == 1 or val == 0):
                pass
            else:
                boolean_count += 1
                detailed_list.append(dict(dataset.loc[i]))
        message = validation_column + " is not boolean datatype"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "boolean_check", source_file, table, validation_column, message, boolean_count, detailed_list, dq_check_action_flag
    return result


def decimal_check(config_id, rule_id, dataset, validation_column, source_file, table, execution_parameter, dq_check_action):
    """
    To check decimal type in Target column
    """
    decimal_count = 0
    detailed_list = []
    comparison_value = ""
    comparison_operator = ""
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        lst = execution_parameter.split(',')
        comparison_value = lst[0].strip()
        comparison_operator = lst[1].strip()
        for i in dataset.index:
            try:
                val = dataset[validation_column][i]
                dec_val = decimal.Decimal(str(val))
                final_val = abs(dec_val.as_tuple().exponent)
                output = check_execution_parameter(execution_parameter, final_val, "decimal_check")
                if output is False:
                    decimal_count += 1
                    detailed_list.append(dict(dataset.loc[i]))
            except:
                decimal_count += 1
                detailed_list.append(dict(dataset.loc[i]))
    if decimal_count == 0:
        message = validation_column + " values are " + comparison_operator + comparison_value + " decimal points"
    else:
        message = validation_column + " values are not " + comparison_operator + comparison_value + " decimal points"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "decimal_check", source_file, table, validation_column, message, decimal_count, detailed_list, dq_check_action_flag
    return result


def specified_list_check(config_id, rule_id, dataset, validation_column, source_file, table,
                         execution_parameter, dq_check_action):
    """
    To check values in specified list are not in Target column
    """
    lst_count = 0
    detailed_list = []
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        lst = execution_parameter.split(',')
        execution_parameter_lst = [ele for ele in lst if ele.strip()]
        for i in dataset.index:
            val = dataset[validation_column][i]
            if val not in execution_parameter_lst:
                lst_count += 1
                detailed_list.append(dict(dataset.loc[i]))
        if lst_count == 0:
            message = validation_column + " values are among " + execution_parameter
        else:
            message = validation_column + " values are not among " + execution_parameter
            if dq_check_action == 'error':
                dq_check_action_flag = True
    else:
        message = validation_column + " specified list not provided in execution parameter"
    result = config_id, rule_id, "lst_values_check", source_file, table, validation_column, message, lst_count, detailed_list, dq_check_action_flag
    return result


def dateformat_check(config_id, rule_id, dataset, validation_column, source_file, table,
                     execution_parameter, dq_check_action):
    """
    To check dateformat type in Target column
    """
    date_count = 0
    detailed_list = []
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        date_format = execution_parameter
        if date_format == 'YYYY-MM-DD':
            date_format = '%Y-%m-%d'
        elif date_format == 'YYYY/MM/DD':
            date_format = '%Y/%m/%d'
        elif date_format == 'YY-MM-DD':
            date_format = '%y-%m-%d'
        elif date_format == 'DD-MM-YY':
            date_format = '%d-%m-%y'
        elif date_format == 'DD/MM/YY':
            date_format = '%d/%m/%y'
        elif date_format == 'DD/MM/YYYY':
            date_format = '%d/%m/%Y'
        elif date_format == 'DD-MM-YYYY':
            date_format = '%d-%m-%Y'
        for i in dataset.index:
            val = dataset[validation_column][i]
            try:
                res = bool(datetime.strptime(str(val), date_format))
            except ValueError:
                date_count += 1
                detailed_list.append(dict(dataset.loc[i]))
    if date_count == 0:
        message = validation_column + " values are in " + execution_parameter + " date format"
    else:
        message = validation_column + " values are not in " + execution_parameter + " date format"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "date_format_check", source_file, table, validation_column, message, date_count, detailed_list, dq_check_action_flag
    return result


def time_check(config_id, rule_id, dataset, validation_column, source_file, table, execution_parameter, dq_check_action):
    """
    To check time format type in Target column
    """
    time_count = 0
    detailed_list = []
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        time_format = execution_parameter
        if time_format == "H:M:S":
            time_format = "%H:%M:%S"
        elif time_format == "H-M-S":
            time_format = "%H-%M-%S"
        elif time_format == "H:M":
            time_format = "%H:%M"
        elif time_format == "H-M":
            time_format = "%H-%M"
        for i in dataset.index:
            val = dataset[validation_column][i]
            try:
                res = time.strptime(str(val), time_format)
            except ValueError:
                time_count += 1
                detailed_list.append(dict(dataset.loc[i]))
    if time_count == 0:
        message = validation_column + " values are in " + execution_parameter + " time format"
    else:
        message = validation_column + " values are not in " + execution_parameter + " time format"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "timestamp_check", source_file, table, validation_column, message, time_count, detailed_list, dq_check_action_flag
    return result


def dataset_size_check(config_id, rule_id, dataset, source_file_path, source_file,  table, execution_parameter, dq_check_action):
    """
    This function calculate the size of the data set
    """
    output = None
    dq_check_action_flag = False
    file_size = os.path.getsize(source_file_path)  # size in bytes
    finalSize = (round(file_size / 1024, 3))
    if type(execution_parameter) == str:
        output = check_execution_parameter(execution_parameter, file_size, "file_size_check")
    if output:
        message = output
    else:
        message = "size of the file is " + str(finalSize) + " KB"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "file_size", source_file, table, '', message, '', '', dq_check_action_flag
    return result


def file_extension_check(config_id, rule_id, dataset, source_file, table, execution_parameter, dq_check_action):
    """
    This function checks the extension of file
    """

    dq_check_action_flag = False
    file_extension = pathlib.Path(source_file).suffix
    message = "File Extension is " + file_extension
    result = config_id, rule_id, "file_extension", source_file, table, '', message, '', '', dq_check_action_flag
    return result


def dataset_column_count(config_id, rule_id, dataset, source_file, table, execution_parameter, dq_check_action):
    """
    This function counts the number of columns in Data Set
    """
    output = None
    dq_check_action_flag = False
    column_count = len(dataset.columns)
    if type(execution_parameter) == str:
        output = check_execution_parameter(execution_parameter, column_count, "column_count_check")
    if output:
        message = output
    else:
        message = "Number of columns in Dataset are " + str(column_count)
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "file_col_count_check", source_file, table, '', message, '', '', dq_check_action_flag
    return result

def header_pattern_check(config_id, rule_id, dataset, validation_column, source_file, table, execution_parameter, dq_check_action):
    dq_check_action_flag = False
    column_count = dataset.columns
    if validation_column in column_count:
        message = validation_column + " is available in header"
    else:
        message = validation_column + " is not available in header"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "header_pattern_check", source_file, table, '', message, '', '', dq_check_action_flag
    return result

def pattern_check(config_id, rule_id, dataset, validation_column, source_file, table, execution_parameter, dq_check_action):
    message = ""
    detailed_list = []
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        lst = execution_parameter.split(',')
        execution_parameter_lst = [ele for ele in lst if ele.strip()]
        for lst_val in execution_parameter_lst:
            value_check = (dataset.eq(lst_val)).any().sum()
            if value_check > 0:
                message = "The pattern " + execution_parameter +" is available"
            else:
                message = "The pattern " + execution_parameter +" is not available"
                if dq_check_action == 'error':
                    dq_check_action_flag = True
    result = config_id, rule_id, "pattern_check", source_file, table, validation_column, message, '', detailed_list, dq_check_action_flag
    return result

def file_availability_check(config_id, rule_id, dataset, validation_column, source_path, table, execution_parameter, dq_check_action):
    message = ""
    detailed_list = []
    source_file_path = ""
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        execution_parameter = execution_parameter.strip()
        source_file_path = source_path + execution_parameter
        file_exists = exists(source_file_path)
        if file_exists:
            message = "File "+ execution_parameter + " is available"
        else:
            message = "File " + execution_parameter+ " is NOT available"
            if dq_check_action == 'error':
                dq_check_action_flag = True

    result = config_id, rule_id, "file_availability_check", source_file_path, table, validation_column, message, '', detailed_list, dq_check_action_flag
    return result

def file_folder_availability_check(config_id, rule_id, dataset, validation_column, source_path, table, execution_parameter, dq_check_action):
    message = ""
    detailed_list = []
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        execution_parameter = execution_parameter.strip()
        file_exists = exists(execution_parameter)
        if file_exists:
            message = "File " + execution_parameter+ " is available"
        else:
            message = "File " + execution_parameter+ " is NOT available"
            if dq_check_action == 'error':
                dq_check_action_flag = True

    result = config_id, rule_id, "file_folder_availability_check", '', table, validation_column, message, '', detailed_list, dq_check_action_flag
    return result



def relationship(config_id, rule_id, validation_column, source_file, table, platform_name, dq_check_action, conf_obj):
    """
    This function is checking the referential integrity of tables
    """

    dq_check_action_flag = False
    table_names = [a.strip() for a in table.split(',')]
    # column_names = [a.strip() for a in validation_column.split(',')]
    lst1 = [a.strip() for a in validation_column.split(',')]
    column_names = {}
    for i in lst1:
        lst2 = i.split('.')
        column_names[lst2[0]] = lst2[1]
    df_dicts = {}
    record_count = 0
    for table_name in table_names:
        df = establish_db_conn(conf_obj, table_name, platform_name)
        df_dicts[table_name + "_df"] = df
    uncommon_records = df_dicts[table_names[0]+"_df"][~df_dicts[table_names[0]+"_df"][column_names[table_names[0]]].isin(df_dicts[table_names[1]+"_df"][column_names[table_names[1]]])]
    if not uncommon_records.empty:
        record_count = uncommon_records.shape[0]
        message =  "Few records from " + table_names[1] + " does not refers to " + table_names[0] + " records."
        if dq_check_action == 'error':
            dq_check_action_flag = True
    else:
        message = "All record from " + table_names[1] + " refers to " + table_names[0] + " records."
    result = config_id, rule_id, "relationship", source_file, table, validation_column, message, record_count, uncommon_records, dq_check_action_flag
    return result


def file_count(config_id, rule_id, source_path, source_file, table, execution_parameter, dq_check_action):
    """
    This function checks if source and target folder contains same number of files or not
    """
    global destination_count, source_count
    dq_check_action_flag = False
    paths = [a.strip() for a in source_file.split(',')]
    source_folder_path = paths[0]
    destination_folder_path = paths[1]
    source_count = len([entry for entry in os.listdir(source_folder_path) if os.path.isfile(os.path.join(source_folder_path, entry))])
    destination_count = len([entry for entry in os.listdir(destination_folder_path) if os.path.isfile(os.path.join(destination_folder_path, entry))])
    if source_count != destination_count:
        message = "Number of files in source and destination folder does not match"
    else:
        message = "Number of files in source and destination folder does match"
    if dq_check_action == 'error':
        dq_check_action_flag = True
    result = config_id, rule_id, "file_count_check", source_file, table, '', message, '', '', dq_check_action_flag
    return result


def column_length_check(config_id, rule_id, dataset, validation_column, source_file, table, execution_parameter, dq_check_action):
    """
    To check decimal type in Target column
    """
    value_count = 0
    detailed_list = []
    comparison_value = ""
    comparison_operator = ""
    dq_check_action_flag = False
    if type(execution_parameter) == str:
        lst = execution_parameter.split(',')
        comparison_value = lst[0].strip()
        comparison_operator = lst[1].strip()
        for i in dataset.index:
            try:
                val = dataset[validation_column][i]
                length_of_val = len(val)
                output = check_execution_parameter(execution_parameter, length_of_val, "column_length_check")
                if output is False:
                    value_count += 1
                    detailed_list.append(dict(dataset.loc[i]))
            except:
                value_count += 1
                detailed_list.append(dict(dataset.loc[i]))
    if value_count == 0:
        message = validation_column + " values are " + comparison_operator + comparison_value + " specified length"
    else:
        message = validation_column + " values are not " + comparison_operator + comparison_value + " specified length"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    result = config_id, rule_id, "column_length_check", source_file, table, validation_column, message, value_count, detailed_list, dq_check_action_flag
    return result


def dataset_equality_check(config_id, rule_id, source_path, source_file, table, execution_parameter, dq_check_action):
    dq_check_action_flag = False
    detailed_list = []
    paths = [a.strip() for a in source_file.split(',')]
    source_folder_path = paths[0]
    destination_folder_path = paths[1]
    source_dataset = read_source_file(source_folder_path)
    destination_dataset = read_source_file(destination_folder_path)
    if source_dataset.equals(destination_dataset):
        message = "Records in source and destination dataset does not match"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    else:
        message = "Records in source and destination dataset does match"
    result = config_id, rule_id, "dataset_equality_check", source_file, table, '', message, '', detailed_list, dq_check_action_flag
    return result


def dataset_length_check(config_id, rule_id, source_path, source_file, table, execution_parameter, dq_check_action):
    dq_check_action_flag = False
    detailed_list = []
    paths = [a.strip() for a in source_file.split(',')]
    source_folder_path = paths[0]
    destination_folder_path = paths[1]
    source_dataset = read_source_file(source_folder_path)
    destination_dataset = read_source_file(destination_folder_path)
    source_rows_count = source_dataset.shape[0]
    destination_rows_count = destination_dataset.shape[0]
    if source_rows_count != destination_rows_count:
        message = "Number of records in source and destination dataset does not match"
        if dq_check_action == 'error':
            dq_check_action_flag = True
    else:
        message = "Number of records in source and destination dataset does match"
    result = config_id, rule_id, "dataset_length_check", source_file, table, '', message, '', detailed_list, dq_check_action_flag
    return result


def dataset_content_check(config_id, rule_id, source_path, source_file, table, execution_parameter, dq_check_action):
    dq_check_action_flag = False
    row_count = 0
    paths = [a.strip() for a in source_file.split(',')]
    source_folder_path = paths[0]
    destination_folder_path = paths[1]
    df1 = read_source_file(source_folder_path)
    df2 = read_source_file(destination_folder_path)
    df1['match'] = df1.apply(lambda x: hash(tuple(x)), axis=1)
    df2['match'] = df2.apply(lambda x: hash(tuple(x)), axis=1)
    df_diff = df1[~df1['match'].isin(df2['match'])]
    df_diff1 = df2[~df2['match'].isin(df1['match'])]
    df3 = df_diff.append(df_diff1)
    df3.drop('match', axis=1, inplace=True)
    if not df3.empty:
        message = "Content of source and destination dataset does not match"
        row_count = df3.shape[0]
        if dq_check_action == 'error':
            dq_check_action_flag = True
    else:
        message = "Content of source and destination dataset does match"
    result = config_id, rule_id, "dataset_content_check", source_file, table, '', message, row_count, df3, dq_check_action_flag
    return result

