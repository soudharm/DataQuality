import pandas as pd
from dataprofiler import Data, Profiler


def statistics_of_df(df):
    profile = Profiler(df)
    readable_report = profile.report(report_options={"output_format": "pretty"})
    global_stats = readable_report['global_stats']
    data_stats = readable_report['data_stats']
    dict1 = {"column_name": [], "data_type": [], "% of unique": [], "% of null": [], "num_zeros": [], "min": [], "max": [], "mode": [],
             "median": [], "sum": [], "mean": [], "variance": [], "stddev": [], "skewness": []}
    for i in data_stats:
        unique_percentage = i['statistics']['unique_ratio'] * 100
        null_percentage = ((i['statistics']['null_count']) / (i['statistics']['sample_size'])) * 100
        dict1["column_name"].append(i["column_name"]),
        dict1["data_type"].append(i["data_type"]),
        dict1["% of unique"].append(unique_percentage),
        dict1["% of null"].append(null_percentage),
        if i['data_type'] == "int":
            dict1["num_zeros"].append(i['statistics']['num_zeros']),
            dict1["min"].append(i['statistics']['min']),
            dict1["max"].append(i['statistics']['max']),
            dict1["mode"].append(i['statistics']['mode']),
            dict1["median"].append(i['statistics']['median']),
            dict1["sum"].append(i['statistics']['sum']),
            dict1["mean"].append(i['statistics']['mean']),
            dict1["variance"].append(i['statistics']['variance']),
            dict1["stddev"].append(i['statistics']['stddev']),
            dict1["skewness"].append(i['statistics']['skewness'])
        else:
            dict1["num_zeros"].append(None),
            dict1["min"].append(None),
            dict1["max"].append(None),
            dict1["mode"].append(None),
            dict1["median"].append(None),
            dict1["sum"].append(None),
            dict1["mean"].append(None),
            dict1["variance"].append(None),
            dict1["stddev"].append(None),
            dict1["skewness"].append(None)
    stats_df = pd.DataFrame.from_dict(dict1)
    return stats_df
