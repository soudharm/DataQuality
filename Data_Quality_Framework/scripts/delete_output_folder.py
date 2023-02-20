import datetime as DT
import os
import shutil


output_folder_path = r"C:\Users\soupatil\Videos\DQF\Data_Quality_Framework_Tool\DQF\Projects\Azure\DQ_output"
today = DT.date.today()
week_ago = today - DT.timedelta(days=7)
path = output_folder_path + str(week_ago)
# lst_files = os.listdir(path)
# remove 7 days older folder
shutil.rmtree(path)
