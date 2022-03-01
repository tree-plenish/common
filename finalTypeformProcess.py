import pandas as pd
from gdrive import GoogleDriveOperations
import os
from datetime import datetime
import tech_team_database.dependencies.UploadDataToDatabase as ud_file
import numpy as np

#########
# INPUT #
#########
create_date = datetime.now()
end_date = datetime(2022, 3, 31)  # Set End Date of Upload
NEW_FOLDER = False # Tag to create a new folder for easier automation (currently doesn't work... unshareable)
ROOT_GDRIVE_ID = "1cK6ss2Jf_pcZNYPBNNwNHtJzHqQmev3_"  # Set ID of root GDRIVE folder for all outputs
parentID = "1PmOXHwSLsXfyGdwmRphF8pjX12r4612I"  # Set parentID of GDRIVE folder (used if not NEW_FOLDER)

#############
# END INPUT #
#############

# Getting school & event tabletable
ud = ud_file.UploadData(logfile="finalTypeformProcess.log")
school = ud.SQL.getTable("school")
event = ud.SQL.getTable("event")
df = school.merge(event, on="schoolid")

# Cleaning event dates & formatting
clean_date = [df["date"].iloc[i][-5:] for i in range(len(df))]
clean_dttm = [datetime(2022, int(clean_date[i][:2]), int(clean_date[i][-2:])) if clean_date[i] != "NULL" else datetime(3000,1,1) for i in range(len(clean_date))]
df["date"] = clean_dttm

# Filtering by dttm
df_select = df[df["date"] <= end_date]

# Looping through files & checking for names in df_select to load filename for upload
DIR = "/home/justinmiller/tpMain/tree_requests/"
TEMP_DF_DIR = "/home/justinmiller/tpMain/temp_df_upload/"
files = os.listdir(DIR)

files_to_upload = []
for name in df_select["name"].values:
    name_files = []
    for file in files:
        if name in file:
            name_files.append(file)

    for file_to_append in name_files:
        files_to_upload.append(file_to_append)

unique_schools_to_upload = [" ".join(file.split(" ")[:-1]) for file in files_to_upload]
unique_schools_to_upload = np.unique(unique_schools_to_upload)

# Getting files with data in them
files_with_data = []
for file in files_to_upload:
    data = pd.read_csv(DIR + file)
    if len(data) > 0:
        # Duplicating, stripping and deleting files with data, saving in temp_df folder
        files_with_data.append(file) 
        data = data.drop(columns=data.columns[-4:])
        data = data.drop("Unnamed: 0", axis=1)
        data.to_csv(TEMP_DF_DIR + file, index=False)
        
unique_schools_with_data = [" ".join(file.split(" ")[:-1]) for file in files_with_data]
unique_schools_with_data = np.unique(unique_schools_with_data)
unique_schools_without_data = np.setdiff1d(unique_schools_to_upload, unique_schools_with_data)

# Naming conventions
create_date_str = create_date.strftime('%m%d%y')
end_date_str = end_date.strftime('%m%d%y')
report_name = f"{create_date_str}-{end_date_str}_report"
report_file = f"./reports/{report_name}.txt"

# Making summary report.txt
uswd = "\n    ".join([u for u in unique_schools_without_data])
content = f"""
All typeform data for events up to {end_date_str}.\n
{len(df_select)} total events originally planned before then.\n
{len(files_to_upload)} total typeform downloads (including duplicates/empties).\n
{len(unique_schools_to_upload)} total schools with typeforms created.\n
{len(unique_schools_with_data)} total schools with typeform submissions. These csvs have been uploaded. \n
Schools with empty typeforms:\n
    {uswd}
"""
# Local report creation
with open(report_file, "w") as f:
    f.write(content)


##################
# GDRIVE Uploads #
##################
g_handle = GoogleDriveOperations.GDrive()  # Google Drive handle

# Folder ID-ing
if NEW_FOLDER:
    parentID = g_handle.createFolder(report_name, shared=True, folderID=ROOT_GDRIVE_ID)
else:
    parentID = parentID  # Reminder... manually input at top of file

# Uploading report summary & files
g_handle.uploadShareableFile(report_file, report_name, parentID=parentID)
csv_links = []
upload_names = []
for file in files_with_data:
    upload_name = " ".join(file.split(" ")[:-1])
    link = g_handle.uploadShareableFile(TEMP_DF_DIR + file, upload_name, parentID=parentID)
    csv_links.append(link)
    upload_names.append(upload_name)

# Also generating list of links
ll_name = f"{end_date_str}_link_list"
ll_file = f"./reports/{ll_name}.txt"
content = "\n".join([f"{upload_names[i]}: {csv_links[i]}" for i in range(len(upload_names))])

# Local LL creation & gdrive upload
with open(ll_file, "w") as f:
    f.write(content)
g_handle.uploadShareableFile(ll_file, ll_name, parentID=parentID)
