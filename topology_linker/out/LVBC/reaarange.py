import glob
import pandas as pd
from fginvestigation.extraction import get_data_sql, export_sqlserver


def get_offset() -> int:
    query = (
        "SELECT MAX(TAG_ID) as MAX"
        " from TAGS"
    )
    df = get_data_sql(query)

    return int(df.MAX.values[0])



name = 'L258-2*.csv'
name = 'LVBC*.csv'
csvs = glob.glob(name)
csvs = [c for c in csvs if '*' not in c]
print(csvs)

division_object = 'placeholder'

spreadsheet = []
database = []
offset = get_offset() #max number in TAGS table
cols = ["TAG_ID", "TAG_DESC", "TAG_UNITS", "OBJECT_NO"]
tagd = ['System_Efficiency', 'Diverted', 'Delivered', 'Evaporative_loss', 'Rainfall', 'Seepage_loss', 'Unaccounted_loss', 'Calculation_time']
tagu = ['percent', 'ML', 'ML','ML','ML','ML','ML', 'time']
tagi = [i for i in range(offset + 1, offset+len(tagd)+1)]
obj = [division_object]*len(tagd)
data = {}
for c, item in zip(cols, [tagi, tagd, tagu, obj]):
    data[c] = item
tags = pd.DataFrame(data)

for csv in csvs:
    # EVENT_TIME #TAG_ID #TAG_DESC

    lines = open(csv, 'r').readlines()
    reg = lines[0].split(') and')[0].split('(')[1].strip()

    time = lines[1].split('to')[0].split('period')[1].strip()
    time = pd.to_datetime(time, format="%Y-%m-%d %H:%M")
    summary = pd.read_csv(csv, skiprows=3, nrows=6).transpose().reset_index()
    summary.columns = ["System_Efficiency", "Diverted", "Delivered", "Evaporative_loss", "Rainfall", "Seepage_loss",
                       "Unaccounted_loss"]
    summary.drop(0, inplace=True)
    summary.loc[1, 'Diverted'], summary.loc[1, 'Out'] = summary.loc[1, 'Diverted'].strip().split(' ')
    summary.loc[1, 'EVENT_TIME'] = time
    summary.set_index('EVENT_TIME', inplace=True)
    spreadsheet.append(summary)

    df = summary[["System_Efficiency", "Diverted", "Delivered", "Evaporative_loss", "Rainfall", "Seepage_loss",
                  "Unaccounted_loss"]].transpose().reset_index()
    df.columns = ["TAG_DESC", "EVENT_VALUE"]
    df.loc[:, "TAG_ID"] = None

    start, end = [index for index, line in enumerate(lines) if
                  'RTU_totaliser' in line or ', Total,' in line]
    outlets = pd.read_csv(csv, skiprows=start, nrows=end - start - 1)
    outlets.columns = ["Outlet", "object_id", "RTU_totaliser", "Flow_integral", "Manual_reading"]+outlets.columns[5:].to_list()

    if ~tags.OBJECT_NO.isin(outlets.object_id).any():
        #ADD TAG_IDS to TAGS if not in TAGS
        for o in outlets.object_id:
            for tagd in ["RTU_totaliser", "Flow_integral", "Manual_reading"]:
                tags.loc[len(tags.index)+1, :] = [max(tags.TAG_ID) +1, tagd, 'ML', o]

    for o in outlets.object_id:
        o = outlets.loc[outlets.object_id == o]
        for tagd in ["RTU_totaliser", "Flow_integral", "Manual_reading"]:
            ev = o[tagd].values[0]
            tagi = tags.loc[(tags.OBJECT_NO == int(o.object_id)) & (tags.TAG_DESC == tagd), "TAG_ID"].values[0]
            df.loc[len(df.index)+1, ["TAG_DESC","EVENT_VALUE", "TAG_ID"]] = [tagd, ev, tagi]

    runtime = [line.split('collection:')[1].strip() for line in lines if 'time of data collection:' in line][0]
    runtime = pd.to_datetime(runtime, format="%Y-%m-%d %H:%M")
    df.loc[len(df.index)+1, ["TAG_DESC","EVENT_VALUE"]] = ["Calculation_time", runtime]

    start = [index for index, line in enumerate(lines) if 'meters not read:' in line][0]
    not_read = 'None' if len(lines) == start + 1 else lines[start + 1:]
    #df.loc[len(df.index)+1, ["TAG_DESC","EVENT_VALUE"]] = ["Meters_not_read", not_read]

    tagi = df.loc[df.TAG_ID.isna()]
    for tagd in tagi.TAG_DESC:
        df.loc[df.TAG_DESC==tagd, "TAG_ID"] = tags.loc[(tags.OBJECT_NO == division_object) & (tags.TAG_DESC == tagd), "TAG_ID"].values[0]
    tagi.to_string()

    df.loc[:, "EVENT_TIME"] = time
    database.append(df)

#update the regulator object_no
tags.loc[tags.OBJECT_NO==division_object, "OBJECT_NO"] = reg
division_object = reg



objects =  pd.DataFrame(tags.OBJECT_NO.unique())
objects.loc[:, "DATE_ADDED"] = pd.datetime(year=2019, month=12, day=1)
objects.loc[:, "DATE_REMOVED"] = None
objects.rename(columns={"0":"OBJECT_NO"}, inplace=True)

spreadsheet = pd.concat(spreadsheet).sort_index()

database = pd.concat((database))

database.to_csv("EVENTS.csv", index=False)
export_sqlserver(database, "EVENTS", if_exists='append')
tags.to_csv("TAGS.csv", index=False)
export_sqlserver(tags, "TAGS", if_exists='append')
objects.to_csv("OBJECTS.csv", index=False)
export_sqlserver(objects, "OBJECTS", if_exists='fail')

print(spreadsheet.to_string())
# spreadsheet.to_csv(name)
