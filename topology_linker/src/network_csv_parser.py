import pandas as pd
from topology_linker.res.FGinvestigation.fginvestigation.extraction import get_data_ordb

def parse(file_path:str):
    with open(file_path, 'r', encoding="UTF-8") as fh:
        start_index = 0
        for line in fh.readlines():
            if line.__contains__("Regulator Number"):
                break
            start_index += 1

    csv_df = pd.read_csv(file_path, skiprows=start_index, na_values=' ', skipfooter=3, usecols=['Branch','Regulator Number ','Outlet '])
    csv_df.dropna(axis=0, how='all', inplace=True)
    csv_df.columns = [col.strip() for col in csv_df.columns]

    lateral_descriptions = {}

    #collect lateral descriptions
    branch = None
    main_channel_last_index = None
    for index, row in csv_df.iterrows():
        if pd.isna(row[:4]).tolist() == [False, False, False]:
            #this is marks the start of a description (and end of the last one)
            if main_channel_last_index is None:
                main_channel_last_index = index - 1
            if branch is not None:
                lateral_descriptions[branch] = pd.DataFrame(lateral_descriptions[branch])
            branch = row["Branch"]

        if branch is not None:
            if branch not in lateral_descriptions:
                lateral_descriptions[branch] = []
            lateral_descriptions[branch].append(row)
    lateral_descriptions[branch] = pd.DataFrame(lateral_descriptions[branch]) #covert last branch to df

    print(lateral_descriptions)

    for index, row in csv_df.iloc[:main_channel_last_index].iterrows():
        print(row)


    #for index, row in csv_df[:main_channel_last_index].iterrows():


    return