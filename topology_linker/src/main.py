from typing import Union

from node import Node
from network_csv_parser import parse
from topology_linker.res.FGinvestigation.fginvestigation.extraction import get_data_ordb
import pandas as pd

file_path = "../res/LVBC_network_structure.csv"

__DS_METER = 'D/S Meter'
__US_REG = 'Regulator'
__DS_ESC = 'D/S Escape'
__DS_OFFTAKE = 'D/S Offtake'
__DS_SV = 'D/S Scour Valve'


main_channel, lateral_descriptions = parse(file_path)

headings = []



#FIND THE HEADINGS IN THE MAIN CHANNEL (REGULATORS, ESCAPES, OTHERS)
for index, values in main_channel.iterrows():
    #pattern for NEW REG in channel [NaN, Regulator name, Dont care]
    if pd.isna(values[:2]).tolist() == [True, False]:
        headings.append(values["Regulator Number"].strip())

headings = tuple(headings)

def query(headings: tuple) -> pd.DataFrame:
    # query = ("SELECT OBJECT_NO, ASSET_CODE, SITE_NAME, VALUE"
    #         f" FROM V_D_SITE_DETAILS WHERE SITE_NAME IN "
    #         f" {headings}"
    #         f" AND ATTRIBUTE = 'Channel Name'")

    query = (
        f"Select OBJECT.OBJECT_NO, ATTRIBUTE_VALUE, OBJECT_NAME "
        f" From OBJECT_ATTR_VALUE INNER JOIN OBJECT"
        f" ON OBJECT_ATTR_VALUE.OBJECT_NO = OBJECT.OBJECT_NO"
        f" WHERE OBJECT_NAME IN {headings}"
        f" AND ATTRIBUTE_TYPE = '117'")

    query = get_data_ordb(query)

    # rename columns
    cols = query.columns.tolist()
    cols[1] = 'ASSET_CODE'
    cols[2] = 'SITE_NAME'
    query.columns = pd.Index(cols)

    return query

headings = query(headings)

def build_lateral(lateral_key:str) -> Node:
    children = []
    if lateral_key in lateral_descriptions:
        desc = lateral_descriptions[lateral_key]
    else:
        return None
    for index, values in desc.iterrows():
        if pd.notna(values[:2]).tolist() == [True, True]:
            offtake = Node(values[1].strip(), __DS_OFFTAKE)
        elif pd.isna(values).tolist() == [True, False, True]:
            # this is either a branch, SV, or ESC
            heading = values[1].strip()
            if heading in lateral_descriptions:
                # branch if heading in lateral descriptions
                offtake.addNode(build_lateral(heading))
            else:
                a_c = query("('"+heading+"')")['ASSET_CODE'][0]
                if a_c.startswith('ES-'):
                    offtake.addNode((Node(heading, __DS_ESC)))
                elif a_c.startswith('SV-'):
                    # else SV
                   offtake.addNode(Node(heading, __DS_SV))
                else:
                    print(f"CODE: {a_c} not recognised")

        if pd.notna(values[2]):
            offtake.addNode(Node(values[2].strip(), __DS_METER))

    return offtake

#Start building the tree data structure
root = Node('LVBC', 'Main Channel', object_id='214362')
current_reg = None

for index, values in main_channel.iterrows():
    #PATTERN FOR REGULATOR: in headings, asset code starts with RG-
    #PATTERN FOR ESCAPE: in headings, asset code starts with ES-
    if pd.isna(values[:2]).tolist() == [True, False]:
        values["Regulator Number"] = values["Regulator Number"].strip()
        df = headings.loc[headings['SITE_NAME'] == values["Regulator Number"]]
        asset_code: pd.Series = df['ASSET_CODE'].values
        if len(asset_code) > 0:
            if asset_code[0].startswith('RG-'):
                # new regulator
                if current_reg is not None:
                    root.addNode(current_reg)
                current_reg = Node(values["Regulator Number"], __US_REG)
            elif asset_code[0].startswith('ES-'):
                # escape
                current_reg.addNode(Node(values["Regulator Number"], __DS_ESC))
            else:
                print(f"CODE: {asset_code[0]} not recognised")

    elif pd.isna(values[:2]).tolist() == [False, True]:
        # this is a call to branch to a lateral
        # build the lateral node and the add to the current reg
        current_reg.addNode(build_lateral(values['Branch']))
        print(values['Branch'])

    if pd.notna(values[2]):
        current_reg.addNode(Node(values['Outlet'].strip(), __DS_METER))

root.addNode(current_reg) # add the last regulator in the file

print(root)
print(root.as_df().to_string())




