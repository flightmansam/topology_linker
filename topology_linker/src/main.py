"""Utility that can parse an input branch summary csv (that has been modified and standardised to include lateral definitions).
It builds the topology of the branch and generates a linkage table that describes relationships between regulators, meters, offtakes, escapes and scour valves.
The linkage is built as a pandas dataframe that can be uploaded to a database (currently not implemented)

The standardisation of the input csv is described in the src readme.md"""

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

from constants import file_path, DS_METER, US_REG, DS_ESC, DS_OFFTAKE, DS_SV
from node import Node
from utils import parse, query
import pandas as pd

main_channel, lateral_descriptions = parse(file_path)

headings = []

#FIND THE HEADINGS IN THE MAIN CHANNEL (REGULATORS, ESCAPES, OTHERS)
for index, values in main_channel.iterrows():
    #pattern for NEW REG in channel [NaN, Regulator name, Dont care]
    if pd.isna(values[:2]).tolist() == [True, False]:
        headings.append(values["Regulator Number"].strip())

headings = tuple(headings)

headings = query(headings)

def build_lateral(lateral_key:str) -> Node:
    if lateral_key in lateral_descriptions:
        desc = lateral_descriptions[lateral_key]
    else:
        return None
    for index, values in desc.iterrows():
        if pd.notna(values[:2]).tolist() == [True, True]:
            offtake = Node(values[1].strip(), DS_OFFTAKE)
        elif pd.isna(values).tolist() == [True, False, True]:
            # this is either a branch, SV, or ESC
            heading = values[1].strip()
            if heading in lateral_descriptions:
                # branch if heading in lateral descriptions
                offtake.addNode(build_lateral(heading))
            else:
                a_c = query(heading)['ASSET_CODE'][0]
                if a_c.startswith('ES-'):
                    offtake.addNode((Node(heading, DS_ESC)))
                elif a_c.startswith('SV-'):
                    # else SV
                   offtake.addNode(Node(heading, DS_SV))
                else:
                    print(f"CODE: {a_c} not recognised")

        if pd.notna(values[2]):
            offtake.addNode(Node(values[2].strip(), DS_METER))

    return offtake

#Start building the tree data structure
root = Node('LVBC', 'Main Channel', object_no='214362')
previous_reg = root
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
                    previous_reg.addNode(current_reg)
                    previous_reg = current_reg
                current_reg = Node(values["Regulator Number"], US_REG)
            elif asset_code[0].startswith('ES-'):
                # escape
                current_reg.addNode(Node(values["Regulator Number"], DS_ESC))
            else:
                print(f"CODE: {asset_code[0]} not recognised")

    elif pd.isna(values[:2]).tolist() == [False, True]:
        # this is a call to branch to a lateral
        # build the lateral node and the add to the current reg
        current_reg.addNode(build_lateral(values['Branch']))
        print(values['Branch'])

    if pd.notna(values[2]):
        current_reg.addNode(Node(values['Outlet'].strip(), DS_METER))

previous_reg.addNode(current_reg) # add the last regulator in the file

print(root)
df = root.as_df()
print(df.to_string())
#df.to_csv("../out/LINKED.csv")

#this is where you can send to the database this linkage df


