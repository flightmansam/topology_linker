"""Utility that can parse an input branch summary csv (that has been modified and standardised to include lateral definitions).
It builds the topology of the branch and generates a linkage table that describes relationships between regulators, meters, offtakes, escapes and scour valves.
The linkage is built as a pandas dataframe that can be uploaded to a database (currently not implemented)

The standardisation of the input csv is described in the src readme.md"""

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

from typing import Union
from topology_linker.src.constants import file_path, DS_METER, US_REG, DS_ESC, DS_OFFTAKE, DS_SV
from node import Node
from utils import parse, query
import pandas as pd

#parse() will break the document up into its respective areas - see readme.md
main_channel, lateral_descriptions = parse(file_path)

### DEFINE THE NAME OF THE BRANCH YOU ARE PARSING HERE
# Start building the tree data structure
root = Node('LVBC')
root.object_no = '214362' #still unsure of what object no to put here
root.object_description = 'Main Channel'
### DEFINE THE NAME OF THE BRANCH YOU ARE PARSING HERE

headings = []
# FIND THE HEADINGS IN THE MAIN CHANNEL (REGULATORS, ESCAPES, OTHERS)
for index, values in main_channel.iterrows():
    # pattern for NEW REG in channel [NaN, Regulator name, don't care]
    if pd.isna(values[:2]).tolist() == [True, False]:
        headings.append(values["Regulator Number"].strip())

headings = tuple(headings)
headings = query(headings) #find the information about headings (ASSET_CODE, DESCRIPTION ETC..)

def build_lateral(lateral_key: str) -> Union[Node, None]:
    """Looks at the lateral key (name of the lateral in the network topology file)
    and pulls together information to build a node upon that lateral. Can call upon it self for
    sub-laterals.
    @:returns None if there is no information about that lateral key."""
    if lateral_key in lateral_descriptions:
        desc = lateral_descriptions[lateral_key]
    else:
        return None
    for index, values in desc.iterrows():
        # pattern for NEW Lateral in channel [NaN, NaN, don't care]
        if pd.notna(values[:2]).tolist() == [True, True]:
            lateral = Node(values[1].strip(), DS_OFFTAKE)
        # pattern for object in that lateral [NaN, object name, NaN]
        elif pd.isna(values).tolist() == [True, False, True]:
            # this is either a branch, SV, or ESC
            heading = values[1].strip()
            if heading in lateral_descriptions:
                # branch if heading in lateral descriptions
                lateral.addNode(build_lateral(heading))
            else:
                a_c = query(heading)['ASSET_CODE'][0]
                if a_c.startswith('ES-'):
                    lateral.addNode((Node(heading, DS_ESC)))
                elif a_c.startswith('SV-'):
                    # else SV
                    lateral.addNode(Node(heading, DS_SV))
                else:
                    print(f"CODE: {a_c} not recognised")

        if pd.notna(values[2]):
            lateral.addNode(Node(values[2].strip(), DS_METER))
    return lateral

previous_reg = root
current_reg = None
for index, values in main_channel.iterrows():
    # PATTERN FOR REGULATOR: in headings, asset code starts with RG-
    # PATTERN FOR ESCAPE: in headings, asset code starts with ES-
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

    if pd.notna(values[2]):
        current_reg.addNode(Node(values['Outlet'].strip(), DS_METER))

previous_reg.addNode(current_reg)  # add the last regulator in the file

print(root)
df = root.as_df()
print(df.to_string())
# df.to_csv("../out/LINKED.csv")

# this is where you can send to the database this linkage df
