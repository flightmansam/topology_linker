from typing import Tuple
import pandas as pd
from topology_linker.res.FGinvestigation.fginvestigation.extraction import get_data_ordb
from node import Node

def parse(file_path:str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    with open(file_path, 'r', encoding="UTF-8") as fh:
        start_index = 0
        for line in fh.readlines():
            if line.__contains__("Regulator Number"):
                break
            start_index += 1

    csv_df = pd.read_csv(file_path, skiprows=start_index, na_values=' ', skipfooter=3, usecols=['Branch','Regulator Number ','Outlet '], engine='python')
    csv_df.dropna(axis='index', how='all', inplace=True)
    csv_df.columns = [col.strip() for col in csv_df.columns]

    lateral_descriptions = {}
    main_channel = []
    #collect lateral descriptions
    branch = None
    main_channel_last_index = None
    for index, row in csv_df.iterrows():
        if pd.isna(row[:4]).tolist() == [False, False, False]:
            #this is marks the start of a description (and end of the last one)
            if main_channel_last_index is None:
                main_channel_last_index = index
            if branch is not None:
                lateral_descriptions[branch] = pd.DataFrame(lateral_descriptions[branch])
            branch = row["Branch"].strip()

        if main_channel_last_index is None:
            main_channel.append(row.tolist())

        if branch is not None:
            if branch not in lateral_descriptions:
                lateral_descriptions[branch] = []
            lateral_descriptions[branch].append(row)
    lateral_descriptions[branch] = pd.DataFrame(lateral_descriptions[branch]) #covert last branch to df
    main_channel = pd.DataFrame(main_channel, columns=row.keys())

    #print(lateral_descriptions)

    # for index, row in csv_df.iloc[:main_channel_last_index].iterrows():
    #     print(row)
    return main_channel, lateral_descriptions

def query(headings: tuple, by:str = 'OBJECT_NAME') -> pd.DataFrame:
    # query = ("SELECT OBJECT_NO, ASSET_CODE, SITE_NAME, VALUE"
    #         f" FROM V_D_SITE_DETAILS WHERE SITE_NAME IN "
    #         f" {headings}"
    #         f" AND ATTRIBUTE = 'Channel Name'")

    if isinstance(headings, str):
        #assuming if string only one item
        headings = "('"+headings+"')"
    if by == 'OBJECT_NO':
        by = 'OBJECT.OBJECT_NO'

    query = (
        f"Select OBJECT.OBJECT_NO, ATTRIBUTE_VALUE, OBJECT_NAME, ATTRIBUTE_TYPE"
        f" From OBJECT_ATTR_VALUE RIGHT JOIN OBJECT"
        f" ON OBJECT_ATTR_VALUE.OBJECT_NO = OBJECT.OBJECT_NO"
        f" WHERE {by} IN {headings}"
        f" AND (ATTRIBUTE_TYPE in ('117', '45') OR OBJECT_VALUE_NO IS NULL)")

    query = get_data_ordb(query)
    query = query.astype({"OBJECT_NO":str,"ATTRIBUTE_TYPE":str})
    query.replace(' ', pd.np.NaN, inplace=True)

    out = {
        'OBJECT_NO':[],
        'ASSET_CODE':[],
        'SITE_NAME':[]
    }

    vals = query["OBJECT_NAME"].unique()
    for site in vals.tolist():
        q = query.loc[query["OBJECT_NAME"] == site]
        if q["ATTRIBUTE_TYPE"].values.all() == 'None':
            # Q came from the RIGHT side of the SQL join only
            q["ATTRIBUTE_VALUE"] = q["OBJECT_NAME"]
            q["ATTRIBUTE_TYPE"] = '117'
        q = q.dropna()
        if len(q) == 2:
            q = q.loc[q["ATTRIBUTE_TYPE"] == '117']
        q = q.values.tolist()[0]
        out['OBJECT_NO'].append(q[0])
        out['ASSET_CODE'].append(q[1])
        out['SITE_NAME'].append(q[2])

    return pd.DataFrame(out)

def get_linked_ojects(object_A:str, object_B:str, encoding:str = "OBJECT_NO", source:pd.DataFrame = None):
    """
    Will return a topology of objects in a pool between object_A and object_B
    :param object_A: the identifier of the upstream object
    :param object_B: the identifier of the downstream object
    :param encoding: valid options 'OBJECT_NO', 'SITE_NAME', 'ASSET_CODE'
    :param source: if a source df is given it will be used as the link table (cols = OBJECT_NO, LINK_OBJECT_NO, LINK_DESCRIPTION)
    :return: Tuple(Node, List): a node representing that topology and a list of object numbers in that pool (object_A and object_B)
    """

    objects = [object_A]


    def get(object, column="OBJECT_NO"):
        if source is None:
            #get all children of object from __LINK_TABLE via SQL
            link_obj:pd.DataFrame = get_data_ordb("")
        else:
            #get all children of object from df
            link_obj:pd.DataFrame = source.loc[source[column] == str(object)]
        return link_obj.reset_index(drop=True)

    def explore(upstream_reg_id, _object_B):
        children = []

        up_children = get(upstream_reg_id)

        for index, link in up_children.iterrows():
            link_object_no = link['LINK_OBJECT_NO']
            objects.append(link_object_no)
            link_description = link['LINK_DESCRIPTION']
            link_asset_code = query(str(link_object_no), by="OBJECT_NO")['SITE_NAME'][0]
            child = Node()
            child.object_no = link_object_no
            child.object_description = link_description
            child.object_name = link_asset_code
            if link_object_no != _object_B:
                expl = explore(link_object_no, _object_B)
                child.addNode(expl)
            children.append(child)

        return children

    object_A = query(object_A, by=encoding)
    A_object_no = object_A["OBJECT_NO"][0]

    def build(_object_B:str):
        _object_B = query(_object_B, by=encoding)
        B_object_no = _object_B["OBJECT_NO"][0]

        g = get(B_object_no, "LINK_OBJECT_NO").head(1)
        up_object_no = g["OBJECT_NO"][g.first_valid_index()]
        up_asset_code = query(str(up_object_no), by="OBJECT_NO")['SITE_NAME'][0]
        up_description = g["LINK_DESCRIPTION"][0]

        upstream = Node()
        upstream.object_no = up_object_no
        upstream.object_description = up_description
        upstream.object_name = up_asset_code
        upstream.addNode(explore(up_object_no, B_object_no))

        if up_object_no == A_object_no:
            return upstream
        else:
            next_reg = build(up_object_no)

            # i = list(filter(lambda child: child.object_no == up_object_no, next_reg.children))[0]
            # print(i.object_no)
            # next_reg.children.remove(i)

            next_reg.get_last_child().addNode(upstream.children)
            return next_reg


    return build(object_B), objects