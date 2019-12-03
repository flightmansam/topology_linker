from typing import List
import pandas as pd
from topology_linker.res.FGinvestigation.fginvestigation.extraction import get_data_ordb
class Node:
    def __init__(self, object_name: str = 'root',
                 object_description: str ='root',
                 object_id: str = 'root',
                 children: list = None, parent: object = None):
        """
        :type children: List[Node]
        :type parent: Node

        """
        self.children = list() if children is None else children
        self.parent = parent
        self.object_name = object_name
        self.object_description = object_description
        #self.object_id = str(object_id)
        try:
            self.object_id = get_data_ordb(f"Select OBJECT_NO From OBJECT WHERE OBJECT_NAME = '{object_name}'").iloc[0,0]
        except:
            self.object_id = str(object_name)

    def get_children_as_dict(self):
        dict_out = dict()
        for child in self.children:
            if child.object_description not in dict_out:
                dict_out[child.object_description] = []
            dict_out[child.object_description].append(child)

        return dict_out

    def __str__(self):
        out = ""
        rep = f"{self.object_name} - {self.object_description} ({self.object_id})"
        out += rep+"\n"

        for i, v in enumerate(self.children):
            padding_str = '│    '
            padding_str_blank = '     '
            blank_padding = 0
            chld = v.__str__()
            padding = self.at_end_array()
            padding = [padding_str_blank if i is True else padding_str for i in padding]
            out += "".join(padding)
            # at_end = self.parent.children.index(self) == len(self.parent.children) - 1 \
            #     if self.parent is not None else False
            # if at_end:
            #     blank_padding = padding
            #     padding = 0
            # out += padding * padding_str + blank_padding * padding_str_blank
            if i == len(self.children) - 1:
                out += '└─── '
            else:
                out += '├─── '
            out += chld
        return out

    def get_depth(self):
        if self.parent is None:
            return 0
        else: return 1 + self.parent.get_depth()

    def at_end_array(self):
        """Returns an array of booleans as to whether the parent at each level
         is at the end of the chain or not from thr root at index 0 to the node at level n at index n"""
        if self.parent is None:
            #root is next
            return []
        else:
            return self.parent.at_end_array() + [self.parent.children.index(self) == len(self.parent.children) - 1]


    def addNode(self, childNode):
        if isinstance(childNode, list):
            for i in childNode:
                self.children.append(i)
                i.parent = self
        else:
            self.children.append(childNode)
            childNode.parent = self

    def as_df(self):
        df = {
            "OBJECT_ID":[],
            "LINK_OBJECT_ID":[],
            "LINK_DESCRIPTION":[]
        }
        df = pd.DataFrame(df)

        for i, v in enumerate(self.children):
            if len(v.children) > 0:
                df = pd.concat([df, v.as_df()])
            df = df.append({
                "OBJECT_ID": str(self.object_id),
                "LINK_OBJECT_ID": str(v.object_id),
                "LINK_DESCRIPTION": str(v.object_description)
            },
            ignore_index=True)

        return df




