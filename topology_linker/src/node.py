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
            padding = self.get_depth()
            padding_str = '│    '
            if i == len(self.children) - 1:
                out += padding * padding_str + '└─── ' + v.__str__()
            else:
                out += padding * padding_str + '├─── ' + v.__str__()
        return out

    def get_depth(self):
        if self.parent is None:
            return 0
        else: return 1 + self.parent.get_depth()

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




