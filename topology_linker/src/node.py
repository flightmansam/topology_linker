"""Lightweight and specific implementation of a tree data structure and helper functions for visualisation and exportation."""

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

from constants import OBJECT, LINK_OBJECT, LINK_DESCRIPTION, POSITION, DS_METER
from typing import List, Union
import pandas as pd
from topology_linker.res.FGinvestigation.fginvestigation.extraction import get_data_ordb

class Node:
    def __init__(self, object_name: str = 'root',
                 object_description: str ='root',
                 object_no: str = 'root',
                 children: list = None, parent: object = None):
        """
        :type children: List[Node]
        :type parent: Node
        """
        self.children = list() if children is None else children
        self.parent = parent
        self.object_name = object_name
        self.object_description = object_description
        self.index = None
        #self.object_id = str(object_id)
        try:
            self.object_no = get_data_ordb(f"Select OBJECT_NO From OBJECT WHERE OBJECT_NAME = '{object_name}'").iloc[0, 0] if object_name != 'root' else object_no
        except:
            self.object_no = str(object_name)

    def get_children_as_dict(self):
        """Currently not used but could be helpful?"""
        dict_out = dict()
        for child in self.children:
            if child.object_description not in dict_out:
                dict_out[child.object_description] = []
            dict_out[child.object_description].append(child)

        return dict_out

    def __str__(self):
        out = ""
        rep = f"{self.object_name} - {self.object_description} ({self.object_no})"
        out += rep+"\n"

        for i, v in enumerate(self.children):
            padding_str = '│    '
            padding_str_blank = '     '
            chld = v.__str__()
            padding = self.__at_end_array()
            padding = [padding_str_blank if i is True else padding_str for i in padding]
            out += "".join(padding)
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

    def __at_end_array(self):
        """Private method. Returns an array of booleans as to whether the parent at each level
         is at the end of the chain or not from thr root at index 0 to the node at level n at index n"""
        if self.parent is None:
            #root is next
            return []
        else:
            return self.parent.__at_end_array() + [self.parent.children.index(self) == len(self.parent.children) - 1]


    def addNode(self, childNode):
        if isinstance(childNode, list):
            for i in childNode:
                self.children.append(i)
                i.index = len(self.children) - 1
                i.parent = self

        else:
            self.children.append(childNode)
            childNode.index = len(self.children) - 1
            childNode.parent = self

    def as_df(self):
        """Helper function for easy exporting of the tree data structure to a link table"""
        df = {
            OBJECT:[],
            LINK_OBJECT:[],
            LINK_DESCRIPTION:[],
            POSITION:[]
        }
        df = pd.DataFrame(df)

        for i, child in enumerate(self.children):
            if len(child.children) > 0:
                df = pd.concat([df, child.as_df()])
            df = df.append({
                OBJECT: str(self.object_no),
                LINK_OBJECT: str(child.object_no),
                LINK_DESCRIPTION: str(child.object_description),
                POSITION: str(child.index)
            },
            ignore_index=True)

        return df

    def get_last_child(self):
        if len(self.children) > 0:
            return self.children[-1].get_last_child()
        return self

    def get_all_of_desc(self, desc: Union[str, list] = DS_METER):
        """
        Gets all of the objects of "desc" from the current node (and all of the children of this node)
        :return: array of nodes (in no particular order)
        """
        if isinstance(desc, str):
            desc = [desc]
        out = []
        for child in self.children:
            if child.object_description in desc:
                out.append(child)
            out += child.get_all_of_desc(desc)
        return out




