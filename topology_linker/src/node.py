from typing import List
from tree import VERBOSE_MODE


class Node:
    __display_middle = '├───'
    __display_middle_bus = '╞//═'
    __display_last_bus = '╘//═'
    __display_last = '└───'
    __display_parent_middle = '     '
    __display_parent_last = '│    '

    def __init__(self, object_name: str = 'root', object_description: str ='root',
                 children: List[__name__.Node] = [], parent: __name__.Node = None):
        """
        :type parent: Node

        """
        self.children = children
        self.parent = parent
        self.object_name = object_name
        self.object_description = object_description

    @property
    def get_children(self):
        return self.children

    def get_children_as_dict(self):
        dict_out = dict()
        for child in self.children:
            dict_out[child.object_name] = child
        return dict_out

    if VERBOSE_MODE:
        def __str__(self):
            out = ''
            if self.parent is None:
                out += self.object_name + "\n"
            if len(self.children) > 0:
                padding = self.get_depth()
                if padding == 1: out += '│    '
                elif padding > 1: out += '│    ' + (padding - 1) * '     '

                for i, v in enumerate(self.children):
                    if i == len(self.children) - 1:
                        out += f"└─── {self.object_name}\n"
                    else:
                        out += f"├─── {self.object_name}\n"
            else:

                return

    else:
        def __str__(self):
            return ""

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