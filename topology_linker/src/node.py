from typing import List

class Node:
    def __init__(self, object_name: str = 'root', object_description: str ='root',
                 children: list = None, parent: object = None):
        """
        :type children: List[Node]
        :type parent: Node

        """
        self.children = list() if children is None else children
        self.parent = parent
        self.object_name = object_name
        self.object_description = object_description
        self.object_id = None

    def get_children_as_dict(self):
        dict_out = dict()
        for child in self.children:
            if child.object_description not in dict_out:
                dict_out[child.object_description] = []
            dict_out[child.object_description].append(child)

        return dict_out

    def __str__(self):
        out = ""
        rep = f"{self.object_name} - {self.object_description}"
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