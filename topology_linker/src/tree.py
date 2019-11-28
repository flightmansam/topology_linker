VERBOSE_MODE = True

from node import Node

lvbc = Node("LVBC")

scotts = Node("Scotts", "Regulator")

ch = [
    Node("M1845/1", "Outlet"),
    Node("M1845A/P","Outlet"),
    Node("N134/P"  ,"Outlet"),
    Node("M1846/1" ,"Outlet"),
    Node("N599/P"  ,"Outlet"),
    Node("M1846/2" ,"Outlet"),
    Node("M1848/1" ,"Outlet"),
    Node("M1848/2" ,"Outlet"),
    Node("GOLFC/1" ,"Outlet"),
    Node("M1848/3" ,"Outlet")
]

scotts.addNode(ch)

lat166 = Node("OT L166", "Lateral Regulator")

lat_children = [
 Node("M1846A/1", "Outlet"),
 Node("M1868/1 ", "Outlet"),
 Node("M1870/1 ", "Outlet"),
 Node("M1876/1 ", "Outlet"),
 Node("M1874/P ", "Outlet"),
 Node("M1820/1 ", "Outlet"),
 Node("M1825B/P", "Outlet"),
 Node("M1825/2 ", "Outlet"),
 Node("M853/P", "Outlet")
]

lat_children[0].addNode(Node("TEST", "TEST_DESC"))
print(len(lat_children))

lat166.addNode(lat_children)
scotts.addNode(lat166)
lvbc.addNode(scotts)
# scotts.addNode(Node("TEST", "TEST_DESC"))

print(lvbc)