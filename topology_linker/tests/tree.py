VERBOSE_MODE = True

from node import Node

lvbc = Node("LVBC")

scotts = Node("SCOTTS RD REGULATOR", "Regulator", object_id=1)

ch = [
    Node("M1845/1", "Outlet", object_id=2),
    Node("M1845A/P","Outlet", object_id=3),
    Node("N134/P"  ,"Outlet", object_id=4),
    Node("M1846/1" ,"Outlet", object_id=5),
    Node("N599/P"  ,"Outlet", object_id=6),
    Node("M1846/2" ,"Outlet", object_id=7),
    Node("M1848/1" ,"Outlet", object_id=8),
    Node("M1848/2" ,"Outlet", object_id=9),
    Node("GOLFC/1" ,"Outlet", object_id=10),
    Node("M1848/3" ,"Outlet", object_id=11)
]

scotts.addNode(ch)

lat166 = Node("OT L166", "D/S Regulator", object_id=23)

lat_children = [
 Node("M1846A/1", "Outlet", object_id=12),
 Node("M1868/1", "Outlet", object_id=13),
 Node("M1870/1", "Outlet", object_id=14),
 Node("M1876/1", "Outlet", object_id=15),
 Node("M1874/P", "Outlet", object_id=16),
 Node("M1820/1", "Outlet", object_id=17),
 Node("M1825B/P", "Outlet", object_id=18),
 Node("M1825/2", "Outlet", object_id=19),
 Node("M853/P", "Outlet",   object_id=20)
]

lat_children[0].addNode(Node("TEST_BRANCH", "TEST_DESC", object_id=22))
print(len(lat_children))

lat166.addNode(lat_children)
scotts.addNode(lat166)
lvbc.addNode(scotts)
# scotts.addNode(Node("TEST", "TEST_DESC"))

print(lvbc)

print(lvbc.as_df())