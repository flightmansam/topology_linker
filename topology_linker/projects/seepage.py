"""A seepage calculator tool.
Takes a pool (described by a linkage table) and finds when flow is zero. It then calculates statisics on those zero flows:
    * Duration, frequency
    * Seepage gradient"""
import io
from typing import Tuple

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import pandas as pd
import requests
import fginvestigation.extraction as ext
from topology_linker.src.constants import DS_METER, DS_ESC, US_REG
from topology_linker.src.utils import get_linked_ojects, Q_flume, volume, get_ET_RF
import matplotlib.pyplot as plt
from topology_linker.projects.csv2pdftable import csv2pdftable


period_start = pd.datetime(year=2019, month=6, day=1, hour=00)
period_start = pd.datetime(year=2019, month=5, day=1, hour=00)
period_end = pd.datetime(year=2020, month=3, day=11, hour=00)

print("collecting topology...")
upstream_point = '26025'
downstream_point = '40949'

threshold = 0.0

link_df = "../out/WARBURN_LINKED.csv"

link_df = pd.read_csv(link_df,
                          usecols=['OBJECT_NO', 'LINK_OBJECT_NO', 'LINK_DESCRIPTION', 'POSITION'],
                          dtype={'OBJECT_NO': str, 'LINK_OBJECT_NO': str, 'LINK_DESCRIPTION': str})

link, link_list = get_linked_ojects(object_A=upstream_point,
                                    object_B=downstream_point,
                                    source=link_df)
link_list = [link] + link.get_all_of_desc(desc=[US_REG, DS_METER])

print("done.")

query = (f"SELECT OBJECT_NO, SC_EVENT_LOG.EVENT_TIME, SC_EVENT_LOG.EVENT_VALUE, TAG_NAME "
             f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
             f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
             f" WHERE "
             f" OBJECT_NO IN {tuple([l.object_no for l in link_list])} AND TAG_NAME in ('USL_VAL', 'DSL_VAL', 'FLOW_VAL')"
             f" AND EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f"AND EVENT_TIME < TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f" ORDER BY EVENT_TIME")

obj_data = ext.get_data_ordb(query)
obj_data = obj_data.astype({"OBJECT_NO": str, "EVENT_VALUE": float})

all_zeros = None
for object_no in obj_data.OBJECT_NO.unique():
    data = obj_data.loc[obj_data.OBJECT_NO == object_no, :].pivot_table(index="EVENT_TIME", columns="TAG_NAME").interpolate(limit_direction="both")
    data.plot()

    # get all the times it transitions from >=threshold -> < threshold and < threshold -> =< threshold
    data["THRESH"] = data.EVENT_VALUE.FLOW_VAL > threshold
    if all_zeros is None:
        all_zeros = pd.DataFrame(data=data["THRESH"].values, index=data.index, columns=[object_no])
    else:
        new_index = all_zeros.index.append(data.index).drop_duplicates()
        all_zeros = all_zeros.reindex(new_index)
        all_zeros[object_no] = data["THRESH"]

    data = data.loc[((data.THRESH == True) & (data.shift(-1).THRESH == False)) | ((data.THRESH == True) & (data.shift().THRESH == False))]
    for x in data.index:
        plt.axvline(x, color='r')

    data = data.reset_index()

    # calculate the total time of each period above the threshold
    time_thresh = data.EVENT_TIME - data.shift().EVENT_TIME
    time_thresh = time_thresh[1::2].sum()  # get every second result the sum is the total meter time above threshold

    # %zero
    time = 100 * time_thresh / (period_end - period_start)
    print(f"% time {object_no} is 0 = {time:.1f}%")
    plt.title(object_no)
    plt.show()

    print()

all_zeros.sort_index(inplace=True)
all_zeros.fillna(method='ffill', inplace=True)
# all_zeros.astype(float).plot()
# plt.show()

overlap = all_zeros#.loc[(all_zeros == True).all(axis=1)]

edges = overlap.loc[(~(overlap == True).any(axis=1) & (overlap.shift(-1) == True).any(axis=1))|(~(overlap == True).any(axis=1) &(overlap.shift() == True).any(axis=1))]
#overlap.loc[(~(overlap == True).any(axis=1) & (overlap.shift(-1) == True).any(axis=1)) | ((data.THRESH == True) & (data.shift().THRESH == False))]
DSL = obj_data.pivot_table(index=["EVENT_TIME"], columns=["TAG_NAME", "OBJECT_NO"]).interpolate().EVENT_VALUE
DSL.plot()
DSL = DSL[["DSL_VAL", "USL_VAL"]]
for x in edges.index:
    plt.axvline(x, color='r')

# calculate the total time of each period above the threshold
edges = edges.reset_index()
time_thresh = edges.EVENT_TIME - edges.shift().EVENT_TIME
time_thresh = time_thresh[1::2].sum()  # get every second result the sum is the total meter time above threshold

# %zero
time = 100 * time_thresh / (period_end - period_start)
print(len(edges))
print(f"% time all meters is 0 = {time:.1f}%")
print()

out = {}
for object_no in obj_data.OBJECT_NO.unique():
    out[object_no] = [0, 0]
# measure gradients for periods in between the edges
edges = edges.set_index("EVENT_TIME")
i = 0
for lhs, rhs in zip(edges.index[::2], edges.index[1::2]):
    print(lhs, rhs)
    trim = 0.0 #trim the first
    trim = trim*(rhs-lhs)
    print(f"trimmed by {trim}")
    print(lhs+trim, rhs)

    levels = DSL.loc[(DSL.index >= lhs + trim) & (DSL.index <= rhs)]
    for name, site in levels.groupby(level=1, axis=1):
        print(name)
        x = site.reset_index().EVENT_TIME - site.reset_index().EVENT_TIME[0]

        for level in site:
            level_name = level[0]
            if level_name == 'USL_VAL' or level_name == 'DSL_VAL':
                level = site[level]
                m, c = pd.np.polyfit(x.dt.total_seconds().astype(float), level.values, 1)
                #convert m to mm/day
                m = (m * 86400 * 1000)
                if level_name == 'USL_VAL':
                    out[name][0]+=m
                else:
                    out[name][1]+=m
                print(f"{level_name} gradient = {m:.2f} mm/d")
    print()
    i+=1

print(out)

out = pd.DataFrame(out.values(), out.keys())
out /= i
print(out.to_string())

plt.show()

