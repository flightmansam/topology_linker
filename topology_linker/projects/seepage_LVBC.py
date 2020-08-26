"""A seepage calculator tool.
    This tool looks at the a point in time and finds all the regs that have had a totaliser value not change"""

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import pandas as pd
import fginvestigation.extraction as ext
from topology_linker.src.constants import US_REG
from topology_linker.src.node import Node
from topology_linker.src.utils import get_reachFROMreg, get_ET_RF, get_linked_ojects
import hetools.network_map as nm
from topology_linker.projects.seepage_3 import seepage
import time

days=3

period_end = pd.datetime(year=2020, month=6, day=1, hour=12)
#period_start = pd.datetime(year=2020, month=5, day=20, hour=9)

period_end = pd.date_range(start=pd.datetime(year=2018, month=6, day=3, hour=12), end=period_end, freq=f'{days}D').to_list()
period_start = [p_end - pd.Timedelta(days=days) for p_end in period_end]
dates = list(zip(period_start, period_end))

flow_thresh = 0.1 #ML/day
#gate_thresh = 2.0 #mm

#build LVBC topology
upstream_point, downstream_point = '29355', '65041'

link_df = pd.read_csv("../out/LINKED.csv",
                          usecols=['OBJECT_NO', 'LINK_OBJECT_NO', 'LINK_DESCRIPTION', 'POSITION'],
                          dtype={'OBJECT_NO': int, 'LINK_OBJECT_NO': int, 'LINK_DESCRIPTION': str, 'POSITION':int})
link_df = link_df.astype({'OBJECT_NO': str, 'LINK_OBJECT_NO': str, 'LINK_DESCRIPTION': str, 'POSITION':int})

print("collecting topology...")
link, link_list = get_linked_ojects(object_A=upstream_point,
                                    object_B=downstream_point,
                                    source=link_df)
print("done.")
print(link)
objects = [link] + link.get_all_of_desc([US_REG])
lat170 = [l for l in link.get_all() if l.object_no == '30313']
#objects = lat170 + lat170.get_all_of_desc([US_REG])
for date in dates:
    period_start = date[0]
    period_end = date[1]


    #first get the regulators that have had a low flow in the past X days
    query = (
        " SELECT DISTINCT tag.OBJECT_NO"
        " FROM SC_EVENT_LOG ev "
        " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
        " JOIN OBJECT_ATTR_VALUE oav ON oav.OBJECT_NO = tag.OBJECT_NO"
        " JOIN OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE"
        f" WHERE ot.OBJECT_DESC IN {tuple(nm.regs)} "
        " AND tag.TAG_NAME = 'FLOW_VAL'"
        f" AND tag.OBJECT_NO in {tuple([l.object_no for l in objects])}"
        f"  AND ev.EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        f"  AND ev.EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        f" AND ev.EVENT_VALUE <= {flow_thresh}"
    )

    df = ext.get_data_ordb(query)
    print(len(df))

    # GET REGS IN FLOW MODE = 0.0
    flow_mode = []
    rtu = []
    failed = []
    start = time.time()

    totaliser_N = {}

    for i, reg in enumerate(df.OBJECT_NO.unique()):

        msg = ''

        q1 = (
            "SELECT * FROM ("
            " SELECT EVENT_TIME, EVENT_VALUE, TAG_NAME "
            " FROM SC_EVENT_LOG ev "
            " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
            f"  WHERE tag.OBJECT_NO = {reg}"
            f"  AND tag.TAG_NAME in ('FLOW_ACU_SR')"
            f"  AND ev.EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
            f"  AND ev.EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
            f"  )"
        )


        t1 = time.time()
        data = ext.get_data_ordb(q1)

        if data.empty:
            #flows havent changed
            rtu.append(reg)
            totaliser_N[reg] = 0

        elif len(data) == 1:
            #can't determine
            msg += f"Not enough data for {reg} for 'FLOW_ACU_SR' in this time period, try a longer search."
            failed.append(reg)
            totaliser_N[reg] = 1
        elif len(data) > 1:
            # check for difference in first and last
            data = data.pivot_table(index="EVENT_TIME", columns="TAG_NAME")['EVENT_VALUE']
            if abs(data.FLOW_ACU_SR.iloc[-1] - data.FLOW_ACU_SR.iloc[0]) <= 0.0 + 0.001:
                rtu.append(reg)
                totaliser_N[reg] = len(data)
        if msg == '':
            continue
        else:
            print(msg)

    #filtered = [i for i in flow_mode if i in rtu] #not efficient but hey!
    filtered = rtu
    missing_seq = []
    crossover = [] #these object numbers have two sequence numbers, one for each reference point

    #check filtered for regulator pairs (pools)
    pools = {
        "reg":[],
        "next_reg":[]
    }

    start = time.time()
    #data = filtered.loc[filtered.ATTRIBUTE_VALUE == channel]
    # find sequence numbers of all the regs in this channel
    all_regs = objects

    for reg in filtered:
        # get next regulator (assuming the next regulator exists...)
        reg:Node = [l for l in objects if l.object_no == str(reg)][0]
        next_reg = [l for l in objects if l in reg.children]
        if len(next_reg) > 0:
            next_reg:Node = next_reg[0]

            if int(next_reg.object_no) in filtered:
                #Found a pool
                pools["reg"].append(int(reg.object_no))
                pools["next_reg"].append(int(next_reg.object_no))

    print(f"Done collecting pools for {period_end.strftime('%D')}")

    pools = pd.DataFrame(pools)
    #pools.to_csv("pools.csv", index=False) # for safe keeping
    if not pools.empty:
        seepage_df = seepage(pools, period_start, period_end-pd.Timedelta(days=1), days=2)
        for k, v in totaliser_N.items():
            seepage_df.loc[seepage_df.OBJECT_NO == int(k), "totaliser_N"] = [v]
        seepage_df.to_csv(f"LCBC_seepage_{period_start.strftime('%d-%m-%y')}_2day.csv", index=True)

#export out to mi sql



