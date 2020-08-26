"""A seepage calculator tool.
    This tool looks at the a point in time and finds all the regs that have had a totaliser value not change"""

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import pandas as pd
import fginvestigation.extraction as ext
from topology_linker.src.utils import get_reachFROMreg, get_ET_RF
import hetools.network_map as nm
from topology_linker.projects.seepage_3 import seepage
import time



days=3
period_end = pd.datetime(year=2020, month=8, day=11
                         , hour=12)
#period_start = pd.datetime(year=2020, month=5, day=20, hour=9)
period_start = period_end - pd.Timedelta(days=days) #24Hr

flow_thresh = 0.1 #ML/day
#gate_thresh = 2.0 #mm

#first get the regulators that have had a low flow in the past X days
query = (
    " SELECT DISTINCT tag.OBJECT_NO"
    " FROM SC_EVENT_LOG ev "
    " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
    " JOIN OBJECT_ATTR_VALUE oav ON oav.OBJECT_NO = tag.OBJECT_NO"
    " JOIN OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE"
    f" WHERE ot.OBJECT_DESC IN {tuple(nm.regs)} "
    " AND tag.TAG_NAME = 'FLOW_VAL'"
    f"  AND ev.EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    f"  AND ev.EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    f" AND ev.EVENT_VALUE <= {flow_thresh}"
)

# query = (
#     " SELECT DISTINCT o.OBJECT_NO"
#     " FROM OBJECT_ATTR_VALUE oav JOIN"
#     " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
#     " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
#     " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
#     f" WHERE ot.OBJECT_DESC IN {tuple(nm.regs)} "
#
# )

df = ext.get_data_ordb(query)
print(len(df))
# gates = [f'G{i}_POS_VAL' for i in range(7)]
# query = (
#     "SELECT ev.EVENT_TIME, tag.OBJECT_NO, ev.EVENT_VALUE, tag.TAG_NAME"
#     " FROM SC_EVENT_LOG ev "
#     " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
#     f" WHERE tag.TAG_NAME in {tuple(gates)}"
#     f" AND tag.OBJECT_NO in {tuple(df.OBJECT_NO.unique())}"
#     f" AND ev.EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
#     f" AND ev.EVENT_VALUE <= {gate_thresh}"
#     f" ORDER BY ev.EVENT_TIME"
# )
# gates = ext.get_data_ordb(query)
# filtered = []
# for obj in gates.OBJECT_NO.unique():
#     #check to see all were below thresh
#     data = gates.loc[gates.OBJECT_NO == obj]
#     if data.pivot(index="EVENT_TIME", columns="TAG_NAME", values="EVENT_VALUE").mean(axis=1).le(gate_thresh/1000).all():
#         filtered.append(obj)

# GET REGS IN FLOW MODE = 0.0
flow_mode = []
rtu = []
failed = []
start = time.time()
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

    # q2 = (
    #     "SELECT * FROM ("
    #     " SELECT EVENT_TIME, EVENT_VALUE, TAG_NAME"
    #     " FROM SC_EVENT_LOG ev "
    #     " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
    #     f"  WHERE tag.OBJECT_NO = {reg}"
    #     f"  AND tag.TAG_NAME IN ('FLOW_SP')"
    #     f"  AND ev.EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    #     " ORDER BY EVENT_TIME)"
    #     " WHERE ROWNUM <2"
    # )
    #
    # q3 =(
    #     "SELECT * FROM ("
    #     " SELECT EVENT_TIME, EVENT_VALUE, TAG_NAME"
    #     " FROM SC_EVENT_LOG ev "
    #     " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
    #     f"  WHERE tag.OBJECT_NO = {reg}"
    #     f"  AND tag.TAG_NAME IN ('CTRL_MODE')"
    #     f"  AND ev.EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    #     " ORDER BY EVENT_TIME)"
    #     " WHERE ROWNUM <2"
    # )

    # query = (
    #     f"{q1} UNION ALL {q2} UNION ALL {q3}"
    # )

    t1 = time.time()
    data = ext.get_data_ordb(q1)
    dt = time.time() - t1
    if i%10 == 0:
        print(f"{i} - {time.time() - start:.1f}s (dt = {dt:.2f}s)")

    # if data.empty:
    #     msg = f'{reg} Failed.'
    #     failed.append(reg)
    #     print(msg)
    #     continue

    # data = data.pivot_table(index="EVENT_TIME", columns="TAG_NAME")['EVENT_VALUE']
    # if 'FLOW_SP' in data.columns and 'CTRL_MODE' in data.columns:
    #     if (data.CTRL_MODE.dropna() == 3).all() and (data.FLOW_SP.dropna() == 0.0).all(): flow_mode.append(reg)
    # else:
    #     msg += f"No data for {reg} for 'FLOW_SP' or 'CTRL_MODE' in this time period, try a longer search.\n"
    #
    # # CHECK for totaliser volumes that haven't changed
    #
    # if 'FLOW_ACU_SR' not in data.columns or data.FLOW_ACU_SR.dropna().empty:
    #     #flows havent changed
    #     rtu.append(reg)
    # elif len(data.FLOW_ACU_SR.dropna()) == 1:
    #     #can't determine
    #     msg += f"Not enough data for {reg} for 'FLOW_ACU_SR' in this time period, try a longer search."
    #     failed.append(reg)
    # elif len(data.FLOW_ACU_SR.dropna()) > 1:
    #     # check for diffrence in first and last
    #     if abs(data.FLOW_ACU_SR.iloc[-1] - data.FLOW_ACU_SR.iloc[0]) <= 0.0 + 0.001:
    #         rtu.append(reg)
    # if msg == '':
    #     continue
    # else:
    #     print(msg)
    # CHECK for totaliser volumes that haven't changed

    if data.empty:
        #flows havent changed
        rtu.append(reg)
    elif len(data) == 1:
        #can't determine
        msg += f"Not enough data for {reg} for 'FLOW_ACU_SR' in this time period, try a longer search."
        failed.append(reg)
    elif len(data) > 1:
        # check for difference in first and last
        data = data.pivot_table(index="EVENT_TIME", columns="TAG_NAME")['EVENT_VALUE']
        if abs(data.FLOW_ACU_SR.iloc[-1] - data.FLOW_ACU_SR.iloc[0]) <= 0.0 + 0.001:
            rtu.append(reg)
    if msg == '':
        continue
    else:
        print(msg)

#filtered = [i for i in flow_mode if i in rtu] #not efficient but hey!
filtered = rtu
missing_seq = []
crossover = [] #these object numbers have two sequence numbers, one for each reference point

#get channel names
filtered = get_reachFROMreg(filtered)

#check filtered for regulator pairs (pools)
pools = {
    "reg":[],
    "next_reg":[]
}
for channel in filtered.ATTRIBUTE_VALUE.unique():
    start = time.time()
    data = filtered.loc[filtered.ATTRIBUTE_VALUE == channel]
    #find sequence numbers of all the regs in this channel
    all_regs = nm.get_regsINreach(channel)
    t1 = time.time()
    t2 = t1
    if len(all_regs) > 1:
        # also get sequence numbers for the regs.
        query = ("SELECT obr.SUB_OBJECT_NO as OBJECT_NO, o.OBJECT_NAME, obr.SUB_OBJECT_TYPE,ot.OBJECT_DESC, obr.SEQUENCE_NO"
                 " FROM OBJECT_RELATION obr "
                 "JOIN OBJECT o "
                 "ON o.OBJECT_NO = obr.SUB_OBJECT_NO"
                 " JOIN OBJECT_TYPE ot "
                 "ON ot.OBJECT_TYPE = obr.SUB_OBJECT_TYPE"
                 " JOIN RELATION_TYPE rt "
                 f"ON rt.RELATION_TYPE = obr.RELATION_TYPE WHERE "
                 f"(obr.SUP_OBJECT_NO = 214925 and obr.SUB_OBJECT_NO IN {tuple(all_regs['OBJECT_NO'])})"
                 f" or (obr.SUP_OBJECT_NO = 215103 and obr.SUB_OBJECT_NO IN {tuple(all_regs['OBJECT_NO'])})"
                 " ORDER BY SEQUENCE_NO")

        seq = ext.get_data_ordb(query)
        if seq.empty: missing_seq.append(channel)
        else:
            all_regs = all_regs.merge(seq[['OBJECT_NO', 'SEQUENCE_NO']], how='inner', on='OBJECT_NO').sort_values('SEQUENCE_NO')

            #exclude "regs" that are not in the EVENTS table, and therefore not going to be in filtered (these are decomissioned regs and other crap)
            querys = [
                f"SELECT '{i}', MAX(ROWNUM) FROM (SELECT * FROM SC_EVENT_LOG ev JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID WHERE tag.OBJECT_NO = {i})"
                for i in all_regs.OBJECT_NO.unique()]
            all_regs = all_regs.loc[all_regs.OBJECT_NO.isin(ext.get_data_ordb(" UNION ALL ".join(querys)).dropna().iloc[:, 0]), :].reset_index(drop=True)
            t2 = time.time()
            for reg in data.OBJECT_NO.unique():
                if reg in all_regs.OBJECT_NO.to_list():
                    reg = all_regs.loc[all_regs.OBJECT_NO == reg]
                    if len(reg)> 1: crossover.append(reg)
                    else:
                        idx = reg.index.item()
                        idx+=1
                        if idx < len(all_regs):
                            # get next regulator (assuming the next regulator exists...)
                            next_reg = all_regs.iloc[idx]

                            if next_reg["OBJECT_NO"] in data.OBJECT_NO.to_list():
                                #Found a pool
                                pools["reg"].append(reg.OBJECT_NO.iloc[0])
                                pools["next_reg"].append(next_reg.OBJECT_NO)
    t3 = time.time()
    if t2 == t1: t2 = -1.0
    print(f"{channel} took {t3 - start:.1f} seconds (t1, t2, t3) = ({t1 - start:.1f}, {t2-t1 if t2 >0 else t2:.1f}, {t3-t2:.1f})")


pools = pd.DataFrame(pools)
pools.to_csv("pools.csv", index=False) # for safe keeping
seepage = seepage(pools, period_start, period_end, days=days)
seepage.to_csv(f"seepage_{period_start.strftime('%d-%m-%y')}.csv", index=True)

#export out to mi sql



