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
#from topology_linker.src.constants import DS_METER, DS_ESC, US_REG
#from topology_linker.src.utils import get_linked_ojects, Q_flume, volume, get_ET_RF
import matplotlib.pyplot as plt
#from topology_linker.projects.csv2pdftable import csv2pdftable
import hetools.network_map as nm
from scipy import stats

position_regs = pd.read_csv("position_regs.csv")
period_start = pd.datetime(year=2020, month=5, day=15, hour=12)
period_end = pd.datetime(year=2020, month=5, day=20, hour=9)
# show=False
#
def get_reachFROMreg(object_no:Tuple[int, list]):
    if isinstance(object_no, list):
        query = (
            "SELECT o.OBJECT_NO, oav.ATTRIBUTE_VALUE FROM OBJECT_ATTR_VALUE oav JOIN"
            " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
            " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
            " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
            f" WHERE at.ATTRIBUTE_DESC = 'CHANNEL NAME' AND o.OBJECT_NO in {tuple(object_no)}")
        return ext.get_data_ordb(query)
    else:
        query = (
            "SELECT oav.ATTRIBUTE_VALUE FROM OBJECT_ATTR_VALUE oav JOIN"
            " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
            " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
            " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
            f" WHERE at.ATTRIBUTE_DESC = 'CHANNEL NAME' AND o.OBJECT_NO = '{object_no}'")
        return ext.get_data_ordb(query).iloc[0,0]
#
# #split the dates for the data into DAYS
# bins = pd.date_range(start=period_start, end=period_end, freq='1D')
# least_squares = pd.DataFrame(index = bins)
# seepage = pd.DataFrame(index=bins)
#
# for top_reg, top_reg_name in zip(position_regs.OBJECT_NO, position_regs.name):
#     if 'L37' not in top_reg_name:
#         continue
#
#     channel = get_reachFROMreg(int(top_reg))
#     print(channel)
#     all_regs = nm.get_regsINreach(channel)
#
#     #also get sequence numbers for the regs.
#     query = ("SELECT obr.SUB_OBJECT_NO as OBJECT_NO, o.OBJECT_NAME, obr.SUB_OBJECT_TYPE,ot.OBJECT_DESC, obr.SEQUENCE_NO"
#                 " FROM OBJECT_RELATION obr "
#                  "JOIN OBJECT o "
#                      "ON o.OBJECT_NO = obr.SUB_OBJECT_NO"
#                 " JOIN OBJECT_TYPE ot "
#                      "ON ot.OBJECT_TYPE = obr.SUB_OBJECT_TYPE"
#                 " JOIN RELATION_TYPE rt "
#                      f"ON rt.RELATION_TYPE = obr.RELATION_TYPE WHERE obr.SUP_OBJECT_NO = 215103 and obr.SUB_OBJECT_NO IN {tuple(all_regs['OBJECT_NO'])}"
#                         " ORDER BY SEQUENCE_NO")
#
#     seq = ext.get_data_ordb(query)
#     all_regs = all_regs.merge(seq[['OBJECT_NO', 'SEQUENCE_NO']], how='inner', on='OBJECT_NO').sort_values('SEQUENCE_NO')
#
#     if all_regs.empty: continue
#     #get heights of the regs
#     query =  (f"SELECT SC_EVENT_LOG.EVENT_TIME, OBJECT_NO,  SC_TAG.TAG_NAME, SC_EVENT_LOG.EVENT_VALUE "
#                      f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
#                      f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
#                      f" WHERE "
#                      f" OBJECT_NO IN {tuple(all_regs['OBJECT_NO'])} AND TAG_NAME in ('USL_VAL', 'DSL_VAL')"
#                      f" AND EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
#                      f" ORDER BY EVENT_TIME")
#     heights = ext.get_data_ordb(query)
#
#
#     days = pd.DataFrame(index=bins)
#
#     for reg in heights.OBJECT_NO.unique(): #each reg that has level data
#         event_data = heights.loc[heights.OBJECT_NO == reg, ["EVENT_TIME", "TAG_NAME", "EVENT_VALUE"]]
#         event_data = event_data.drop_duplicates().pivot(index="EVENT_TIME", columns="TAG_NAME")["EVENT_VALUE"]
#         name = all_regs.loc[all_regs.OBJECT_NO == reg, "OBJECT_NAME"].iloc[0]
#         for col in event_data.columns:
#             for start, end in zip(bins, bins.shift(1)):
#                 daily_drop = event_data.loc[(event_data.index > start) & (event_data.index < end), col].dropna()
#
#                 if len(daily_drop) > 1:
#                     xs =(daily_drop.index - daily_drop.index[0]).total_seconds()
#                     m, b, r_value, p_value, std_err = stats.linregress(xs, daily_drop.to_numpy())
#                     # p, res, _, _, _ = pd.np.polyfit(, 1, full=True)
#                     # m, b = p
#                     res = r_value * r_value
#                     daily_drop = list(map(lambda x: m*x + b, [xs[0], xs[-1]]))
#                     days.loc[start, f"{reg}-{col}"] = daily_drop[0] - daily_drop[1] #start - end
#                     seepage.loc[start, f"{name}-{col}"] = daily_drop[0] - daily_drop[1] #start - end
#                     least_squares.loc[start, f"{name}-{col}"] = res #if len(res) > 0 else None
#
#     fig, ax = plt.subplots(1, 1, constrained_layout=True)
#
#     import itertools
#     markers = itertools.cycle(['H', '^', 'v', 'd', '3', '.', '1', '_'])
#     days*=1000 #convert to mm
#     days.plot(ax=ax, style='x-', legend=False)
#
#     for line in ax.get_lines():
#         line.set_marker(next(markers))
#     handles, labels = ax.get_legend_handles_labels()
#     names = all_regs.loc[all_regs.OBJECT_NO.isin([l.split('-')[0] for l in labels]), ["OBJECT_NAME", "OBJECT_NO"]]
#     labels = [f"{l} - {names.loc[names.OBJECT_NO == int(l.split('-')[0]), 'OBJECT_NAME'].iloc[0]}" for l in labels]
#     ax.legend(handles, labels, loc=8, ncol=4)
#     mean, median, std = days.mean(axis=1), days.median(axis=1), days.std(axis=1)
#
#     ax.set_xticks([],[])
#     ax.table(cellText=list(map(lambda a: [f"{i:.2f}" for i in a], [mean, median, std])),
#              rowLabels=["MEAN", "MEDIAN", "STD"],
#              colLabels=[day.strftime("%D") for day in days.index])
#     ax.plot(days.index, median , 'sr', label='MEDIAN')
#     ax.errorbar(days.index, mean ,yerr=std, fmt='sk', ecolor='red', label="MEAN-STD")
#     ax.set_title(f"{channel} SEEPAGE")
#     ax.set_ylabel("DAILY SEEPAGE (mm)")
#
#     #scale factor to adjust plot to align with cell columns
#     dx = ax.get_xbound()
#     change = (dx[1] - dx[0]) / (2*len(days.index) - 2)
#     ax.set_xbound(dx[0]-change, dx[1]+change)
#
#     fig.set_size_inches(w=13.33, h=9.3)
#     if show:
#         plt.show()
#     else:
#         plt.savefig(f"../out/seepage/{channel}-{top_reg_name}.png")
#         plt.close()
#
#
#
#     ####### BAR CHART #####
#     days.iloc[[-2, -1], ::-1].transpose().plot.bar(subplots=True, rot=25, legend=False)
#     locs, labels = plt.xticks()
#
#     for i, l in enumerate(labels):
#         s = l.get_text().split('-')
#         s = f"{all_regs.loc[all_regs.OBJECT_NO==int(s[0]), 'OBJECT_NAME'].iloc[0]}-{s[1].split('_')[0]}"
#         l.set_text(s)
#         labels[i] = l
#
#     plt.xticks(locs, labels)
#
#     plt.gcf().suptitle(f"{channel}-{top_reg_name} SEEPAGE (two day)")
#     plt.gcf().set_size_inches(w=13.33, h=9.3)
#     if show:
#         plt.show()
#     else:
#         plt.savefig(f"../out/seepage/{channel}-{top_reg_name}_2d.png")
#         plt.close()
#
# #####TABLES
# #change column object_nos to name
# usl = least_squares.loc[:, least_squares.columns.str.contains("USL")].iloc[-3:, :].transpose()
# usl = pd.concat([seepage.loc[:, seepage.columns.str.contains("USL")].iloc[-3:, :].transpose()*1000, usl], join='inner', axis=1)
#
# #rename columns
# col = [f"Seepage - {i.strftime('%D')}" for i in bins[-3:]]+[f"r2 -{i.strftime('%D')}" for i in bins[-3:]]
# usl.columns = col
# usl = usl.iloc[:, [0, 3, 1, 4, 2, 5]]
# usl.to_csv("USL.csv")
#
# #change column object_nos to name
# dsl = least_squares.loc[:, least_squares.columns.str.contains("DSL")].iloc[-3:, :].transpose()
# dsl = pd.concat([seepage.loc[:, seepage.columns.str.contains("DSL")].iloc[-3:, :].transpose()*1000, usl], join='inner', axis=1)
#
# dsl.columns = col
# dsl = usl.iloc[:, [0, 3, 1, 4, 2, 5]]
#
# dsl.to_csv("DSL.csv")
# print()

period_end = pd.datetime(year=2020, month=5, day=20, hour=9)
period_start = pd.datetime(year=2020, month=5, day=20, hour=9)
period_start = period_end - pd.Timedelta(days=10) #24Hr

flow_thresh = 0.0 #ML/day
gate_thresh = 2.0 #mm

object_no = 31236
query = ("SELECT * FROM OBJECT_ATTR_VALUE oav JOIN"
             " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
             " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
             " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
            f" WHERE o.OBJECT_NO = '31236'")

print()
#first get the regulators that havehad a low flow in the past X hours
query = (
    " SELECT DISTINCT tag.OBJECT_NO"
    " FROM SC_EVENT_LOG ev "
    " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
    " JOIN OBJECT_ATTR_VALUE oav ON oav.OBJECT_NO = tag.OBJECT_NO"
    " WHERE oav.OBJECT_TYPE IN ('7', '269', '90') "
    " AND tag.TAG_NAME = 'FLOW_VAL'"
    f" AND ev.EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    f" AND ev.EVENT_VALUE <= {flow_thresh}"

)

df = ext.get_data_ordb(query)
#df = pd.read_csv("df.csv")

gates = [f'G{i}_POS_VAL' for i in range(7)]
query = (
    "SELECT ev.EVENT_TIME, tag.OBJECT_NO, ev.EVENT_VALUE, tag.TAG_NAME"
    " FROM SC_EVENT_LOG ev "
    " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
    f" WHERE tag.TAG_NAME in {tuple(gates)}"
    f" AND tag.OBJECT_NO in {tuple(df.OBJECT_NO.unique())}"
    f" AND ev.EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    f" AND ev.EVENT_VALUE <= {gate_thresh}"
    f" ORDER BY ev.EVENT_TIME"
)
gates = ext.get_data_ordb(query)
filtered = []
for obj in gates.OBJECT_NO.unique():
    #check to see all were below thresh
    data = gates.loc[gates.OBJECT_NO == obj]
    if data.pivot(index="EVENT_TIME", columns="TAG_NAME", values="EVENT_VALUE").mean(axis=1).le(gate_thresh/1000).all():
        filtered.append(obj)

# GET REGS IN FLOW MODE = 0.0
flow_mode = []
rtu = []
for reg in df.OBJECT_NO.unique():

    query = (
        "SELECT EVENT_TIME, EVENT_VALUE, TAG_NAME "
        " FROM SC_EVENT_LOG ev "
        " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
        f"  WHERE tag.OBJECT_NO = {object_no}"
        f"  AND tag.TAG_NAME in ('FLOW_SP', 'CTRL_MODE', 'FLOW_ACU_SR')"
        f"  AND ev.EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        f"  AND ev.EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    )
    data = ext.get_data_ordb(query)
    data = data.pivot_table(index="EVENT_TIME", columns="TAG_NAME")['EVENT_VALUE']
    if 'FLOW_SP' in data.columns and 'CTRL_MODE' in data.columns:
        if (data.CTRL_MODE.dropna() == 3).all() and (data.FLOW_SP.dropna() == 0.0).all(): flow_mode.append(reg)
    else:
        print(f"No data for {reg} for 'FLOW_SP' or 'CTRL_MODE' in this time period, try a longer search.")

    # CHECK for totaliser volumes that haven't changed

    if data.FLOW_ACU_SR.dropna().empty:
        #flows havent changed
        rtu.append(reg)
    elif len(data.FLOW_ACU_SR.dropna()) == 1:
        #can't determine
        print(f"No data for {reg} for 'FLOW_ACU_SR' in this time period, try a longer search.")
    elif len(data.FLOW_ACU_SR.dropna()) > 1:
        # check for diffrence in first and last
        if abs(data.FLOW_ACU_SR.iloc[-1] - data.FLOW_ACU_SR[0]) <= 0.0 + 0.001:
            rtu.append(reg)


filtered = [i for i in flow_mode if i in rtu]

#check filtered for regulator pairs

#get channel names
filtered = get_reachFROMreg(filtered)
pools = []
for channel in filtered.ATTRIBUTE_VALUE.unique():
    data = filtered.loc[filtered.ATTRIBUTE_VALUE == channel]
    #find sequence numbers of all the regs in this channel
    all_regs = nm.get_regsINreach(channel)
    if len(all_regs) > 1:
        # also get sequence numbers for the regs.
        query = ("SELECT obr.SUB_OBJECT_NO as OBJECT_NO, o.OBJECT_NAME, obr.SUB_OBJECT_TYPE,ot.OBJECT_DESC, obr.SEQUENCE_NO"
                 " FROM OBJECT_RELATION obr "
                 "JOIN OBJECT o "
                 "ON o.OBJECT_NO = obr.SUB_OBJECT_NO"
                 " JOIN OBJECT_TYPE ot "
                 "ON ot.OBJECT_TYPE = obr.SUB_OBJECT_TYPE"
                 " JOIN RELATION_TYPE rt "
                 f"ON rt.RELATION_TYPE = obr.RELATION_TYPE WHERE obr.SUP_OBJECT_NO = 215103 and obr.SUB_OBJECT_NO IN {tuple(all_regs['OBJECT_NO'])}"
                 " ORDER BY SEQUENCE_NO")

        seq = ext.get_data_ordb(query)
        all_regs = all_regs.merge(seq[['OBJECT_NO', 'SEQUENCE_NO']], how='inner', on='OBJECT_NO').sort_values('SEQUENCE_NO').reset_index(drop=True)

        #inner join data with all the regs
        for reg in data.OBJECT_NO.unique():
            if reg in all_regs.OBJECT_NO.to_list():
                idx = all_regs.loc[all_regs.OBJECT_NO == reg].index.item()
                #get next regulator (assuming the next regulator exists...)
                idx+=1
                if idx < len(all_regs):
                    next_reg = all_regs.iloc[idx]["OBJECT_NO"]

                    if next_reg in data.OBJECT_NO.to_list():
                        #Found a pool
                        pools.append([reg, next_reg])
                    print()




cutoff = 0.1 #ratio for only getting main regulators

new_filtered = []

for reg in filtered:
    query = (
        "SELECT AVG(EVENT_VALUE) FROM SC_EVENT_LOG ev "
        " JOIN SC_TAG tag ON ev.TAG_ID = tag.TAG_ID "
        f" WHERE tag.TAG_NAME = 'FLOW_VAL'"
        f" AND tag.OBJECT_NO = {reg}"
        f" AND ev.EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    )
    ave = ext.get_data_ordb(query).iloc[0, 0]

    if (flow_thresh / ave) < cutoff:
        new_filtered.append(reg)

query = (
    "SELECT DISTINCT oav.OBJECT_NO, o.OBJECT_NAME, oav.OBJECT_TYPE, ot.OBJECT_DESC"
    " FROM OBJECT_ATTR_VALUE oav JOIN"
    " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
    " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
    " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
    f" WHERE o.OBJECT_NO IN {tuple(df.OBJECT_NO)}"
)

out = ext.get_data_ordb(query)
out.astype({"OBJECT_NO":int, "OBJECT_TYPE":int})
out.to_csv("low_flows.csv", index=False)

print()
#
# print("collecting topology...")
# upstream_point = '26025'
# downstream_point = '40949'
#
# threshold = 0.0
#
# link_df = "../out/WARBURN_LINKED.csv"
#
# link_df = pd.read_csv(link_df,
#                           usecols=['OBJECT_NO', 'LINK_OBJECT_NO', 'LINK_DESCRIPTION', 'POSITION'],
#                           dtype={'OBJECT_NO': str, 'LINK_OBJECT_NO': str, 'LINK_DESCRIPTION': str})
#
# link, link_list = get_linked_ojects(object_A=upstream_point,
#                                     object_B=downstream_point,
#                                     source=link_df)
# link_list = [link] + link.get_all_of_desc(desc=[US_REG, DS_METER])
#
# print("done.")
#
# query = (f"SELECT OBJECT_NO, SC_EVENT_LOG.EVENT_TIME, SC_EVENT_LOG.EVENT_VALUE, TAG_NAME "
#              f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
#              f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
#              f" WHERE "
#              f" OBJECT_NO IN {tuple([l.object_no for l in link_list])} AND TAG_NAME in ('USL_VAL', 'DSL_VAL', 'FLOW_VAL')"
#              f" AND EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
#              f"AND EVENT_TIME < TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
#              f" ORDER BY EVENT_TIME")
#
# obj_data = ext.get_data_ordb(query)
# obj_data = obj_data.astype({"OBJECT_NO": str, "EVENT_VALUE": float})
#
# all_zeros = None
# for object_no in obj_data.OBJECT_NO.unique():
#     data = obj_data.loc[obj_data.OBJECT_NO == object_no, :].pivot_table(index="EVENT_TIME", columns="TAG_NAME").interpolate(limit_direction="both")
#     data.plot()
#
#     # get all the times it transitions from >=threshold -> < threshold and < threshold -> =< threshold
#     data["THRESH"] = data.EVENT_VALUE.FLOW_VAL > threshold
#     if all_zeros is None:
#         all_zeros = pd.DataFrame(data=data["THRESH"].values, index=data.index, columns=[object_no])
#     else:
#         new_index = all_zeros.index.append(data.index).drop_duplicates()
#         all_zeros = all_zeros.reindex(new_index)
#         all_zeros[object_no] = data["THRESH"]
#
#     data = data.loc[((data.THRESH == True) & (data.shift(-1).THRESH == False)) | ((data.THRESH == True) & (data.shift().THRESH == False))]
#     for x in data.index:
#         plt.axvline(x, color='r')
#
#     data = data.reset_index()
#
#     # calculate the total time of each period above the threshold
#     time_thresh = data.EVENT_TIME - data.shift().EVENT_TIME
#     time_thresh = time_thresh[1::2].sum()  # get every second result the sum is the total meter time above threshold
#
#     # %zero
#     time = 100 * time_thresh / (period_end - period_start)
#     print(f"% time {object_no} is 0 = {time:.1f}%")
#     plt.title(object_no)
#     plt.show()
#
#     print()
#
# all_zeros.sort_index(inplace=True)
# all_zeros.fillna(method='ffill', inplace=True)
# # all_zeros.astype(float).plot()
# # plt.show()
#
# overlap = all_zeros#.loc[(all_zeros == True).all(axis=1)]
#
# edges = overlap.loc[(~(overlap == True).any(axis=1) & (overlap.shift(-1) == True).any(axis=1))|(~(overlap == True).any(axis=1) &(overlap.shift() == True).any(axis=1))]
# #overlap.loc[(~(overlap == True).any(axis=1) & (overlap.shift(-1) == True).any(axis=1)) | ((data.THRESH == True) & (data.shift().THRESH == False))]
# DSL = obj_data.pivot_table(index=["EVENT_TIME"], columns=["TAG_NAME", "OBJECT_NO"]).interpolate().EVENT_VALUE
# DSL.plot()
# DSL = DSL[["DSL_VAL", "USL_VAL"]]
# for x in edges.index:
#     plt.axvline(x, color='r')
#
# # calculate the total time of each period above the threshold
# edges = edges.reset_index()
# time_thresh = edges.EVENT_TIME - edges.shift().EVENT_TIME
# time_thresh = time_thresh[1::2].sum()  # get every second result the sum is the total meter time above threshold
#
# # %zero
# time = 100 * time_thresh / (period_end - period_start)
# print(len(edges))
# print(f"% time all meters is 0 = {time:.1f}%")
# print()
#
# out = {}
# for object_no in obj_data.OBJECT_NO.unique():
#     out[object_no] = [0, 0]
# # measure gradients for periods in between the edges
# edges = edges.set_index("EVENT_TIME")
# i = 0
# for lhs, rhs in zip(edges.index[::2], edges.index[1::2]):
#     print(lhs, rhs)
#     trim = 0.0 #trim the first
#     trim = trim*(rhs-lhs)
#     print(f"trimmed by {trim}")
#     print(lhs+trim, rhs)
#
#     levels = DSL.loc[(DSL.index >= lhs + trim) & (DSL.index <= rhs)]
#     for name, site in levels.groupby(level=1, axis=1):
#         print(name)
#         x = site.reset_index().EVENT_TIME - site.reset_index().EVENT_TIME[0]
#
#         for level in site:
#             level_name = level[0]
#             if level_name == 'USL_VAL' or level_name == 'DSL_VAL':
#                 level = site[level]
#                 m, c = pd.np.polyfit(x.dt.total_seconds().astype(float), level.values, 1)
#                 #convert m to mm/day
#                 m = (m * 86400 * 1000)
#                 if level_name == 'USL_VAL':
#                     out[name][0]+=m
#                 else:
#                     out[name][1]+=m
#                 print(f"{level_name} gradient = {m:.2f} mm/d")
#     print()
#     i+=1
#
# print(out)
#
# out = pd.DataFrame(out.values(), out.keys())
# out /= i
# print(out.to_string())
#
# plt.show()

