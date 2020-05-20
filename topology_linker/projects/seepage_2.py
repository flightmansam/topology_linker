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
show=False

def get_reachFROMreg(object_no:int):
    query = (
        "SELECT oav.ATTRIBUTE_VALUE FROM OBJECT_ATTR_VALUE oav JOIN"
        " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
        " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
        " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
        f" WHERE at.ATTRIBUTE_DESC = 'CHANNEL NAME' AND o.OBJECT_NO = '{object_no}'")
    return ext.get_data_ordb(query).iloc[0,0]

#split the dates for the data into DAYS
bins = pd.date_range(start=period_start, end=period_end, freq='1D')
least_squares = pd.DataFrame(index = bins)
seepage = pd.DataFrame(index=bins)

for top_reg, top_reg_name in zip(position_regs.OBJECT_NO, position_regs.name):
    # if 'L37' not in top_reg_name:
    #     continue

    channel = get_reachFROMreg(int(top_reg))
    print(channel)
    all_regs = nm.get_regsINreach(channel)

    #also get sequence numbers for the regs.
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
    all_regs = all_regs.merge(seq[['OBJECT_NO', 'SEQUENCE_NO']], how='inner', on='OBJECT_NO').sort_values('SEQUENCE_NO')

    if all_regs.empty: continue
    #get heights of the regs
    query =  (f"SELECT SC_EVENT_LOG.EVENT_TIME, OBJECT_NO,  SC_TAG.TAG_NAME, SC_EVENT_LOG.EVENT_VALUE "
                     f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
                     f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
                     f" WHERE "
                     f" OBJECT_NO IN {tuple(all_regs['OBJECT_NO'])} AND TAG_NAME in ('USL_VAL', 'DSL_VAL')"
                     f" AND EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                     f" ORDER BY EVENT_TIME")
    heights = ext.get_data_ordb(query)


    days = pd.DataFrame(index=bins)

    for reg in heights.OBJECT_NO.unique(): #each reg that has level data
        event_data = heights.loc[heights.OBJECT_NO == reg, ["EVENT_TIME", "TAG_NAME", "EVENT_VALUE"]]
        event_data = event_data.drop_duplicates().pivot(index="EVENT_TIME", columns="TAG_NAME")["EVENT_VALUE"]
        name = all_regs.loc[all_regs.OBJECT_NO == reg, "OBJECT_NAME"].iloc[0]
        for col in event_data.columns:
            for start, end in zip(bins, bins.shift(1)):
                daily_drop = event_data.loc[(event_data.index > start) & (event_data.index < end), col].dropna()

                if len(daily_drop) > 1:
                    xs =(daily_drop.index - daily_drop.index[0]).total_seconds()
                    m, b, r_value, p_value, std_err = stats.linregress(xs, daily_drop.to_numpy())
                    # p, res, _, _, _ = pd.np.polyfit(, 1, full=True)
                    # m, b = p
                    res = r_value * r_value
                    daily_drop = list(map(lambda x: m*x + b, [xs[0], xs[-1]]))
                    days.loc[start, f"{reg}-{col}"] = daily_drop[0] - daily_drop[1] #start - end
                    seepage.loc[start, f"{name}-{col}"] = daily_drop[0] - daily_drop[1] #start - end
                    least_squares.loc[start, f"{name}-{col}"] = res #if len(res) > 0 else None

    fig, ax = plt.subplots(1, 1, constrained_layout=True)

    import itertools
    markers = itertools.cycle(['H', '^', 'v', 'd', '3', '.', '1', '_'])
    days*=1000 #convert to mm
    days.plot(ax=ax, style='x-', legend=False)

    for line in ax.get_lines():
        line.set_marker(next(markers))
    handles, labels = ax.get_legend_handles_labels()
    names = all_regs.loc[all_regs.OBJECT_NO.isin([l.split('-')[0] for l in labels]), ["OBJECT_NAME", "OBJECT_NO"]]
    labels = [f"{l} - {names.loc[names.OBJECT_NO == int(l.split('-')[0]), 'OBJECT_NAME'].iloc[0]}" for l in labels]
    ax.legend(handles, labels, loc=8, ncol=4)
    mean, median, std = days.mean(axis=1), days.median(axis=1), days.std(axis=1)

    ax.set_xticks([],[])
    ax.table(cellText=list(map(lambda a: [f"{i:.2f}" for i in a], [mean, median, std])),
             rowLabels=["MEAN", "MEDIAN", "STD"],
             colLabels=[day.strftime("%D") for day in days.index])
    ax.plot(days.index, median , 'sr', label='MEDIAN')
    ax.errorbar(days.index, mean ,yerr=std, fmt='sk', ecolor='red', label="MEAN-STD")
    ax.set_title(f"{channel} SEEPAGE")
    ax.set_ylabel("DAILY SEEPAGE (mm)")

    #scale factor to adjust plot to align with cell columns
    dx = ax.get_xbound()
    change = (dx[1] - dx[0]) / (2*len(days.index) - 2)
    ax.set_xbound(dx[0]-change, dx[1]+change)

    fig.set_size_inches(w=13.33, h=9.3)
    if show:
        plt.show()
    else:
        plt.savefig(f"../out/seepage/{channel}-{top_reg_name}.png")
        plt.close()

    ####### BAR CHART #####
    for i, v in enumerate(bins[-3:]):

        days.iloc[-(i + 1), ::-1].transpose().plot.bar(subplots=True, rot=25, legend=False)
        locs, labels = plt.xticks()

        for i, l in enumerate(labels):
            s = l.get_text().split('-')
            s = f"{all_regs.loc[all_regs.OBJECT_NO==int(s[0]), 'OBJECT_NAME'].iloc[0]}-{s[1].split('_')[0]}"
            l.set_text(s)
            labels[i] = l

        plt.xticks(locs, labels)

        plt.gcf().suptitle(f"{channel}-{top_reg_name} SEEPAGE ({v.strftime('%D')})")
        plt.gcf().set_size_inches(w=13.33, h=9.3)
        if show:
            plt.show()
        else:
            plt.savefig(f"../out/seepage/{channel}-{top_reg_name}_d{i}.png")
            plt.close()

    #3 day average

#####TABLES
#change column object_nos to name
usl = least_squares.loc[:, least_squares.columns.str.contains("USL")].iloc[-3:, :].transpose()
usl = pd.concat([seepage.loc[:, seepage.columns.str.contains("USL")].iloc[-3:, :].transpose()*1000, usl], join='inner', axis=1)

#rename columns
col = [f"Seepage - {i.strftime('%D')}" for i in bins[-3:]]+[f"r2 -{i.strftime('%D')}" for i in bins[-3:]]
usl.columns = col
usl = usl.iloc[:, [0, 3, 1, 4, 2, 5]]
usl.to_csv("USL.csv")

#change column object_nos to name
dsl = least_squares.loc[:, least_squares.columns.str.contains("DSL")].iloc[-3:, :].transpose()
dsl = pd.concat([seepage.loc[:, seepage.columns.str.contains("DSL")].iloc[-3:, :].transpose()*1000, dsl], join='inner', axis=1)

dsl.columns = col
dsl = dsl.iloc[:, [0, 3, 1, 4, 2, 5]]

dsl.to_csv("DSL.csv")
print()
