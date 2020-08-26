"""A seepage calculator tool.
Takes a pool (described by a linkage table) and finds when flow is zero. It then calculates statistics on those zero flows:
    * Duration, frequency
    * Seepage gradient"""

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import pandas as pd

import fginvestigation.extraction as ext
from topology_linker.src.utils import get_reachFROMreg, get_name
from scipy import stats

def seepage(pools:pd.DataFrame, period_start:pd.datetime, period_end:pd.datetime, days:int):

    #split the dates for the data into DAYS
    bins = pd.date_range(start=period_start, end=period_end, freq=f'{days}D')
    #least_squares = pd.DataFrame(index = bins)
    seepage = pd.DataFrame(columns=["TIME", "OBJECT_NO", "POOL", "SEEPAGE", "R2", "N"])

    for reg, next_reg in zip(pools.reg, pools.next_reg):
        reg_name = get_name(object_no=reg)
        print(reg_name)
        # next_reg_name = get_name(object_no=next_reg)

        #get heights of the regs
        query =  (f"SELECT * FROM ("
                  f"    SELECT ev.EVENT_TIME, OBJECT_NO,  tag.TAG_NAME, ev.EVENT_VALUE "
                  f"    FROM SC_EVENT_LOG ev INNER JOIN SC_TAG tag"
                  f"    ON ev.TAG_ID = tag.TAG_ID"
                  f" WHERE "
                  f"    OBJECT_NO in ('{reg}') AND TAG_NAME in ('DSL_VAL')"
                  f"    AND EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                  f"    AND EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS'))"
                  f" UNION ALL"
                  f" SELECT * FROM("
                  f"    SELECT ev.EVENT_TIME, OBJECT_NO,  tag.TAG_NAME, ev.EVENT_VALUE "
                  f"    FROM SC_EVENT_LOG ev INNER JOIN SC_TAG tag"
                  f"    ON ev.TAG_ID = tag.TAG_ID"
                  f" WHERE "
                  f"    OBJECT_NO in ('{next_reg}') AND TAG_NAME in ('USL_VAL')"
                  f"    AND EVENT_TIME >= TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                  f"    AND EVENT_TIME <= TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS'))")
        heights = ext.get_data_ordb(query)

        #days = pd.DataFrame(index=bins)

        for r in [reg, next_reg]: #each reg that has level data
            event_data = heights.loc[heights.OBJECT_NO == r, ["EVENT_TIME", "TAG_NAME", "EVENT_VALUE"]]
            if not event_data.empty:
                event_data = event_data.drop_duplicates().pivot(index="EVENT_TIME", columns="TAG_NAME")["EVENT_VALUE"]
                for col in event_data.columns:
                    for start, end in zip(bins, bins.shift(1)):
                        daily_drop = event_data.loc[(event_data.index > start) & (event_data.index < end), col].dropna()

                        if len(daily_drop) > 1:
                            n = len(daily_drop)
                            xs =(daily_drop.index - daily_drop.index[0]).total_seconds()
                            m, b, r_value, p_value, std_err = stats.linregress(xs, daily_drop.to_numpy())

                            res = r_value * r_value
                            daily_drop = -1 * m * 60*60*24*1000 #seconds in a day, m to mm
                            # days.loc[start, f"{r}-{col}"] = daily_drop
                            data = [start, r, reg, daily_drop, res, n]
                            seepage.loc[len(seepage)+1, :] = data

                            #least_squares.loc[start, f"{r}-{col}"] = res

        #fig, ax = plt.subplots(1, 1, constrained_layout=True)

        # import itertools
        # markers = itertools.cycle(['H', '^', 'v', 'd', '3', '.', '1', '_'])
        # days*=1000 #convert to mm
        # days.plot(ax=ax, style='x-', legend=False)
        #
        # for line in ax.get_lines():
        #     line.set_marker(next(markers))
        # handles, labels = ax.get_legend_handles_labels()
        #
        # labels = [f"{l} - {names[int(l.split('-')[0])]}" for l in labels]
        # ax.legend(handles, labels, loc=8, ncol=4)
        # mean, median, std = days.mean(axis=1), days.median(axis=1), days.std(axis=1)
        #
        # ax.set_xticks([],[])
        # ax.table(cellText=list(map(lambda a: [f"{i:.2f}" for i in a], [mean, median, std])),
        #          rowLabels=["MEAN", "MEDIAN", "STD"],
        #          colLabels=[day.strftime("%D") for day in days.index])
        # ax.plot(days.index, median , 'sr', label='MEDIAN')
        # ax.errorbar(days.index, mean ,yerr=std, fmt='sk', ecolor='red', label="MEAN-STD")
        # ax.set_title(f"{channel} SEEPAGE")
        # ax.set_ylabel("DAILY SEEPAGE (mm)")
        #
        # #scale factor to adjust plot to align with cell columns
        # dx = ax.get_xbound()
        # change = (dx[1] - dx[0]) / (2*len(days.index) - 2)
        # ax.set_xbound(dx[0]-change, dx[1]+change)
        #
        # fig.set_size_inches(w=13.33, h=9.3)
        # if save:
        #     plt.savefig(f"../out/seepage/{channel}-{reg_name}.png")
        #
        # if show:
        #     plt.show()
        #
        # plt.close()
        #
        # ####### BAR CHART #####
        # for i, v in enumerate(bins[-3:]):
        #
        #     days.iloc[-(i + 1), ::-1].transpose().plot.bar(subplots=True, rot=25, legend=False)
        #     locs, labels = plt.xticks()
        #
        #     for i, l in enumerate(labels):
        #         s = l.get_text().split('-')[0]
        #         s = f"{s} - {names[int(s)]}"
        #         l.set_text(s)
        #         labels[i] = l
        #
        #     plt.xticks(locs, labels)
        #
        #     plt.gcf().suptitle(f"{channel}-{reg_name} SEEPAGE ({v.strftime('%D')})")
        #     plt.gcf().set_size_inches(w=13.33, h=9.3)
        #
        #     if save:
        #         plt.savefig(f"../out/seepage/{channel}-{reg_name}_d{i}.png")
        #
        #     if show:
        #         plt.show()
        #
        #     plt.close()
    return seepage
        #3 day average

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
    # dsl = pd.concat([seepage.loc[:, seepage.columns.str.contains("DSL")].iloc[-3:, :].transpose()*1000, dsl], join='inner', axis=1)
    #
    # dsl.columns = col
    # dsl = dsl.iloc[:, [0, 3, 1, 4, 2, 5]]
    #
    # dsl.to_csv("DSL.csv")
    # print()

if __name__ == "__main__":
    period_end = pd.datetime(year=2020, month=6, day=1, hour=12)
    #period_start = pd.datetime(year=2020, month=5, day=20, hour=9)
    period_start = period_end - pd.Timedelta(days=3)  # 24Hr
    pools = {
        "reg":[8309],
        "next_reg":[8315]
    }
    pools = pd.DataFrame(pools)
    #pools = pd.read_csv("pools.csv")
    seepage(pools, period_start, period_end, days=3)

# object_no = [reg for reg in network]
# all_totaliser_data = ("ORACLE QUERY OF EVENTS DB WHERE OBJECT_NO in object_no AND TAG_NAME in ('FLOW_ACU_SR', 'USL_VAL', 'DSL_VAL'")
# for reg in all_totaliser_data.OBJECT_NO.unique():
#     candidates = find periods where totaliser hasnt changed and have greater than 6 records of height
#
# for reg in all_totaliser_data.OBJECT_NO.unique():
#     next_reg = topology_linker.get_next_reg().OBJECT_NO
#     times = find times in reg and nex_reg candidates that overlap
#
#     for event_start, event_end in times:
#         do seepage measurement between reg, next_reg for event_start, event_end