"""A water balance tool.
Takes LVBC (described by a linkage table) and performs a water balance on them """

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import topology_linker.res.FGinvestigation.fginvestigation.extraction as ext
from constants import DS_METER, DS_ESC, US_REG
from utils import get_linked_ojects, subtract_one_month, fix_resets, get_manual_meter
import matplotlib.pyplot as plt
import scipy.integrate as integrate
import pandas as pd

number_of_months = 1
export = True
show = False
use_rtu_as_source = True
max_source_discrepancy = 1.3 #ratio

if export: fh = open("../out/WATER_USAGE.csv", 'w', newline='')

#NORMALLY this df would be fetched by pulling down the linked table from SQL
link_df = pd.read_csv("../out/LINKED.csv", usecols=['OBJECT_NO',  'LINK_OBJECT_NO', 'LINK_DESCRIPTION', 'POSITION'],
                 dtype={'OBJECT_NO':str,  'LINK_OBJECT_NO':str, 'LINK_DESCRIPTION':str})

upstream_point = '29355' #scotts
downstream_point = '65041' #end of the line

print("collecting whole topology...")
link, link_list = get_linked_ojects(object_A=upstream_point,
                                        object_B=downstream_point,
                                        source=link_df)
print("done.")
regs = [upstream_point] + link.get_all_of_desc(desc = [US_REG]) + [downstream_point]

lc = link.get_last_child()
if export: fh.writelines(
    f"Water balance between {link.object_name} ({link.object_no}) and {lc.object_name} ({lc.object_no})\n")
out_df = pd.DataFrame()

last = False
for index, pool in enumerate(regs[:-1]):
    upstream_point = pool
    downstream_point = regs[index + 1]

    period_end = (pd.datetime(year=2019, month=10, day=9, hour=00))
    period_start = (pd.datetime(year=2019, month=11, day=9, hour=00))

    last = True if index == len(regs) - 2 else last # -2 because we are enumerating up to index - 1 of regs

    link, link_list = get_linked_ojects(object_A=upstream_point,
                                        object_B=downstream_point,
                                        source=link_df)

    print(link)
    link_list = [upstream_point] + link.get_all_of_desc(desc = [DS_METER, DS_ESC]) + [downstream_point]

    for _ in range(number_of_months):

        query = (f"SELECT OBJECT_NO, SC_EVENT_LOG.EVENT_TIME, SC_EVENT_LOG.EVENT_VALUE, TAG_NAME "
                 f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
                 f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
                 f" WHERE "
                 f" OBJECT_NO IN {tuple(link_list)} AND TAG_NAME IN ('FLOW_ACU_SR', 'FLOW_VAL')"
                 f" AND EVENT_TIME > TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                 f"AND EVENT_TIME < TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                 f" ORDER BY EVENT_TIME")

        obj_data = ext.get_data_ordb(query)
        obj_data = obj_data.astype({"OBJECT_NO": str, "EVENT_VALUE": float})
        obj_data = obj_data.set_index("OBJECT_NO")

        delivered = 0
        meters_not_checked = set()
        meters = 0
        recovered = 0
        meters_neg = []

        for link_obj in link_list:
            if link_obj in obj_data.index:
                df = obj_data.loc[link_obj]
                if isinstance(df, pd.DataFrame):
                    #collect RTU data (primary source)
                    RTU_df = df.loc[df["TAG_NAME"] == 'FLOW_ACU_SR'].sort_values(by=["EVENT_TIME"])
                    if show: ax = RTU_df.plot(x="EVENT_TIME", y="EVENT_VALUE", label="RTU_SOURCE")
                    RTU_df = fix_resets(RTU_df)
                    if show: RTU_df.plot(x="EVENT_TIME", y ="EVENT_VALUE", ax=ax, label="RTU_INTEGRAL")
                    RTU = (RTU_df["EVENT_VALUE"].max() - RTU_df.head(1)["EVENT_VALUE"].iat[0]) * 1000

                    #calculate INTEGRAL (secondary source)
                    FLOW_df = df.loc[df["TAG_NAME"] == 'FLOW_VAL'].sort_values(by=["EVENT_TIME"])
                    neg = FLOW_df["EVENT_VALUE"].values < 0.0
                    if neg.any(): meters_neg.append(link_obj)
                    #FLOW_df.loc[FLOW_df["EVENT_VALUE"] < 0.0, "EVENT_VALUE"] = 0.0
                    first = FLOW_df.head(1)["EVENT_TIME"].values[0]
                    time = [pd.Timedelta(td - first).total_seconds() for td in FLOW_df["EVENT_TIME"].values]
                    FLOW_df["EVENT_VALUE"] = FLOW_df["EVENT_VALUE"] / 86.4
                    integral = integrate.cumtrapz(y=FLOW_df["EVENT_VALUE"].values, x=time) / 1000
                    integral = pd.Series(data=integral, index=FLOW_df["EVENT_TIME"].values[1:]).transpose()
                    if show: integral.plot(ax=ax, label="INTEGRAL")
                    integral = max(integral) * 1000

                    if show:
                        plt.title(link_obj)
                        ax2 = ax.twinx()
                        FLOW_df.plot(x="EVENT_TIME", y="EVENT_VALUE", label="FLOW", ax=ax2, color="#9467bd", alpha=0.2)
                        ax2.set_ylabel("FLOW (m3/s)")
                        box = ax.get_position()
                        ax.set_position([box.x0, box.y0 + box.height * 0.1,
                                         box.width, box.height * 0.9])
                        ax_ln, ax_lb = ax.get_legend_handles_labels()
                        ax_lb[1] = ax_lb[1] + f"\n= {RTU/ 1000:.1f} ML"
                        ax_lb[2] = ax_lb[2] + f"\n= {integral / 1000:.1f} ML"
                        ax2_ln, ax2_lb = ax2.get_legend_handles_labels()
                        ax.legend(ax_ln+ax2_ln, ax_lb+ax2_lb, loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=4)
                        ax.set_xlabel("")
                        ax.set_ylabel("CUMULATIVE FLOW (ML)")
                        ax2.get_legend().remove()
                        plt.show()
                    else: plt.close()

                    if True:#RTU == 0.0 or integral == 0.0 or RTU / integral < max_source_discrepancy:
                        volume = RTU
                        RTU = True
                    else:
                        volume = integral
                        RTU = False

                    if link_obj == upstream_point:
                        link_obj = link_obj + " (IN)"
                        IN = volume
                    elif link_obj == downstream_point:
                        # if last:
                        #     OUT = 0.0
                        #     delivered += volume
                        # else:
                        link_obj = link_obj + " (OUT)"
                        OUT = volume
                    else:
                        delivered += volume
                else:
                    volume = df["EVENT_VALUE"]

                print(f"{link_obj} volume = {volume / 1000:.1f}ML using {'RTU' if RTU else 'INTEGRAL'}")
            else:
                # meter data not found in Events, maybe these are un telemetry or un metered flows
                volume, date = get_manual_meter(link_obj, period_end)

                if volume is None:
                    print(f"{link_obj} - No data")
                    meters_not_checked.add(link_obj)
                else:
                    print(f"{link_obj} volume = {volume:.1f}ML using MANUAL @ {date}")
                    delivered += volume * 1000
                    recovered += volume

            meters += 1


        balance = IN - delivered - OUT

        out = {
            "pool":[pool],
            "pool_name":[link.object_name],
            "dates": [f"{period_end.strftime('%Y-%m-%d')} - {period_start.strftime('%Y-%m-%d')}"],
            "IN (ML)": [f"{IN / 1000:.1f}"],
            "delivered (ML)" : [f"{delivered / 1000:.1f}"],
            "balance (ML)" : [f"{balance / 1000:.1f}"],
            f"rubicon_PE (%)" : [f"{((delivered + OUT) / IN) * 100:.1f}"],
            f"MI_PE (%)" : [f"{(delivered / (IN - OUT)) * 100:.1f}"],
            "manually_read (ML)": [recovered],
            "total meters": [meters],
            "meters not used": [f"{meters_not_checked}"],
            "negative meters": [meters_neg]
        }

        for key, line in out.items():
            print(key, line)

        out = pd.DataFrame(out)
        out_df = pd.concat([out_df, out], ignore_index=True)

        print(f"{len(meters_neg)}/{meters} are negative")
        print()

        period_start = subtract_one_month(period_start)
        period_end = subtract_one_month(period_end)

if export: out_df.to_csv(path_or_buf=fh, index=False)
if export: fh.close()




