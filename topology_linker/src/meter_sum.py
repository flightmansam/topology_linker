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

export = True
show = False

if export: fh = open("../out/METER_USAGE.csv", 'w', newline='')

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
print(link)
link_list = link.get_all_of_desc(desc = [DS_METER, DS_ESC])
lc = link.get_last_child()

period_end = (pd.datetime(year=2020, month=1, day=3, hour=00))
period_start = (pd.datetime(year=2020, month=1, day=4, hour=00))

if export: fh.writelines([
    f"Meter data between {link.object_name} ({link.object_no}) and {lc.object_name} ({lc.object_no})\n",
    f"from: {period_end.strftime('%Y-%m-%d %H:%M')} to {period_start.strftime('%Y-%m-%d %H:%M')}\n"])
out_df = pd.DataFrame()



query = (f"SELECT OBJECT_NO, SC_EVENT_LOG.EVENT_TIME, SC_EVENT_LOG.EVENT_VALUE, TAG_NAME "
         f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
         f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
         f" WHERE "
         f" OBJECT_NO IN {tuple([l.object_no for l in link_list])} AND TAG_NAME IN ('FLOW_ACU_SR', 'FLOW_VAL')"
         f" AND EVENT_TIME > TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
         f"AND EVENT_TIME < TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
         f" ORDER BY EVENT_TIME")

obj_data = ext.get_data_ordb(query)
obj_data = obj_data.astype({"OBJECT_NO": str, "EVENT_VALUE": float})
obj_data = obj_data.set_index("OBJECT_NO")

delivered = 0
meters_not_checked = set()
manual_meters = set()
meters = 0
recovered = 0
meters_neg = []

out = {
    "asset_code": [],
    "object_id": [],
    "RTU_totaliser (ML)":[],
    "flow_integral (ML)":[],
    "manual_reading (ML)": [],
}

for link_obj in link_list:
    out["asset_code"].append(link_obj.object_name)
    out["object_id"].append(link_obj.object_no)
    link_obj = link_obj.object_no
    print(link_obj)
    if link_obj in obj_data.index:
        df = obj_data.loc[link_obj]
        if isinstance(df, pd.DataFrame):
            #collect RTU data (primary source)
            RTU_df = df.loc[df["TAG_NAME"] == 'FLOW_ACU_SR'].sort_values(by=["EVENT_TIME"])
            if show: ax = RTU_df.plot(x="EVENT_TIME", y="EVENT_VALUE", label="RTU_SOURCE")
            RTU_df = fix_resets(RTU_df)
            if show: RTU_df.plot(x="EVENT_TIME", y ="EVENT_VALUE", ax=ax, label="RTU_INTEGRAL")
            RTU = 0.0 if RTU_df.empty else (RTU_df["EVENT_VALUE"].max() - RTU_df.head(1)["EVENT_VALUE"].iat[0]) * 1000

            #calculate INTEGRAL (secondary source)
            FLOW_df = df.loc[df["TAG_NAME"] == 'FLOW_VAL'].sort_values(by=["EVENT_TIME"])
            neg = FLOW_df["EVENT_VALUE"].values < 0.0
            if neg.any(): meters_neg.append(link_obj)
            if FLOW_df.empty:
                integral = [0.0]
            else:
                #FLOW_df.loc[FLOW_df["EVENT_VALUE"] < 0.0, "EVENT_VALUE"] = 0.0
                first = FLOW_df.head(1)["EVENT_TIME"].values[0]
                time = [pd.Timedelta(td - first).total_seconds() for td in FLOW_df["EVENT_TIME"].values]
                FLOW_df["EVENT_VALUE"] = FLOW_df["EVENT_VALUE"] / 86.4
                integral = integrate.cumtrapz(y=FLOW_df["EVENT_VALUE"].values, x=time) / 1000
                integral = pd.Series(data=integral, index=FLOW_df["EVENT_TIME"].values[1:]).transpose()
                if integral.empty:
                    integral = [0.0]
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

            out["RTU_totaliser (ML)"].append(RTU / 1000)
            out["flow_integral (ML)"].append(integral / 1000)
            out["manual_reading (ML)"].append("")

            # if True:#RTU == 0.0 or integral == 0.0 or RTU / integral < max_source_discrepancy:
            #     volume = RTU
            #     RTU = True
            # else:
            #     volume = integral
            #     RTU = False
            #
            # if link_obj == upstream_point:
            #     link_obj = link_obj + " (IN)"
            #     IN = volume
            # elif link_obj == downstream_point:
            #     # if last:
            #     #     OUT = 0.0
            #     #     delivered += volume
            #     # else:
            #     link_obj = link_obj + " (OUT)"
            #     OUT = volume
            # else:
            #     delivered += volume
        else:
            print("YIKES")
            volume = df["EVENT_VALUE"]
            out["RTU_totaliser (ML)"].append(volume / 1000)
            out["flow_integral (ML)"].append(volume / 1000)
            out["manual_reading (ML)"].append("")

    else:
        # meter data not found in Events, maybe these are un telemetry or un metered flows
        volume, date = get_manual_meter(link_obj, period_end)

        if volume is None:
            print(f"{link_obj} - No data")
            meters_not_checked.add(link_obj)
            out["RTU_totaliser (ML)"].append("")
            out["flow_integral (ML)"].append("")
            out["manual_reading (ML)"].append("")
        else:
            print(f"{link_obj} volume = {volume:.1f}ML using MANUAL @ {date}")
            out["RTU_totaliser (ML)"].append("")
            out["flow_integral (ML)"].append("")
            out["manual_reading (ML)"].append(volume)
            manual_meters.add(link_obj)

    meters += 1

# for key, line in out.items():
#     print(key, line)

out = pd.DataFrame(out)
out_df = pd.concat([out_df, out], ignore_index=True)
print(out_df.to_string())
print()

if export:
    out_df.to_csv(path_or_buf=fh, index=False)
    fh.writelines([f"total meters in pool: {meters}\n",
                   f"meters not used ({len(meters_not_checked)}): {meters_not_checked}\n",
                   f"negative meters ({len(meters_neg)}): {meters_neg}\n",
                   f"manually read ({len(manual_meters)}): {manual_meters}\n",
                   f"time of data collection: {pd.datetime.now().strftime('%Y-%m-%d %H:%M')}\n"])
    fh.close()




