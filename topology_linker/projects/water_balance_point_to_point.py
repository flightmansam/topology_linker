"""A water balance tool.
Takes two points on a network (described by a linkage table) and performs a water balance on them """

# TODO: Rewrite this file to use the utils.volume() function to minimise code duplication

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import fginvestigation.extraction as ext
from topology_linker.src.constants import DS_METER, DS_ESC
from topology_linker.src.utils import get_linked_ojects, subtract_one_month, fix_resets
import matplotlib.pyplot as plt
import scipy.integrate as integrate
import pandas as pd

upstream_point = '30239'
downstream_point = '30460'
period_end = (pd.datetime(year=2019, month=10, day=9, hour=00))
#period_end = pd.datetime.now()- pd.Timedelta(days=7)
period_start = (pd.datetime(year=2019, month=11, day=9, hour=00))
#period_start = pd.datetime.now()
export = False
show = False
use_rtu_as_source = True
max_source_discrepancy = 1.3 #ratio

if export: fh = open("../out/WATER_USAGE.csv", 'w', newline='')

#NORMALLY this df would be fetched by pulling down the linked table from SQL
df = pd.read_csv("../out/LINKED.csv", usecols=['OBJECT_NO',  'LINK_OBJECT_NO', 'LINK_DESCRIPTION', 'POSITION'],
                 dtype={'OBJECT_NO':str,  'LINK_OBJECT_NO':str, 'LINK_DESCRIPTION':str})

link, link_list = get_linked_ojects(object_A=upstream_point,
                                        object_B=downstream_point,
                                        source=df)
print(link)
link_list = [upstream_point] + link.get_all_of_desc(desc = [DS_METER, DS_ESC]) + [downstream_point]
print(link_list) #these are all the nodes we are going to get sensor data from (if applicable)

lc = link.get_last_child()
if export: fh.writelines( f"Water balance between {link.object_name} ({link.object_no}) and {lc.object_name} ({lc.object_no})\n")

out_df = pd.DataFrame(columns=["dates","delivered (ML)","balance (ML)", "rubicon_PE (per)", "MI_PE (per)", "meters not used"])

for _ in range(1):

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
                FLOW_df.loc[FLOW_df["EVENT_VALUE"] < 0.0, "EVENT_VALUE"] = 0.0
                first = FLOW_df.head(1)["EVENT_TIME"].values[0]
                time = [pd.Timedelta(td - first).total_seconds() for td in FLOW_df["EVENT_TIME"].values]
                FLOW_df["EVENT_VALUE"] = FLOW_df["EVENT_VALUE"] / 86.4
                integral = integrate.cumtrapz(y=FLOW_df["EVENT_VALUE"].values, x=time) / 1000
                integral = pd.Series(data=integral, index=FLOW_df["EVENT_TIME"].values[:-1]).transpose()
                if show: integral.plot(ax=ax, label="INTEGRAL")
                integral = max(integral) * 1000

                plt.legend()
                plt.title(link_obj)

                if show: FLOW_df.plot(x="EVENT_TIME", y="EVENT_VALUE", label="FLOW")

                if show: plt.show()
                else: plt.close()


                if False:#RTU == 0.0 or integral == 0.0 or RTU / integral < max_source_discrepancy:
                    volume = RTU
                    RTU = True
                else:
                    volume = integral
                    RTU = False

                if link_obj == upstream_point:
                    link_obj = link_obj + " (IN)"
                    IN = volume
                elif link_obj == downstream_point:
                    link_obj = link_obj + " (OUT)"
                    OUT = volume
                else:
                    delivered += volume
            else:
                volume = df["EVENT_VALUE"]
            print(f"{link_obj} volume = {volume / 1000:.1f}ML using {'RTU' if RTU else 'INTEGRAL'}")
        else:
            print(f"{link_obj} - No data")
            meters_not_checked.add(link_obj)

        meters += 1


    balance = IN - delivered - OUT

    out = {
        "dates": [f"{period_end.strftime('%Y-%m-%d')} - {period_start.strftime('%Y-%m-%d')}"],
        "delivered (ML)" : [f"{delivered / 1000:.1f}"],
        "balance (ML)" : [f"{balance / 1000:.1f}"],
        "rubicon_PE (per)" : [f"{((delivered + OUT) / IN) * 100:.1f}"],
        "MI_PE (per)" : [f"{(delivered / (IN - OUT)) * 100:.1f}"],
        "meters not used": [f"{meters_not_checked}"]
    }

    for key, line in out.items():
        print(key, line)
    print()

    out = pd.DataFrame(out)
    out_df = pd.concat([out_df, out], ignore_index=True)

    period_start = subtract_one_month(period_start)
    period_end = subtract_one_month(period_end)

    print(f"{len(meters_neg)}/{meters} are negative")
    print(meters_neg)

if export: out_df.to_csv(path_or_buf=fh, index=False)
if export: fh.close()




