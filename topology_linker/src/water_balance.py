import topology_linker.res.FGinvestigation.fginvestigation.extraction as ext
from constants import DS_METER, DS_ESC
from utils import get_linked_ojects, subtract_one_month, fix_resets
import matplotlib.pyplot as plt
import scipy.integrate as integrate
import pandas as pd

upstream_point = '47528'
downstream_point = '67507'
period_end = (pd.datetime(year=2019, month=10, day=9, hour=00))
period_start = (pd.datetime(year=2019, month=11, day=9, hour=00))
#period_start = (pd.datetime.now() - pd.Timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
export = False
use_rtu_as_source = False
use_rtu_as_source = True
if export: fh = open("../out/WATER_USAGE.csv", 'w', newline='')


#NORMALLY this df would be fetched by pulling down the linked table from SQL
df = pd.read_csv("../out/LINKED.csv", usecols=['OBJECT_NO',  'LINK_OBJECT_NO', 'LINK_DESCRIPTION'], dtype={'OBJECT_NO':str,  'LINK_OBJECT_NO':str, 'LINK_DESCRIPTION':str})

link, link_list = get_linked_ojects(object_A=upstream_point,
                                        object_B=downstream_point,
                                        source=df)
print(link)
# print(link_list)
# link_list = [link.object_no] + [child.object_no for child in link.children]
# print(link_list)
link_list = [upstream_point] + link.get_all_of_desc(desc = [DS_METER, DS_ESC]) + [downstream_point]
print(link_list)


lc = link.get_last_child()
if export: fh.writelines( f"Water balance between {link.object_name} ({link.object_no}) and {lc.object_name} ({lc.object_no})\n")
# link_list = ['31075', '67522', '65020', '141752', '31109']
# print(link_list)

# query = (f"SELECT TAG_ID, EVENT_TIME, EVENT_VALUE "
#                              f" FROM SC_EVENT_LOG"
#                              f" WHERE TAG_ID IN"
#                              f" ( SELECT TAG_ID FROM SC_TAG WHERE"
#                              f" OBJECT_NO IN {tuple(link_list)} AND TAG_NAME = 'FLOW_VAL')"
#                              f" AND EVENT_TIME > TO_DATE('{period_end}', 'YYYY-MM-DD HH24:MI:SS')"
#                              f"AND EVENT_TIME < TO_DATE('{period_start}', 'YYYY-MM-DD HH24:MI:SS')"
#                              f" ORDER BY EVENT_TIME DESC")

out_df = pd.DataFrame(columns=["dates","delivered (ML)","balance (ML)", "rubicon_PE (per)", "MI_PE (per)", "meters not used"])

for _ in range(1):

    if use_rtu_as_source:
        flow_data = "FLOW_ACU_SR"
    else:
        flow_data = "FLOW_VAL"

    query = (f"SELECT OBJECT_NO, SC_EVENT_LOG.EVENT_TIME, SC_EVENT_LOG.EVENT_VALUE "
             f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
             f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
             f" WHERE "
             f" OBJECT_NO IN {tuple(link_list)} AND TAG_NAME = '{flow_data}'"
             f" AND EVENT_TIME > TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f"AND EVENT_TIME < TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f" ORDER BY EVENT_TIME")

    obj_data = ext.get_data_ordb(query)
    obj_data = obj_data.astype({"OBJECT_NO": str, "EVENT_VALUE": float})
    obj_data = obj_data.set_index("OBJECT_NO")

    delivered = 0
    meters_not_checked = set()

    for link_obj in link_list:
        if link_obj in obj_data.index:
            df = obj_data.loc[link_obj]
            if isinstance(df, pd.DataFrame):
                if use_rtu_as_source:
                    # USING METER TOTALALISER
                    df_old = df.sort_values(by=["EVENT_TIME"])
                    df = fix_resets(df_old)
                    p = df["EVENT_VALUE"].values# - df_old["EVENT_VALUE"].values
                    ax = df["EVENT_VALUE"].plot(label="ADJUSTED")
                    df_old["EVENT_VALUE"].plot(ax = ax, label="OLD")
                    plt.legend()
                    plt.title(link_obj)
                    plt.show()
                    integral = (df["EVENT_VALUE"].max() - df.head(1)["EVENT_VALUE"].iat[0]) * 1000

                else:
                    # USING TRAPEZOIDAL INTEGRATION OF FLOW
                    df = df.sort_values(by=["EVENT_TIME"])
                    first = df.head(1)["EVENT_TIME"].values[0]
                    time = [pd.Timedelta(td - first).total_seconds() for td in df["EVENT_TIME"].values]
                    df["EVENT_VALUE"] = df["EVENT_VALUE"] / 86.4
                    integral = integrate.trapz(y=df["EVENT_VALUE"].values, x=time)

                if link_obj == upstream_point:
                    link_obj = link_obj + " (IN)"
                    IN = integral
                elif link_obj == downstream_point:
                    link_obj = link_obj + " (OUT)"
                    OUT = integral
                else:
                    delivered += integral
            else:
                integral = df["EVENT_VALUE"]
            print(f"{link_obj} integral = {integral / 1000:.1f}ML")
        else:
            print(f"{link_obj} - No data")
            meters_not_checked.add(link_obj)

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

if export: out_df.to_csv(path_or_buf=fh, index=False)
if export: fh.close()




