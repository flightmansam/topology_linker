import fginvestigation.extraction as ext
from topology_linker.src.utils import get_linked_ojects, fix_resets, get_manual_meter, query
import scipy.integrate as integrate
import pandas as pd
import matplotlib.pyplot as plt

meters = ("MITCHELLS",
          "M2362B/1",
          "M2364B/1",
          "B18/1",
          "B18A/P",
          "B18B/1",
          "CH7BN-1",
          "M2364C/1",
          "OT TABBITA",
          "WARBURN SPLIT")

object_IDS= query(meters)


# query = ("SELECT *"
#          f" FROM OBJECT_ATTR_VALUE"
#          f" WHERE OBJECT_NO = '31210'"
#          f" FETCH NEXT 10000 ROWS ONLY")
#
#obj_data = ext.get_data_ordb(query)
#
period_start = (pd.datetime(year=2019, month=11, day=9, hour=00))
# asset_code = 'RG-2-698'
#
# query = (
#         f" SELECT *"
#         f"    FROM TAGS"
#         f" WHERE OBJECT_NO = ( select OBJECT_NO from Objects WHERE ASSET_CODE = '{asset_code}') "
#     )
# df = ext.get_data_sql(query)
#
# m = get_manual_meter('61320', period_start)
#
# query = (
#         f"Select *"
#         f" From SC_EVENT_LOG"
#         f" WHERE TAG_ID IN ("
#         f" SELECT TAG_ID FROM SC_TAG WHERE"
#         f" OBJECT_NO IN ('64783')"
#         f" AND TAG_NAME = 'FLOW_ACU_SR')"
#         f" AND EVENT_TIME > TO_DATE('{period_start}', 'YYYY-MM-DD HH24:MI:SS')"
#         f" ORDER BY EVENT_TIME"
# )
#
# # query = (
# #         f"Select *"
# #         f" From SC_TAG"
# #         f" WHERE OBJECT_NO IN ('64615')"
# #
# # )
# obj_data = ext.get_data_ordb(query)
# obj_data.set_index("EVENT_TIME", inplace=True)
# ax = obj_data["EVENT_VALUE"].plot(label="OLD")
#
# print()
#
# df = fix_resets(obj_data)
# df.set_index("EVENT_TIME", inplace=True)
# df["EVENT_VALUE"].plot(ax=ax, label="NEW")
# plt.legend()
# plt.show()
# from utils import query
# obj_data = query('219201', by='OBJECT_NO')
#
# print(obj_data.to_string())
#
# df = pd.read_csv("../out/LINKED.csv", usecols=['OBJECT_NO',  'LINK_OBJECT_NO', 'LINK_DESCRIPTION'], dtype={'OBJECT_NO':str,  'LINK_OBJECT_NO':str, 'LINK_DESCRIPTION':str})
#
# up = '30239'
# down = '30460'

# link, link_list = get_linked_ojects(object_A=up,
#                                     object_B=down,
#                                     source=df)
# print(link)
# print(link_list)
# link_list = [link.object_no] + [child.object_no for child in link.children]
# print(link_list)
# link_list = ['29355', '']
# # print(link_list)
#
# query = (
#         f"Select * "
#         f" From SC_TAG"
#         f" Where OBJECT_NO IN {tuple(link_list)}"
#
# )
#
# q = ext.get_data_ordb(query)

period_end = (pd.datetime(year=2019, month=10, day=1, hour=00, second=43, microsecond=432)).strftime("%Y-%m-%d %H:%M:%S")
period_start = (pd.datetime(year=2019, month=11, day=30, hour=00))#.strftime("%Y-%m-%d %H:%M:%S")
# #period_start = (pd.datetime.now() - pd.Timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
# import io
# import requests
# url = f"http://www.bom.gov.au/watl/eto/tables/nsw/griffith_airport/griffith_airport-{period_start.year}{period_start.month}.csv"
# s = requests.get(url).content
# prev_month = pd.read_csv(io.StringIO(s.decode('utf-8', errors='ignore')))
# prev_month = prev_month[prev_month.iloc[:, 0] == "GRIFFITH AIRPORT"]
# prev_month = prev_month.set_index([prev_month.iloc[:, 1]])
# prev_month = pd.to_numeric(prev_month[prev_month.index >= "16/11/2019"].iloc[:, 2]).sum()


# query = (f"SELECT EVENT_TIME, TAG_DESC, EVENT_VALUE "
#                              f" FROM SC_EVENT_LOG"
#                              f" WHERE TAG_ID IN"
#                              f" ( SELECT TAG_ID FROM SC_TAG WHERE"
#                              f" OBJECT_NO IN {tuple(link_list)} AND TAG_NAME = 'FLOW_VAL')"
#                              f" AND EVENT_TIME > TO_DATE('{period_end}', 'YYYY-MM-DD HH24:MI:SS')"
#                              f" AND EVENT_TIME < TO_DATE('{period_start}', 'YYYY-MM-DD HH24:MI:SS')"
#                              f" ORDER BY EVENT_TIME DESC")


#q = ext.get_data_ordb(query)


query1 = (f"SELECT SC_EVENT_LOG.EVENT_TIME, SC_TAG.TAG_DESC, SC_EVENT_LOG.EVENT_VALUE, OBJECT_NO"
                             f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
                             f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
                             f" WHERE " 
                             f" OBJECT_NO in ('26025') AND TAG_DESC LIKE 'Gate _ Width'"
                             f" AND EVENT_TIME > TO_DATE('{period_end}', 'YYYY-MM-DD HH24:MI:SS')"
                             f" AND EVENT_TIME < TO_DATE('{period_start}', 'YYYY-MM-DD HH24:MI:SS')"
                             f" ORDER BY EVENT_TIME")

query1 = (f"SELECT SC_TAG.TAG_DESC, SC_EVENT_LOG.EVENT_VALUE"
                             f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
                             f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
                             f" WHERE " 
                             f" OBJECT_NO in ('26025') AND TAG_DESC LIKE 'Gate _ Width'"
                             f" AND EVENT_TIME > TO_DATE('{period_end}', 'YYYY-MM-DD HH24:MI:SS')"
                             f" AND EVENT_TIME < TO_DATE('{period_start}', 'YYYY-MM-DD HH24:MI:SS')"
                             f" FETCH NEXT 100 ROWS ONLY")


obj_data = ext.get_data_ordb(query1)
obj_data = obj_data.astype({"OBJECT_NO":str, "EVENT_VALUE":float})
obj_data = obj_data.set_index("OBJECT_NO")

delivered = 0

def set_shared_ylabel(a, ylabel, labelpad = 0.01):
    """Set a y label shared by multiple axes
    Parameters
    ----------
    a: list of axes
    ylabel: string
    labelpad: float Sets the padding between ticklabels and axis label"""

    f = a[0].get_figure()
    f.canvas.draw() #sets f.canvas.renderer needed below

    # get the center position for all plots
    top = a[0].get_position().y1
    bottom = a[-1].get_position().y0

    # get the coordinates of the left side of the tick labels
    x0 = 1
    for at in a:
        at.set_ylabel('') # just to make sure we don't and up with multiple labels
        bboxes, _ = at.yaxis.get_ticklabel_extents(f.canvas.renderer)
        bboxes = bboxes.inverse_transformed(f.transFigure)
        xt = bboxes.x0
        if xt < x0:
            x0 = xt
    tick_label_left = x0

    # set position of label
    a[-1].set_ylabel(ylabel)
    a[-1].yaxis.set_label_coords(tick_label_left - labelpad,(bottom + top)/2, transform=f.transFigure)


fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)


for link in link_list:
    if link in obj_data.index:
        df = obj_data.loc[link]
        if isinstance(df, pd.DataFrame):
            df = df.sort_values(by=["EVENT_TIME"])
            first = df.head(1)["EVENT_TIME"].values[0]
            time = [pd.Timedelta(td - first).total_seconds() for td in df["EVENT_TIME"].values]
            df["EVENT_VALUE"] = df["EVENT_VALUE"] / 86.4
            integral = integrate.trapz(y = df["EVENT_VALUE"].values, x = time)
            df["CUMSUM"] = df["EVENT_VALUE"]
            if link == up:
                link = link + " (IN)"
                IN = integral
                df.plot(x='EVENT_TIME', y="CUMSUM", label=link, ax = axes[0])
            elif link == down:
                link = link + " (OUT)"
                OUT = integral
                df.plot(x='EVENT_TIME', y="CUMSUM", label=link, ax=axes[0])
            else:
                delivered += integral
                df.plot(x='EVENT_TIME', y="CUMSUM", label=link, ax= axes[1])
        else: integral = df["EVENT_VALUE"]
        print(f"{link} integral = {integral/1000:.1f}ML")
    else:
        print(f"{link} - No data")

balance = IN - delivered - OUT

print(f"delivered = {delivered / 1000:.1f}ML")
print(f"balance = {balance / 1000:.1f}ML")
print(f"rubicon_PE = {((delivered + OUT) / IN) * 100:.1f} %")
print(f"MI_PE = {(delivered / (IN - OUT)) * 100:.1f} %")
set_shared_ylabel(axes, "FLOW (m^3/s)")

plt.show()
print()

# print(obj_data)
#
# flow_in = obj_data['EVENT_VALUE'][up]
# flow_out = obj_data['EVENT_VALUE'][down]
# supplied = 0
#
# for obj in link_list:
#     if obj not in (up, down) and obj in obj_data:
#         obj = obj_data.loc[obj]
#         supplied += obj["RAW_VALUE"]
#
# print(supplied)



#list of all regs in the database
# all_regs = obj_data['SITE_NAME'].unique()
#
#
# display_filename_prefix_middle = '├───'
# display_filename_prefix_middle_bus = '╞//═'
# display_filename_prefix_last_bus = '╘//═'
# display_filename_prefix_last = '└───'
# display_parent_prefix_middle = '     '
# display_parent_prefix_last = '│    '

