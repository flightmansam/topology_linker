"""A water balance tool.
Takes LVBC (described by a linkage table) and performs a water balance to create a system efficiency report"""
from typing import Tuple



__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import io
import pandas as pd
import requests
import fginvestigation.extraction as ext
from topology_linker.src.constants import DS_METER, DS_ESC, US_REG
from topology_linker.src.utils import get_linked_ojects, Q_flume, volume
from topology_linker.projects.csv2pdftable import csv2pdftable

#default values
EXPORT = True #whether to create a waterbalance csv
DEBUG = False # extra columns in output
SHOW = True #whether to show charts for every meter as the balance is created
TOPOLOGY = False #whether to make a .txt file of the branch topology

def water_balance(branch_name, upstream_point, downstream_point, link_df,
                  period_start, period_end,
                  use_regs,
                  **kwargs) -> pd.DataFrame:
    """

    Parameters
    ----------
    branch_name
    period_end
    period_start
    args dict: overwriting the default values

    Returns
    -------
    A Dataframe of the volumes for each meter in that report.
    If "export" is a .csv and .pdf of the report is created in the /out folder.

    **kwarkgs for default values

    """

    export = kwargs["export"] if "export" in kwargs else EXPORT
    debug = kwargs["debug"] if "debug" in kwargs else DEBUG
    show = kwargs["show"] if "show" in kwargs else SHOW
    topology = kwargs["topology"] if "topology" in kwargs else TOPOLOGY

    file_name = f"../out/{branch_name}_SysEff-{period_end.strftime('%Y%m%d')}"

    link_df = pd.read_csv(link_df,
                          usecols=['OBJECT_NO', 'LINK_OBJECT_NO', 'LINK_DESCRIPTION', 'POSITION'],
                          dtype={'OBJECT_NO': str, 'LINK_OBJECT_NO': str, 'LINK_DESCRIPTION': str})


    print("collecting topology...")
    link, link_list = get_linked_ojects(object_A=upstream_point,
                                        object_B=downstream_point,
                                        source=link_df)
    print("done.")
    print(link)
    if use_regs:
        desc = [DS_METER, DS_ESC, US_REG]
    else: desc = [DS_METER, DS_ESC]
    link_list = link.get_all_of_desc(desc=desc)
    lc = link.get_last_child()

    IN = Q_flume(asset_id=(link.object_name, link.object_no),
                 time_first=period_start, time_last=period_end,
                 alpha=0.738, beta=0.282, adjust=True)

    if export:
        if topology:
            with open(f"{file_name}-topology.txt", 'w', encoding="utf-8") as topo:
                topo.writelines(link.__str__())

        fh = open(f"../out/{file_name}-report.csv", 'w', newline='')
        fh.writelines([
            f"System Efficiency between {link.object_name} ({link.object_no}) and {lc.object_name} ({lc.object_no})\n",
            f"for period {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}\n"])

    query = (f"SELECT OBJECT_NO, SC_EVENT_LOG.EVENT_TIME, SC_EVENT_LOG.EVENT_VALUE, TAG_NAME "
             f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
             f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
             f" WHERE "
             f" OBJECT_NO IN {tuple([l.object_no for l in link_list])} AND TAG_NAME IN ('FLOW_ACU_SR', 'FLOW_VAL')"
             f" AND EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f"AND EVENT_TIME < TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f" ORDER BY EVENT_TIME")

    obj_data = ext.get_data_ordb(query)
    obj_data = obj_data.astype({"OBJECT_NO": str, "EVENT_VALUE": float})
    obj_data = obj_data.set_index("OBJECT_NO")

    out_df, vol_metadata = volume(obj_data, link_list, period_end, period_start, show=show)
    meters_not_checked, meters_not_read, manual_meters, telemetered, meters, meters_neg = vol_metadata

    out_df["diff"] = (out_df["RTU_totaliser (ML)"] - out_df["flow_integral (ML)"]) / out_df[
        ["RTU_totaliser (ML)", "flow_integral (ML)"]].max(axis=1) * 100

    print()

    # MI fg calc for REGS
    REGS = link.get_all_of_desc(desc=[US_REG])
    for reg in REGS:
        out_df.loc[out_df.object_id == reg.object_no, "FG_calc (ML)"] = Q_flume((reg.object_name, reg.object_no),
                                                                                period_start, period_end, adjust=True)

    ###EVAPORATION

    url = f"http://www.bom.gov.au/watl/eto/tables/nsw/griffith_airport/griffith_airport-{period_end.strftime('%Y%m')}.csv"
    s = requests.get(url).content
    prev_month = pd.read_csv(io.StringIO(s.decode('utf-8', errors='ignore')))
    prev_month = prev_month[prev_month.iloc[:, 0] == "GRIFFITH AIRPORT"]
    prev_month = prev_month.set_index([prev_month.iloc[:, 1]])
    prev_month_ET = pd.to_numeric(prev_month[prev_month.index <= period_end.strftime("%d/%m/%Y")].iloc[:, 2]).sum()
    prev_month_RF = pd.to_numeric(prev_month[prev_month.index <= period_end.strftime("%d/%m/%Y")].iloc[:, 3]).sum()


    url = f"http://www.bom.gov.au/watl/eto/tables/nsw/griffith_airport/griffith_airport-{period_start.strftime('%Y%m')}.csv"
    s = requests.get(url).content
    this_month = pd.read_csv(io.StringIO(s.decode('utf-8', errors='ignore')))
    this_month = this_month[this_month.iloc[:, 0] == "GRIFFITH AIRPORT"]  # strip out anything that's not a data column
    this_month = this_month.set_index([this_month.iloc[:, 1]])  # set the index to the date
    this_month_ET = pd.to_numeric(this_month[this_month.index >= period_start.strftime("%d/%m/%Y")].iloc[:,
                               2]).sum()  # sum all of the ET values for the date in the range
    this_month_RF = pd.to_numeric(this_month[this_month.index >= period_start.strftime("%d/%m/%Y")].iloc[:,
                               3]).sum()  # sum all of the rainfall values for the date in the range

    if debug: print(prev_month.to_string(), this_month.to_string())
    ET = prev_month_ET + this_month_ET
    RF = prev_month_RF + this_month_RF
    area = 22000

    EVAP = ((area * ET) / 1000000) * 0.8
    RAINFALL = (area * RF) / 1000000

    print(f"EVAP: {EVAP}, RAINFALL: {RAINFALL}")

    if export:

        if use_regs:
            RTU = out_df.loc[out_df["FG_calc (ML)"].isna(), 'RTU_totaliser (ML)'].sum()
            INT = out_df.loc[out_df["FG_calc (ML)"].isna(), "flow_integral (ML)"].sum()
            MAN = out_df.loc[out_df["FG_calc (ML)"].isna(), "manual_reading (ML)"].sum()
            REG = out_df["FG_calc (ML)"].sum()

        else:
            RTU = out_df['RTU_totaliser (ML)'].sum()
            INT = out_df["flow_integral (ML)"].sum()
            MAN = out_df["manual_reading (ML)"].sum()
            REG = 0.0

        fh.writelines([
            f"\nSystem Efficiency (%):, {((RTU + MAN) / (IN - REG)) * 100:.1f}\n",
            "\n"])

        fh.writelines([f"Diverted (ML):, {IN:.1f}\n",
                       f"Delivered (ML):, {RTU:.1f}\n",
                       f"Evaporative loss (ML):, {EVAP:.1f}\n",
                       f"Rainfall (ML):, {RAINFALL:.1f}\n",
                       f"Seepage loss (ML):, not yet implemented\n",
                       f"Unaccounted loss (ML):, {IN + RAINFALL - (RTU + MAN + REG + EVAP):.1f}\n",
                       "\n"])

        fh.writelines([f"Outlets\n",
                       f"Telemetered:, {len(telemetered)}\n",
                       f"Manually read:, {len(manual_meters)} \n",
                       f"Unmetered sites:, {len(meters_not_checked)}\n",
                       f"Total:, {len(meters_not_checked) + len(manual_meters) + len(telemetered)}\n",
                       "\n" if len(
                           meters_not_read) == 0 else f"{len(meters_not_read)} manually read meters are missing up to date readings.\n"])

        if not debug:
            cols = ["outlet", "RTU_totaliser (ML)", "manual_reading (ML)", "FG_calc (ML)"]
            out_df[cols].to_csv(path_or_buf=fh, index=False, float_format='%.1f')
        else:
            out_df.to_csv(path_or_buf=fh, index=False, float_format='%.3f')

        if not debug:
            meters_not_read = out_df.loc[out_df["object_id"].isin(meters_not_read), "outlet"].values.tolist()
            fh.writelines([f"Total, {RTU:.1f}, {MAN:.1f}, {REG:.1f}\n"
                           "\n",
                           f"time of data collection: {pd.datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
                           "\n",
                           f"meters not read:\n"] + [',' + meter + '\n' for meter in meters_not_read])
        else:
            fh.writelines([f", Total, {RTU:.1f}, {INT:.1f}, {MAN:.1f}, , {REG:.1f}\n"
                           "\n",
                           f"time of data collection: {pd.datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
                           "\n",
                           f"meters not read:\n"] + [',' + meter + '\n' for meter in meters_not_read])

        out_df.set_index("outlet", inplace=True)
        out_df[link.object_name] = pd.Series([link.object_no] + (4 * [pd.np.NaN]) + [IN])
        print(out_df.to_string())


        fh.close()

    if export:
        csv2pdftable(f'{file_name}-report.csv', f"{file_name}-report")

    return out_df


if __name__ == "__main__":
    period_start = pd.datetime(year=2019, month=11, day=16, hour=00)
    period_end = pd.datetime(year=2020, month=2, day=17, hour=00)
    json = water_balance("WARBURN", '26025', '40949', "../out/WARBURN_LINKED.csv", period_start, period_end, use_regs=True, show=False, export=True)
    print(json)