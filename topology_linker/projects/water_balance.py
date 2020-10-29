"""A water balance tool.
Takes LVBC (described by a linkage table) and performs a water balance to create a system efficiency report"""
#TODO this could be rewritten as a class to have better selectability before running the water_balance e.g specifying which D/S meters are flumes
__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

import pandas as pd
import fginvestigation.extraction as ext
from topology_linker.src.constants import DS_METER, DS_ESC, US_REG
from topology_linker.src.utils import get_linked_ojects, Q_flume, volume, get_ET_RF
from topology_linker.projects.csv2pdftable import csv2pdftable

# default values
EXPORT = False  # whether to create a waterbalance csv
DEBUG = False  # extra columns in output
SHOW = False  # whether to show charts for every meter as the balance is created
TOPOLOGY = False  # whether to make a .txt file of the branch topology


def water_balance(branch_name, upstream_point, downstream_point, link_df,
                  period_start, period_end,
                  use_regs, area, out:set = set(),
                  **kwargs) -> pd.DataFrame:
    """

    Parameters
    ----------
    upstream_point
    downstream_point
    link_df
    use_regs
    kwargs
    branch_name
    period_end
    period_start
    area: in m2
    out: this is a set of object_nos that will be the used when not calculating the total branch
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

    file_name = f"../out/LVBC/{branch_name}_SysEff-{period_end.strftime('%Y%m%d')}" #TODO clean this up

    link_df = pd.read_csv(link_df,
                          usecols=['OBJECT_NO', 'LINK_OBJECT_NO', 'LINK_DESCRIPTION', 'POSITION'],
                          dtype={'OBJECT_NO': int, 'LINK_OBJECT_NO': int, 'LINK_DESCRIPTION': str, 'POSITION':int})
    link_df = link_df.astype({'OBJECT_NO': str, 'LINK_OBJECT_NO': str, 'LINK_DESCRIPTION': str, 'POSITION':int})

    print("collecting topology...")
    link, link_list = get_linked_ojects(object_A=upstream_point,
                                        object_B=downstream_point,
                                        source=link_df)
    print("done.")
    print(link)
    if use_regs:
        desc = [DS_METER, DS_ESC, US_REG]
    else:
        desc = [DS_METER, DS_ESC]
    link_list = link.get_all_of_desc(desc=desc) #the link list is the list of nodes that will have their volumes calculated
    link_list = [l for l in link_list if l.object_no not in out]
    lc = link.get_last_child() #for displaying the last node on the report export

    objects = []
    # run through all the children in the topology and classify the metering type so that the correct model is used TODO: refactor the models into a Class
    for obj in [link]+link.get_all():
        object_type = ext.get_data_ordb("SELECT DISTINCT ot.OBJECT_DESC FROM OBJECT_ATTR_VALUE oav JOIN"
              " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
              " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
              " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
             f" WHERE oav.OBJECT_NO = {obj.object_no}")
        obj.object_type = "FLUME" if object_type.isin(["FLUMEGATE R", "FLUMEGATE M"]).all().bool() else "GENERAL"
        objects.append(obj.object_no)
        if obj.object_no in out:
            #swap object no for Node of object in OUT (easier than doing a recursive search later on hehe)
            out.discard(obj.object_no)
            out.add(obj)


    objects = tuple(objects)
    if len(objects) == 1:
        objects = objects[0] #I forgot how this solves any problem? tuple formating?
    query = (f"SELECT OBJECT_NO, SC_EVENT_LOG.EVENT_TIME, SC_EVENT_LOG.EVENT_VALUE, TAG_NAME "
             f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
             f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
             f" WHERE "
             f" OBJECT_NO IN {objects} AND TAG_NAME IN ('FLOW_ACU_SR', 'FLOW_VAL')"
             f" AND EVENT_TIME > TO_DATE('{period_start.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f"AND EVENT_TIME < TO_DATE('{period_end.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
             f" ORDER BY EVENT_TIME")

    # This gets all of the totaliser and current flow data from the ORACLE database for all the objects in the topology
    obj_data = ext.get_data_ordb(query)
    obj_data = obj_data.astype({"OBJECT_NO": str, "EVENT_VALUE": float})
    obj_data = obj_data.set_index("OBJECT_NO") #TODO this is a stupid artifact and will be rewritten, see inside volume() function

    IN = 0.0 #TODO allow more than one IN (MUST be within the same topology linkage)
    if link.object_type == 'FLUME':
        IN += Q_flume(asset_id=(link.object_name, link.object_no),
                     time_first=period_start, time_last=period_end,
                     alpha=0.738, beta=0.282, gamma = 1.74, adjust=True, show=show, debug=debug)
        print(f"Used FLUME calcs for {link.object_name}")
    else:
        in_df, _ = volume(obj_data, [link], period_end, period_start, show=show)
        IN += in_df['RTU_totaliser (ML)'].iloc[0]
        print(f"Used GENERAL calcs for {link.object_name}")

    OUT = 0.0
    for gate in out:
        if gate.object_type == 'FLUME':
            OUT += Q_flume(asset_id=(gate.object_name, gate.object_no),
                     time_first=period_start, time_last=period_end,
                     alpha=0.738, beta=0.282, adjust=True, show=show, debug=debug)
            print(f"Used FLUME calcs for {gate.object_name}")
        else:
            out_df, _ = volume(obj_data, [gate], period_end, period_start, show=show)
            OUT += out_df['RTU_totaliser (ML)'].iloc[0]
            print(f"Used GENERAL calcs for {gate.object_name}")

    if export:
        if topology:
            with open(f"{file_name}-topology.txt", 'w', encoding="utf-8") as topo:
                topo.writelines(link.__str__())

        fh = open(f"../out/{file_name}-report.csv", 'w', newline='')
        fh.writelines([
            f"System Efficiency between {link.object_name} ({link.object_no}) and {lc.object_name} ({lc.object_no})\n",
            f"for period {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}\n"])


    #This gets the volume of each meter for the period.
    # It will also collect the manual readings for meters where telemetered data can not be found
    out_df, vol_metadata = volume(obj_data, link_list, period_end, period_start, show=show)
    meters_not_checked, meters_not_read, manual_meters, telemetered, meters, meters_neg = vol_metadata

    # For checking if there are major differences in totaliser and current flow integration
    out_df["diff"] = (out_df["RTU_totaliser (ML)"] - out_df["flow_integral (ML)"]) / out_df[
        ["RTU_totaliser (ML)", "flow_integral (ML)"]].max(axis=1) * 100

    print()

    # MI fg calc for REGS, requires the object_id for these regs to also be described in link_list, i.e. use_regs = True
    REGS = [l for l in link.get_all_of_desc(desc=[US_REG]) if l.object_no not in out]
    for reg in REGS:
        out_df.loc[out_df.object_id == reg.object_no, "FG_calc (ML)"] = Q_flume((reg.object_name, reg.object_no),
                                                                                period_start, period_end, adjust=True, show=show, debug=debug)

    # EVAPORATION AND RAINFALL

    ET, RF = get_ET_RF(period_start, period_end, debug)

    EVAP = ((area * ET) / 1000000) * 0.8 #0.8 is from the LVBC evaporation study
    RAINFALL = (area * RF) / 1000000 #convert from mm3/Ha to ML

    print(f"EVAP: {EVAP:.1f}, RAINFALL: {RAINFALL:.1f}")

    if use_regs:
        DEL = out_df.loc[(out_df['diff'] > 50) & out_df["FG_calc (ML)"].isna(), "flow_integral (ML)"].sum() + \
              out_df.loc[(out_df['diff'] <= 50) & out_df["FG_calc (ML)"].isna(), 'RTU_totaliser (ML)'].sum() + \
              out_df.loc[out_df["FG_calc (ML)"].isna(), "manual_reading (ML)"].sum()
        RTU = out_df.loc[out_df["FG_calc (ML)"].isna(), 'RTU_totaliser (ML)'].sum()
        INT = out_df.loc[out_df["FG_calc (ML)"].isna(), "flow_integral (ML)"].sum()
        MAN = out_df.loc[out_df["FG_calc (ML)"].isna(), "manual_reading (ML)"].sum()
        REG = out_df["FG_calc (ML)"].sum()

    else:
        # find when diff is very largely positive i.e. INT is << RTU
        DEL = out_df.loc[out_df['diff'] > 50, "flow_integral (ML)"].sum() + out_df.loc[out_df['diff'] <= 50, 'RTU_totaliser (ML)'].sum() + out_df["manual_reading (ML)"].sum()
        RTU = out_df['RTU_totaliser (ML)'].sum()
        INT = out_df["flow_integral (ML)"].sum()
        MAN = out_df["manual_reading (ML)"].sum()
        REG = 0.0

    if (DEL + REG) == 0.0:
        SE = f"{100 * (OUT / IN):.1f}*"
    else:
        SE = f"{((DEL + REG) / (IN - OUT)) * 100:.1f}"


    if export:

        fh.writelines([
            f"\nSystem Efficiency (%):, {SE}\n",
            "\n"])

        fh.writelines([f"Diverted (ML):, {IN:.1f} {OUT:.1f}\n",
                       f"Delivered (ML):, {DEL+REG:.1f}\n",
                       f"Evaporative loss (ML):, {EVAP:.1f}\n",
                       f"Rainfall (ML):, {RAINFALL:.1f}\n",
                       f"Seepage loss (ML):, not yet implemented\n",
                       f"Unaccounted loss (ML):, {IN + RAINFALL - (DEL + REG + EVAP) - OUT:.1f}\n",
                       "\n"])

        fh.writelines([f"Outlets\n",
                       f"Telemetered:, {len(telemetered)}\n",
                       f"Manually read:, {len(manual_meters)} \n",
                       f"Unmetered sites:, {len(meters_not_checked)}\n",
                       f"Total:, {len(meters_not_checked) + len(manual_meters) + len(telemetered)}\n",
                       "\n" if len(
                           meters_not_read) == 0 else f"{len(meters_not_read)} manually read meters are missing up to date readings.\n"])

        if not debug:
            cols = ["outlet", "RTU_totaliser (ML)", "manual_reading (ML)"]
            if use_regs:
                cols.append("FG_calc (ML)")
                total = f"Total, {DEL:.1f}, {MAN:.1f}, {REG:.1f}\n"
            else:
                total = f"Total, {DEL:.1f}, {MAN:.1f}\n"

            out_df[cols].to_csv(path_or_buf=fh, index=False, float_format='%.1f')

            meters_not_read = out_df.loc[out_df["object_id"].isin(meters_not_read), "outlet"].values.tolist()
            fh.writelines([total,
                           "\n",
                           f"time of data collection: {pd.datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
                           "\n",
                           f"meters not read:\n"] + [',' + meter + '\n' for meter in meters_not_read])
        else:
            out_df.to_csv(path_or_buf=fh, index=False, float_format='%.3f')
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


    return out_df, SE


if __name__ == "__main__":
    #running this file on its own
    period_start = pd.datetime(year=2019, month=11, day=16, hour=00)
    period_end = pd.datetime(year=2020, month=2, day=17, hour=00)
    out = water_balance("WARBURN", '26025', '40949', "../out/WARBURN_LINKED.csv", period_start, period_end,
                         use_regs=True, area=22000, show=True, export=True)
    print(out)
