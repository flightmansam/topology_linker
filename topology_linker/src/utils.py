"""Somewhat non-specific tools pertinent to the topology linkage and water balance project"""
import io
import re

import requests

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

from typing import Tuple, Union, Dict
import pandas as pd
from fginvestigation.extraction import get_data_sql, get_data_ordb
from topology_linker.src.node import Node
import matplotlib.pyplot as plt
from scipy import integrate

def parse(file_path: str) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Splits the csv into section 1. and section 2. as described in the readme
    :returns dataframes of each with the branch/lateral as the column"""
    with open(file_path, 'r', encoding="UTF-8") as fh:
        start_index = 0
        for line in fh.readlines():
            # find the start of the data (instead of just skipping "n" rows)
            if line.__contains__("Regulator Number"):
                break
            start_index += 1

    csv_df = pd.read_csv(file_path, skiprows=start_index, na_values=' ', skipfooter=3,
                         usecols=['Branch', 'Regulator Number ', 'Outlet '], engine='python')
    csv_df.dropna(axis='index', how='all', inplace=True)
    csv_df.columns = [col.strip() for col in csv_df.columns]

    lateral_descriptions = {}
    main_channel = []
    # collect lateral descriptions
    branch = None
    main_channel_last_index = None
    for index, row in csv_df.iterrows():
        if pd.isna(row[:2]).tolist() == [False, False]:
            # this is marks the start of a description (and end of the last one)
            if main_channel_last_index is None:
                main_channel_last_index = index
            if branch is not None:
                lateral_descriptions[branch] = pd.DataFrame(lateral_descriptions[branch])
            branch = row["Branch"].strip()

        if main_channel_last_index is None:
            main_channel.append(row.tolist())

        if branch is not None:
            if branch not in lateral_descriptions:
                lateral_descriptions[branch] = []
            lateral_descriptions[branch].append(row)
    lateral_descriptions[branch] = pd.DataFrame(lateral_descriptions[branch])  # covert last branch to df
    main_channel = pd.DataFrame(main_channel, columns=row.keys())  # probably don't need to call row.keys()

    return main_channel, lateral_descriptions


def query(headings: Union[tuple, str], by: str = 'OBJECT_NAME') -> pd.DataFrame:
    """Looks at the input heading and selects whether the heading is an OBJECT_NO or OBJECT_NAME using the "by" parameter
    uses the production database to find all naming information about that object"""
    # query = ("SELECT OBJECT_NO, ASSET_CODE, SITE_NAME, VALUE"
    #         f" FROM V_D_SITE_DETAILS WHERE SITE_NAME IN "
    #         f" {headings}"
    #         f" AND ATTRIBUTE = 'Channel Name'")

    if isinstance(headings, str):
        # assuming string only one item
        headings = "('" + headings + "')"
    if by == 'OBJECT_NO':
        by = 'OBJECT.OBJECT_NO'

    query = (
        f"Select OBJECT.OBJECT_NO, ATTRIBUTE_VALUE, OBJECT_NAME, ATTRIBUTE_TYPE"
        f" From OBJECT_ATTR_VALUE RIGHT JOIN OBJECT"
        f" ON OBJECT_ATTR_VALUE.OBJECT_NO = OBJECT.OBJECT_NO"
        f" WHERE {by} IN {headings}"
        f" AND (ATTRIBUTE_TYPE in ('117', '45') OR OBJECT_VALUE_NO IS NULL)")

    query = get_data_ordb(query)
    query = query.astype({"OBJECT_NO": str, "ATTRIBUTE_TYPE": str})
    query.replace(' ', pd.np.NaN, inplace=True)

    out = {
        'OBJECT_NO': [],
        'ASSET_CODE': [],
        'SITE_NAME': []
    }

    vals = query["OBJECT_NAME"].unique()
    for site in vals.tolist():
        q = query.loc[query["OBJECT_NAME"] == site]
        if q["ATTRIBUTE_TYPE"].values.all() == 'None':
            # Q came from the RIGHT side of the SQL join only
            q["ATTRIBUTE_VALUE"] = q["OBJECT_NAME"]
            q["ATTRIBUTE_TYPE"] = '117'
        q = q.dropna()
        if len(q) == 2:
            q = q.loc[q["ATTRIBUTE_TYPE"] == '117']
        q = q.values.tolist()[0]
        out['OBJECT_NO'].append(q[0])
        out['ASSET_CODE'].append(q[1])
        out['SITE_NAME'].append(q[2])

    return pd.DataFrame(out)


def get_linked_ojects(object_A: str, object_B: str, source: pd.DataFrame, encoding: str = "OBJECT_NO"):
    """
    Will return a topology of objects in a pool between object_A and object_B. edward tufte

    This function recursively ascends (build()) the network topology until it finds object_A,
    all the while building the tree structure of the network between object_B and object_A by
    recursively searching (explore()) the structure at each level.

    :param object_A: the identifier of the upstream object
    :param object_B: the identifier of the downstream object
    :param encoding: encoding of object_A and object_A. valid options 'OBJECT_NO', 'SITE_NAME', 'ASSET_CODE'
    :param source: dataframe to be as the link table (cols = OBJECT_NO, LINK_OBJECT_NO, LINK_DESCRIPTION, POSITION)
    :return: Tuple(Node, List): a node representing that topology and a list of object numbers in that pool (object_A and object_B)
    """
    assert source is not None

    objects = [object_A]

    def get(object, column="OBJECT_NO"):
        link_obj: pd.DataFrame = source.loc[source[column] == str(object)]
        link_obj = link_obj.sort_values("POSITION")  # puts children in order of their appearance in the channel
        return link_obj.reset_index(drop=True)

    def explore(upstream_reg_id, _object_B):
        children = []

        up_children = get(upstream_reg_id)

        for index, link in up_children.iterrows():
            link_object_no = link['LINK_OBJECT_NO']
            objects.append(link_object_no)
            link_description = link['LINK_DESCRIPTION']
            link_asset_code = query(str(link_object_no), by="OBJECT_NO")['SITE_NAME'][0]
            child = Node()
            child.object_no = link_object_no
            child.object_description = link_description
            child.object_name = link_asset_code
            if link_object_no != _object_B:
                expl = explore(link_object_no, _object_B)
                child.addNode(expl)
            children.append(child)

        return children

    object_A = query(object_A, by=encoding)
    A_object_no = object_A["OBJECT_NO"][0]

    def build(_object_B: str):
        _object_B = query(_object_B, by=encoding)
        B_object_no = _object_B["OBJECT_NO"][0]

        g = get(B_object_no, "LINK_OBJECT_NO").head(1)
        up_object_no = g["OBJECT_NO"][g.first_valid_index()]
        up_asset_code = query(str(up_object_no), by="OBJECT_NO")['SITE_NAME'][0]
        #up_description = g["LINK_DESCRIPTION"][0]
        up_description = get(up_object_no, "LINK_OBJECT_NO").LINK_DESCRIPTION.iloc[0]

        upstream = Node()
        upstream.object_no = up_object_no
        upstream.object_description = up_description
        upstream.object_name = up_asset_code
        upstream.addNode(explore(up_object_no, B_object_no))

        if up_object_no == A_object_no:
            return upstream
        else:
            next_reg = build(up_object_no)

            next_reg.get_last_child().addNode(upstream.children)
            return next_reg

    return build(object_B), objects


def subtract_one_month(dt0: pd.datetime):
    dt1 = dt0.replace(day=1)
    dt2 = dt1 - pd.Timedelta(days=1)
    dt3 = dt2.replace(day=1)
    return dt3


def not_monotonic(df: pd.DataFrame, col: str) -> pd.Series:
    """Will check if a column in a dataframe monotonic increasing (within a certain sensitivity)
    @:returns Series of booleans True if the monotonicity is broken at that index"""
    bool_array = []
    sensitivity = 2.0  # this lowers the sensitivity of the filter (i.e. values within this range will still be considered as monotonic increasing)
    vals = df[col].values
    for index, val in enumerate(vals[:-1]):
        # check to see if value is greater than the next n values (this filters out random zeros and weird float decimal point changes)
        n = 100
        n = n if index < len(vals) - n else len(vals) - index
        # get the proceeding n values
        v = vals[index + 1: index + 1 + n] < val - sensitivity
        bool_array.append(v.all())
    return pd.Series(bool_array + [False])  # last value cannont be checked for monoticity


def fix_resets(df: pd.DataFrame, col='EVENT_VALUE') -> pd.DataFrame:
    """Resets are when the totaliser (col) in the RTU goes to zero and starts accumulating again
    This code recognises this condition, and ignores all other conditions that would cause data to go to zero
    Can account for unexpected zeros at end of time series, but NOT at beginning."""
    working_df = df.reset_index()
    pattern = not_monotonic(working_df, col)

    jumps_to_zero = working_df[pattern]  # this grabs the value just prior to whenever the pattern goes from high to low

    # address the latest jumps first (go backwards)
    for index, row in jumps_to_zero.iloc[::-1].iterrows():
        if row[col] != 0.0:
            # this is a jump! - we need to sum this number to all the dates ahead of it
            slice = working_df.loc[working_df.index > index, col]
            slice += row[col]
            working_df.loc[working_df.index> index, col] = slice

    return working_df


def get_manual_meter(object_no, period_start, period_end=None, users: list = None):

    MAX_dist = pd.Timedelta(weeks=2)

    if period_end is None:
        period_end = period_start + pd.Timedelta(weeks=4)

    if users is None:
        users = list(get_data_ordb(
            "SELECT username from all_users WHERE REGEXP_LIKE(username, 'MIA_([0123456789]{4}|PROD)')").USERNAME.unique())

    # query to get all meter reading data, ever (or from predefined users)
    query = f"SELECT DATE_EFFECTIVE, METERED_USAGE, METER_READING FROM {users[0]}.METER_READING" \
            f" WHERE SP_OBJECT_NO = {object_no}"
    if len(users) > 1:
        for username in users[1:]:
            query += " UNION ALL" \
                     " Select DATE_EFFECTIVE, METERED_USAGE, METER_READING" \
                     f" From {username}.METER_READING" \
                     f" WHERE SP_OBJECT_NO = '{object_no}'"
    query += "ORDER BY DATE_EFFECTIVE"

    df = get_data_ordb(query)
    if df.shape[0] == 0:
        return None, period_start

    data = df.loc[(df.DATE_EFFECTIVE <= period_end) & (df.DATE_EFFECTIVE >= period_start)]

    # Ensure the previous reading from the left bound wasn't more than 6 weeks away
    if data.shape[0] > 0:
        left_bound = data.head(1)
        if left_bound.index > 1:
            prv_idx = df.loc[left_bound.index - 1]
            # If the previous reading from the left bound is more than MAX_dist away we shouldn't include the left bound measurement.
            if pd.Timedelta(left_bound.DATE_EFFECTIVE.values[0] - prv_idx.DATE_EFFECTIVE.values[0]) > MAX_dist:
                return data.iloc[1:].METERED_USAGE.sum(), period_end
            else:
                return data.METERED_USAGE.sum(), period_end
        else:
            print(f"{object_no} - Warning! Cannot ensure previous reading to left bound was within MAX_dist!")
            return data.METERED_USAGE.sum(), period_end

    else:
        # if no readings within bounds then find the nearest to the end date
        nearest_idx = abs(df.DATE_EFFECTIVE - period_end).idxmin()

        # Ensure the nearest reading wasn't more than MAX_dist away
        if abs(df.loc[nearest_idx, "DATE_EFFECTIVE"] - period_end) < MAX_dist:
            return df.loc[nearest_idx, "METERED_USAGE"], df.loc[nearest_idx, "DATE_EFFECTIVE"]
        else:
            return -1, period_end



def _Q_flume(h1: float, h2: float, b: float, alpha: float = 0.738, beta: float=0.282, gamma:float = 1.5) -> float:
    g = 9.80665  # (standard g)

    assert isinstance(h1, float) & \
           isinstance(h2, float) & \
           isinstance(alpha, float) & \
           isinstance(beta, float) & \
           isinstance(b, float)
    if h1 == 0.0:
        print(f"Warning! h1 was zero H1 = {h1}, H2 = {h2}")
        h1 = 1e-6  # in case of zero division
        return 0.0
    if h2 < 0.0:
        h2 = 0.0

    C_D = alpha * (1.0 - (h2 / h1) ** gamma) ** beta

    Q = (2 / 3) * C_D * b * (2 * g) ** 0.5 * h1 ** 1.5
    return Q


def invert_Q_flume(Q:Union[float, pd.Series], C_D:float, b:float) -> Union[float, pd.Series]:
    g = 9.80665  # (standard g)
    assert isinstance(C_D, float) & \
           isinstance(b, float)

    h1 = ((3 * Q) / (2 * C_D * b * (2 * g) ** 0.5)) ** (2 / 3)

    return h1


def Q_flume(asset_id: tuple, time_first: pd.datetime, time_last: pd.datetime,
            alpha: float = 0.738, beta: float=0.282, gamma: float = 1.5, adjust=False, show=False, debug=False, timeseries=False) -> float:
    """Collect gate positions and U/S and D/S water level for Scotts from the Hydrology SQL table
    and calculate the flow from that period. Wrapper for _Q_flume()

    Parameters
    ----------alpha, beta=0.282
    adjust : Whether the code will simply adjust the flume gate (False) or calculate flow completely (True)
    timeseries : Whether to return the Q values as a timeseries rather than an integrated volume (default)
    """
    oracle = True

    asset_code = asset_id[0]
    object_no = asset_id[1]

    gate_widths = (f"SELECT SC_TAG.TAG_DESC, SC_EVENT_LOG.EVENT_VALUE"
                             f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
                             f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
                             f" WHERE " 
                             f" OBJECT_NO in ('{object_no}') AND TAG_DESC LIKE 'Gate _ Width'"
                             f" FETCH NEXT 100 ROWS ONLY")
    gate_widths = get_data_ordb(gate_widths).dropna()
    no_gates = len(gate_widths.TAG_DESC.unique())
    gate_width = gate_widths.EVENT_VALUE.mean()

    if no_gates > 0:


        tags = [f'Gate {i} Elevation' for i in range(1, no_gates + 1)] + \
               ['U/S Water Level', 'D/S Water Level', 'Current Flow']
        tags = tuple(tags)

        if not oracle:
            utc = pd.datetime.utcnow().astimezone().utcoffset()
            time_last -= utc
            time_first -= utc
            query = (
                f" SELECT EVENT_TIME, Tags.TAG_DESC, EVENT_VALUE, Tags.TAG_ID"
                f"    FROM EVENTS INNER JOIN Tags ON EVENTS.TAG_ID = Tags.TAG_ID"
                f" WHERE OBJECT_NO = {object_no}) "
                f"    AND TAG_DESC in {tags}"
                f"    AND (EVENT_TIME >= '{time_first.strftime('%Y-%m-%d %H:%M:%S')}')"
                f"    AND (EVENT_TIME <= '{time_last.strftime('%Y-%m-%d %H:%M:%S')}')"
            )
            df = get_data_sql(query)

        else:
            query = (f"SELECT SC_EVENT_LOG.EVENT_TIME, SC_TAG.TAG_DESC, SC_EVENT_LOG.EVENT_VALUE, SC_TAG.TAG_ID "
                     f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
                     f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
                     f" WHERE "
                     f" OBJECT_NO = {object_no} AND TAG_DESC in {tags}"
                     f" AND EVENT_TIME >= TO_DATE('{time_first.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                     f" AND EVENT_TIME <= TO_DATE('{time_last.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                     f" ORDER BY EVENT_TIME")
            df = get_data_ordb(query)

        #pivot and interpolate all the data
        df = df.pivot("EVENT_TIME", "TAG_DESC", "EVENT_VALUE")

        cols = {
            'U/S Water Level':"USL",
            'D/S Water Level':"DSL",
            'Current Flow': "QRC"
        }
        df.rename(columns=cols, inplace=True)

        #is there enough data?
        if 'DSL' in df.columns and 'USL' in df.columns:

            GATES = [f'Gate {i} Elevation' for i in range(1, no_gates + 1)]

            # #set zeros to NA and then interpolate
            df.USL.loc[df.USL == 0.0] = pd.np.NaN
            df.DSL.loc[df.DSL == 0.0] = pd.np.NaN
            for gate in GATES:
                df[gate].loc[df[gate] == 0.0] = pd.np.NaN

            df.interpolate(limit_direction='both', inplace=True)

            df["G_av"] = df[GATES].sum(axis=1) / no_gates

            df.QRC /= 86.4 # convert to m3/s

            h1 = df.USL - df.G_av
            h2 = df.DSL - df.G_av

            if adjust:
                #h2 = 0 if h2 < 0 else h2
                h2.loc[h2 < 0] = 0
                df["QMI"] = 1.0054495 * df.QRC * (1 - (h2/h1)) ** 0.0576

            else:
                Qs = []
                for idx in df.index.values:
                    Q = _Q_flume(h1[idx],
                                 h2[idx],
                                 alpha=alpha,
                                 beta=beta,
                                 gamma=gamma,
                                 b=no_gates * gate_width)
                    Qs.append(Q)  # m3/

                df["QMI"] = Qs

            df["QMI"].interpolate(inplace=True)
            if show:
                ax = df.plot()
                h1.plot(label="H1", ax=ax)
                h2.plot(label="H2", ax=ax)
            if debug: print(df.isna().any().to_string())

            return df["QMI"]

            first = df.index.values[0]
            delta_t = [pd.Timedelta(td - first).total_seconds() for td in df.index.values]
            FG_integral = integrate.cumtrapz(y=df["QMI"].values, x=delta_t) / 1000
            FG_integral = pd.Series(data=FG_integral, index=df.index.values[1:]).transpose()
            FG = max(FG_integral)
            label = f"QMI: INTEGRAL -> {FG:.1f} ML"
            print(label)

            delta_t = [pd.Timedelta(td - first).total_seconds() for td in df.QRC.index.values]
            CF_integral = integrate.cumtrapz(y=df.QRC.values, x=delta_t) / 1000  # ?
            CF_integral = pd.Series(data=CF_integral, index=df.QRC.index.values[1:]).transpose()
            QRC = max(CF_integral)
            label = f"QRC: INTEGRAL -> {QRC:.1f} ML"
            print(label)

            if show:
                ax.legend()
                plt.title(asset_code)
                plt.show()
            else:
                plt.close()

            return FG
        else:
            print(f"{asset_id}: Missing a level in columns: {df.columns}")
            return None

    else:
        print(f"{asset_id}: Could not get any gates in this period")
        return None

def get_ET_RF(period_start:pd.datetime, period_end: pd.datetime, debug=False) -> Tuple[float, float]:

    ET = 0.0
    RF = 0.0

    month = period_end.month
    year = period_end.year

    assert period_start < period_end

    while True: #need a better break condition than that for handling errors
        url = f"http://www.bom.gov.au/watl/eto/tables/nsw/griffith_airport/griffith_airport-{year}{month:02d}.csv"
        s = requests.get(url).content

        BOM_data = pd.read_csv(io.StringIO(s.decode('utf-8', errors='ignore')),na_values=[' '])
        BOM_data = BOM_data[BOM_data.iloc[:, 0] == "GRIFFITH AIRPORT"]
        BOM_data.iloc[:, 1] = pd.to_datetime(BOM_data.iloc[:, 1], format="%d/%m/%Y")
        BOM_data = BOM_data.set_index([BOM_data.iloc[:, 1]])

        if debug: print(BOM_data.to_string())

        ET += pd.to_numeric(BOM_data.iloc[(BOM_data.index <= period_end) & (BOM_data.index >= period_start), 2]).dropna().sum()
        RF += pd.to_numeric(BOM_data.iloc[(BOM_data.index <= period_end) & (BOM_data.index >= period_start), 3]).dropna().sum()


        print(ET, RF)

        if month == period_start.month and year == period_start.year:
            break

        if month > 1:
            month -= 1
        else:
            month = 12
            year -= 1

    return ET, RF

def volume(obj_data: pd.DataFrame, objects: list, period_start, period_end, show=False, verbose=False) \
        -> Tuple[pd.DataFrame, list]:
    """

    :param obj_data: all the data for the search in the period ('EVENT_TIME', 'FLOW_ACU_SR', 'FLOW_VAL')
    :param objects: list of object no.s that you would like to calculate volumes from obj_data
    :param period_start:
    :param period_end:
    :param show: whether to open a plot for every meter calculated
    :param verbose: whether to show extra console output
    """
    out_df = pd.DataFrame()
    meters_not_checked = set()
    meters_not_read = set()
    manual_meters = set()
    telemetered = set()
    meters = 0
    meters_neg = []

    out = {
        "outlet": [],
        "object_id": [],
        "RTU_totaliser (ML)": [],
        "flow_integral (ML)": [],
        "manual_reading (ML)": [],
    }

    for link_obj in objects:
        out["outlet"].append(link_obj.object_name)
        out["object_id"].append(link_obj.object_no)
        link_obj = link_obj.object_no
        if verbose: print(link_obj)
        if link_obj in obj_data.index: #THIS IS SILLY #TODO rewrite index as EVENT_TIME and use obj_data.OBJECT_NO.unique()
            df = obj_data.loc[link_obj]
            if isinstance(df, pd.DataFrame):
                # collect RTU data (primary source)
                RTU_df = df.loc[df["TAG_NAME"] == 'FLOW_ACU_SR'].sort_values(by=["EVENT_TIME"]) #TODO rewrite as pivot and use loc
                if show and not RTU_df.empty: ax = RTU_df.plot(x="EVENT_TIME", y="EVENT_VALUE", label="RTU_SOURCE")
                RTU_df = fix_resets(RTU_df) #TODO this function may need rewriting...
                if show and not RTU_df.empty: RTU_df.plot(x="EVENT_TIME", y="EVENT_VALUE", ax=ax, label="RTU_INTEGRAL")
                RTU = 0.0 if RTU_df.empty else (RTU_df["EVENT_VALUE"].max() - RTU_df.head(1)["EVENT_VALUE"].iat[
                    0]) * 1000

                # calculate INTEGRAL (secondary source)
                FLOW_df = df.loc[df["TAG_NAME"] == 'FLOW_VAL'].sort_values(by=["EVENT_TIME"]) #TODO rewrite as pivot and use loc
                neg = FLOW_df["EVENT_VALUE"].values < 0.0
                if neg.any(): meters_neg.append(link_obj)
                if FLOW_df.empty:
                    integral = [0.0]
                else:
                    # FLOW_df.loc[FLOW_df["EVENT_VALUE"] < 0.0, "EVENT_VALUE"] = 0.0
                    first = FLOW_df.head(1)["EVENT_TIME"].values[0]
                    time = [pd.Timedelta(td - first).total_seconds() for td in FLOW_df["EVENT_TIME"].values]
                    FLOW_df["EVENT_VALUE"] = FLOW_df["EVENT_VALUE"] / 86.4
                    integral = integrate.cumtrapz(y=FLOW_df["EVENT_VALUE"].values, x=time) / 1000
                    integral = pd.Series(data=integral, index=FLOW_df["EVENT_TIME"].values[1:]).transpose()
                    if integral.empty:
                        integral = [0.0]
                    if show: integral.plot(ax=ax, label="INTEGRAL")
                integral = max(integral) * 1000
                if RTU_df.empty:
                    RTU = integral
                    print(f"No totaliser values for {link_obj}, using integral")

                if show:
                    plt.title(f"{link_obj}")
                    ax2:plt.Axes = ax.twinx()
                    FLOW_df.plot(x="EVENT_TIME", y="EVENT_VALUE", label="FLOW", ax=ax2, color="#9467bd", alpha=0.2)
                    ax2.set_ylabel("FLOW (m3/s)")
                    box = ax.get_position()
                    ax.set_position([box.x0, box.y0 + box.height * 0.1,
                                     box.width, box.height * 0.9])
                    ax_ln, ax_lb = ax.get_legend_handles_labels()
                    ax_lb[1] = ax_lb[1] + f"\n= {RTU / 1000:.1f} ML"
                    ax_lb[2] = ax_lb[2] + f"\n= {integral / 1000:.1f} ML"
                    ax2_ln, ax2_lb = ax2.get_legend_handles_labels()
                    ax.legend(ax_ln + ax2_ln, ax_lb + ax2_lb, loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=4)
                    ax.set_xlabel("")
                    ax.set_ylabel("CUMULATIVE FLOW (ML)")
                    ax2.get_legend().remove()
                    plt.show()
                else:
                    plt.close()

                out["RTU_totaliser (ML)"].append(RTU / 1000)
                out["flow_integral (ML)"].append(integral / 1000)
                out["manual_reading (ML)"].append("")

            else:
                print(f"{link_obj} - only one value for flow")
                volume = df["EVENT_VALUE"]
                out["RTU_totaliser (ML)"].append(volume / 1000)
                out["flow_integral (ML)"].append(volume / 1000)
                out["manual_reading (ML)"].append("")

            telemetered.add(link_obj)

        else:
            # meter data not found in Events, maybe these are un telemetry or un metered flows
            volume, date = get_manual_meter(link_obj, period_start, period_end)

            if volume is None:
                print(f"{link_obj} - No data")
                meters_not_checked.add(link_obj)
                out["RTU_totaliser (ML)"].append("")
                out["flow_integral (ML)"].append("")
                out["manual_reading (ML)"].append("")
            elif volume == -1:
                print(f"{link_obj} - meter not yet read")
                meters_not_read.add(link_obj)
                manual_meters.add(link_obj)
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
    out_df["RTU_totaliser (ML)"] = pd.to_numeric(out_df["RTU_totaliser (ML)"])
    out_df["flow_integral (ML)"] = pd.to_numeric(out_df["flow_integral (ML)"])
    out_df["manual_reading (ML)"] = pd.to_numeric(out_df["manual_reading (ML)"])

    return out_df, [meters_not_checked, meters_not_read, manual_meters, telemetered, meters_not_read, meters_neg]

def get_reachFROMreg(object_no:Union[int, list]):
    """
    For every reg object_no in the input, return the Channel Name of the object.
    If input is a list, a dataframe is returned. Else just the Channel name is returned as a string.
    """
    if isinstance(object_no, list):
        query = (
            "SELECT o.OBJECT_NO, oav.ATTRIBUTE_VALUE FROM OBJECT_ATTR_VALUE oav JOIN"
            " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
            " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
            " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
            f" WHERE at.ATTRIBUTE_DESC = 'CHANNEL NAME' AND o.OBJECT_NO in {tuple(object_no)}")
        return get_data_ordb(query)
    else:
        query = (
            "SELECT oav.ATTRIBUTE_VALUE FROM OBJECT_ATTR_VALUE oav JOIN"
            " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
            " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
            " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
            f" WHERE at.ATTRIBUTE_DESC = 'CHANNEL NAME' AND o.OBJECT_NO = '{object_no}'")
        return get_data_ordb(query).iloc[0,0]

def get_linkage_ordb(top_reg:int, bottom_reg:int = None)-> pd.DataFrame:
    """
    Given a regulator, this function will make a linkage of the topology
    through the ORDB sequence numbers. If a bottom reg is not specified then the linkage will include all the regs in the bottom channel
    Parameters
    ----------

    top_reg: Object number for the top regulator
    bottom_reg: Object number for the bottom regulator, optional (will collect all objects until the end of the channel)

    Returns
    -------
    A dataframe that gives all the objects in a network as linkage table

    """

    import hetools.network_map as nm

    channel = get_reachFROMreg(int(top_reg))
    print(channel)
    all_regs = nm.get_regsINreach(channel)

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

    seq = get_data_ordb(query)
    all_regs = all_regs.merge(seq[['OBJECT_NO', 'SEQUENCE_NO']], how='inner', on='OBJECT_NO').sort_values('SEQUENCE_NO')

    all_objects = nm.obj_in_bw(all_regs.OBJECT_NO.iloc[0], all_regs.OBJECT_NO.iloc[-1])

    #refine all regs to have the lateral regs
    all_regs = all_objects.loc[all_objects.OBJECT_DESC.isin(nm.regs)].copy()
    # add channel name to the regs
    all_regs['CH_NAME'] = all_regs.apply(
        lambda x: nm.get_att_value(obj_no=x['OBJECT_NO'], att='CHANNEL NAME'), axis=1)
    # add asset_code
    all_regs['ASSET_CODE'] = all_regs.apply(
        lambda x: nm.get_att_value(obj_no=x['OBJECT_NO'], att='ASSET CODE'), axis=1)

    #remove dummy regs and then remove anything without a tag_id
    dummy_pattern = 'dummy'
    dummy_regs = []
    no_tag_list = []
    for obj_no, ass_code in zip(all_regs.OBJECT_NO, all_regs.ASSET_CODE):
        if ass_code:
            if re.search(dummy_pattern, ass_code.lower()):
                dummy_regs.append(obj_no)
        if get_data_ordb(f"select * from SC_TAG where OBJECT_NO = {obj_no}").shape[0]<1: no_tag_list.append(obj_no)

def get_name(object_no:Union[int, list]):
    if isinstance(object_no, int):
        object_no = f"('{object_no}')"
    else:
        object_no = tuple(object_no)
    query = (
        "SELECT oav.OBJECT_NO, o.OBJECT_NAME FROM OBJECT_ATTR_VALUE oav JOIN"
        " ATTRIBUTE_TYPE at ON oav.ATTRIBUTE_TYPE = at.ATTRIBUTE_TYPE JOIN"
        " OBJECT_TYPE ot ON oav.OBJECT_TYPE = ot.OBJECT_TYPE JOIN"
        " OBJECT o ON oav.OBJECT_NO = o.OBJECT_NO"
        f" WHERE at.ATTRIBUTE_DESC = 'ASSET CODE' AND oav.OBJECT_NO IN {object_no}")

    df = get_data_ordb(query)
    return df.OBJECT_NAME.to_list()
