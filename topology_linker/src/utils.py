"""Somewhat non-specific tools pertinent to the topology linkage and water balance project"""
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
        up_description = g["LINK_DESCRIPTION"][0]

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
    sensitivity = 1.0  # this lowers the sensitivity of the filter (i.e. values within this range will still be considered as monotonic increasing)
    vals = df[col].values
    for index, val in enumerate(vals[:-1]):
        # check to see if value is greater than the next n values (this filters out random zeros and weird float decimal point changes)
        n = 100
        n = n if index < len(vals) - n else len(vals) - index
        # get the proceeding n values
        v = vals[index + 1: index + 1 + n] < val - sensitivity
        bool_array.append(v.all())
    return pd.Series(bool_array + [False])  # last value cannont be checked for monoticity


def fix_resets(df: pd.DataFrame) -> pd.DataFrame:
    """Resets are when the totaliser (EVENT_VALUE) in the RTU goes to zero and starts accumulating again
    This code recognises this condition, and ignores all other conditions that would cause data to go to zero
    Can account for unexpected zeros at end of time series, but NOT at beginning."""
    working_df = df.reset_index()
    pattern = not_monotonic(working_df, "EVENT_VALUE")

    jumps_to_zero = working_df[pattern]  # this grabs the value just prior to whenever the pattern goes from high to low

    # address the latest jumps first (go backwards)
    for index, row in jumps_to_zero.iloc[::-1].iterrows():
        if row["EVENT_VALUE"] != 0.0:
            # this is a jump! - we need to sum this number to all the dates ahead of it
            slice = working_df.loc[working_df["EVENT_TIME"] > row["EVENT_TIME"], "EVENT_VALUE"]
            slice += row["EVENT_VALUE"]
            working_df.loc[working_df["EVENT_TIME"] > row["EVENT_TIME"], "EVENT_VALUE"] = slice

    return working_df


def get_manual_meter(obj_no: str, date: pd.datetime) -> Union[Tuple[float, pd.datetime], Tuple[None, pd.datetime]]:
    """Messy little function to collect the nearest meter reading to the date chosen
    - pretty sure this function could by rewritten with a few pandas methods
    e.g. q.set_index("DATE_EFFECTIVE", inplace=True)
         q[date] = pd.NaN
         q.fillna('nearest', inplace=True)
         #check for distance of date and raise warning if needed
         return q.loc[date, "METERED_USAGE"]
    """
    MAX_dist = pd.Timedelta(weeks=6)
    q = ("Select DATE_EFFECTIVE, METERED_USAGE, METER_READING"
         " From METER_READING"
         f" WHERE SP_OBJECT_NO = '{obj_no}'")

    q = get_data_ordb(q)

    def check_distance(d: list):
        count = 0
        for td in d:
            if td > MAX_dist:
                count += 1
        if count > 0:
            print(
                f"Warning! For {count}/{len(d)} of the timestamps adjacent to {date}, have differences greater than {MAX_dist} for {obj_no}")
            if count / len(d) == 1.0: return -1
        return 1

    if not q.empty:
        # get the value to the LHS and RHS of date
        lhs = q.loc[q["DATE_EFFECTIVE"] < date].tail(1).reset_index(drop=True)
        rhs = q.loc[q["DATE_EFFECTIVE"] >= date].head(1).reset_index(drop=True)

        # find the nearest of the two provided both are of length 1
        rtn = None
        if not lhs.empty:
            del_LHS = date - lhs["DATE_EFFECTIVE"][0]
        else:
            rtn = rhs
        if not rhs.empty:
            del_RHS = rhs["DATE_EFFECTIVE"][0] - date
        else:
            rtn = lhs

        if rtn is not None:
            del_rtn = abs(date - rtn["DATE_EFFECTIVE"][0])
            check = check_distance([del_rtn])
            return -1 if check == -1 else rtn["METERED_USAGE"][0], rtn['DATE_EFFECTIVE'][0]

        check = check_distance([del_LHS, del_RHS])
        if check == -1: return -1, date

        if del_LHS >= del_RHS:
            # if the date is right in the middle of the readings better to overestimate than under?
            return rhs["METERED_USAGE"][0], rhs['DATE_EFFECTIVE'][0]

        else:
            return lhs["METERED_USAGE"][0], lhs['DATE_EFFECTIVE'][0]

    else:
        # No values for this meter
        return None, date


def _Q_flume(h1: float, h2: float, alpha: float, beta: float, b: float) -> float:
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

    C_D = alpha * (1.0 - (h2 / h1) ** 1.5) ** beta

    Q = (2 / 3) * C_D * b * (2 * g) ** 0.5 * h1 ** 1.5
    return Q


def invert_Q_flume(Q:Union[float, pd.Series], C_D:float, b:float) -> Union[float, pd.Series]:
    g = 9.80665  # (standard g)
    assert isinstance(C_D, float) & \
           isinstance(b, float)

    h1 = ((3 * Q) / (2 * C_D * b * (2 * g) ** 0.5)) ** (2 / 3)

    return h1


def Q_flume(asset_id: tuple, time_first: pd.datetime, time_last: pd.datetime,
            alpha: float, beta: float,
            no_gates: int, gate_width: float) -> float:
    """Collect gate positions and U/S and D/S water level for Scotts from the Hydrology SQL table
    and calculate the flow from that period. Wrapper for _Q_flume()"""
    oracle = True
    show = True

    asset_code = asset_id[0]
    object_no = asset_id[1]

    tags = [f'Gate {i} Elevation' for i in range(1, no_gates + 1)] + \
           ['U/S Water Level', 'D/S Water Level', 'Current Flow']
    tags = tuple(tags)
    # query = (f"SELECT SC_EVENT_LOG.EVENT_TIME, SC_TAG.TAG_DESC, SC_EVENT_LOG.EVENT_VALUE, SC_TAG.TAG_ID "
    #          f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
    #          f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
    #          f" WHERE "
    #          f" OBJECT_NO = {object_no} AND TAG_DESC in {tags}"
    #          f" AND EVENT_TIME >= TO_DATE('{time_first.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    #          f" AND EVENT_TIME <= TO_DATE('{time_last.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    #          f" ORDER BY EVENT_TIME")
    # orcl = get_data_ordb(query).set_index("EVENT_TIME")
    # query = (
    #     f" SELECT EVENT_TIME, Tags.TAG_DESC, EVENT_VALUE, Tags.TAG_ID"
    #     f"    FROM EVENTS INNER JOIN Tags ON EVENTS.TAG_ID = Tags.TAG_ID"
    #     f" WHERE OBJECT_NO = ( select OBJECT_NO from Objects WHERE ASSET_CODE = '{asset_code}') "
    #     f"    AND TAG_DESC in {tags}"
    #     f"    AND (EVENT_TIME >= '{time_first.strftime('%Y-%m-%d %H:%M:%S')}')"
    #     f"    AND (EVENT_TIME <= '{time_last.strftime('%Y-%m-%d %H:%M:%S')}')"
    # )
    #
    # sql = get_data_sql(query).set_index("EVENT_TIME")
    # tag = 'U/S Water Level'
    # sql = sql.loc[sql.TAG_DESC == tag, "EVENT_VALUE"]
    # orcl = orcl.loc[orcl.TAG_DESC == tag, "EVENT_VALUE"]
    # idx = orcl.loc[orcl == sql[0]].index[0]
    # sql.index = sql.index + (idx - sql.index[0])
    # orcl = orcl.reindex(sql.index, method='nearest')
    # ax = (sql - orcl).plot(label="SQL - ORACLE")
    # plt.legend()
    # plt.show()
    # plt.cla()
    if not oracle:
        utc = pd.datetime.utcnow().astimezone().utcoffset()
        time_last -= utc
        time_first -= utc
        query = (
            f" SELECT EVENT_TIME, Tags.TAG_DESC, EVENT_VALUE, Tags.TAG_ID"
            f"    FROM EVENTS INNER JOIN Tags ON EVENTS.TAG_ID = Tags.TAG_ID"
            f" WHERE OBJECT_NO = ( select OBJECT_NO from Objects WHERE ASSET_CODE = '{asset_code}') "
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
    #print(df.to_string())
    USL = df.loc[df["TAG_DESC"] == 'U/S Water Level'].set_index("EVENT_TIME")
    DSL = df.loc[df["TAG_DESC"] == 'D/S Water Level'].set_index("EVENT_TIME")

    GATES = pd.DataFrame()
    for i in range(1, no_gates + 1):
        G = df.loc[df["TAG_DESC"] == f'Gate {i} Elevation'].set_index("EVENT_TIME")["EVENT_VALUE"]
        G.columns = pd.Index([f"{i}"])
        GATES = GATES.join(G, how="outer", lsuffix="l_")
    GATES = GATES.interpolate()
    G_av = GATES.sum(axis=1) / no_gates

    CF = df.loc[df["TAG_DESC"] == "Current Flow"].set_index("EVENT_TIME")["EVENT_VALUE"] / 86.4

    out = pd.merge(USL["EVENT_VALUE"], DSL["EVENT_VALUE"], "inner", on="EVENT_TIME", suffixes=("_USL", "_DSL"))
    out = out.join(G_av.to_frame(), how="inner", rsuffix="_G_av")
    out.columns = pd.Index(["USL", "DSL", "G_av"])

    Qs = []
    for idx in out.index.values:
        Q = _Q_flume(h1=out.loc[idx, "USL"] - out.loc[idx, "G_av"],
                     h2=out.loc[idx, "DSL"] - out.loc[idx, "G_av"],
                     alpha=alpha,
                     beta=beta,
                     b=no_gates * gate_width)
        Qs.append(Q)  # m3/

    out["FG_flow_calc"] = Qs
    out["FG_flow_calc"].interpolate(inplace=True)
    CF.interpolate(inplace=True)
    ax = out.plot()

    first = out.index.values[0]
    delta_t = [pd.Timedelta(td - first).total_seconds() for td in out.index.values]
    FG_integral = integrate.cumtrapz(y=out["FG_flow_calc"].values, x=delta_t) / 1000
    FG_integral = pd.Series(data=FG_integral, index=out.index.values[1:]).transpose()
    FG = max(FG_integral)
    label = f"FG_flow: INTEGRAL -> {FG:.1f} ML"
    print(label)

    CF.plot(label="CURRENT FLOW", ax=ax)
    delta_t = [pd.Timedelta(td - first).total_seconds() for td in CF.index.values]
    CF_integral = integrate.cumtrapz(y=CF.values, x=delta_t) / 1000  # ?
    CF_integral = pd.Series(data=CF_integral, index=CF.index.values[1:]).transpose()
    CF = max(CF_integral)
    label = f"CF: INTEGRAL -> {CF:.1f} ML"
    print(label)
    ax.legend()
    plt.title = asset_code
    if show:
        plt.show()
    else:
        plt.close()

    return FG


def volume(obj_data: pd.DataFrame, objects: list, period_start, period_end, show=False, verbose=False) \
        -> Tuple[pd.DataFrame, list]:
    """

    :param obj_data: all the data for the search in the period ('EVENT_TIME', 'FLOW_ACU_SR', 'FLOW_VAL')
    :param objects: list of object no.s that you would like to calculate volumes from obj_data
    :param period_start: RFU
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
        if link_obj in obj_data.index:
            df = obj_data.loc[link_obj]
            if isinstance(df, pd.DataFrame):
                # collect RTU data (primary source)
                RTU_df = df.loc[df["TAG_NAME"] == 'FLOW_ACU_SR'].sort_values(by=["EVENT_TIME"])
                if show: ax = RTU_df.plot(x="EVENT_TIME", y="EVENT_VALUE", label="RTU_SOURCE")
                RTU_df = fix_resets(RTU_df)
                if show: RTU_df.plot(x="EVENT_TIME", y="EVENT_VALUE", ax=ax, label="RTU_INTEGRAL")
                RTU = 0.0 if RTU_df.empty else (RTU_df["EVENT_VALUE"].max() - RTU_df.head(1)["EVENT_VALUE"].iat[
                    0]) * 1000

                # calculate INTEGRAL (secondary source)
                FLOW_df = df.loc[df["TAG_NAME"] == 'FLOW_VAL'].sort_values(by=["EVENT_TIME"])
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

                if show:
                    plt.title = f"{link_obj}"
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
            volume, date = get_manual_meter(link_obj, period_end)

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
