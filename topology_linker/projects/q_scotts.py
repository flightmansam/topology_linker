"""Collect gate positions and U/S and D/S water level for Scotts from the Hydrology SQL table
and calculate the flow from that period."""


asset_code = 'RG-2-698'

def pull(asset_code:str, tags: Union[str, tuple], time_first:pd.datetime, time_last:pd.datetime):
    if isinstance(tags, list):
        tags = tuple(tags)
    if isinstance(tags, str):
        tags = '(' + tags + ')'

    oracle = False
    if not oracle:
        query = (
            f" SELECT EVENT_TIME, Tags.TAG_DESC, EVENT_VALUE"
            f"    FROM EVENTS INNER JOIN Tags ON EVENTS.TAG_ID = Tags.TAG_ID"
            f" WHERE OBJECT_NO = ( select OBJECT_NO from Objects WHERE ASSET_CODE = '{asset_code}') "
            f"    AND TAG_DESC in {tags}"
            f"    AND (EVENT_TIME >= '{time_first.strftime('%Y-%m-%d %H:%M:%S')}')"
            f"    AND (EVENT_TIME <= '{time_last.strftime('%Y-%m-%d %H:%M:%S')}')"
        )
        return get_data_sql(query)

    else:
        asset_code = '29355'
        query = (f"SELECT SC_EVENT_LOG.EVENT_TIME, SC_TAG.TAG_DESC, SC_EVENT_LOG.EVENT_VALUE "
                 f" FROM SC_EVENT_LOG INNER JOIN SC_TAG"
                 f" ON SC_EVENT_LOG.TAG_ID = SC_TAG.TAG_ID"
                 f" WHERE "
                 f" OBJECT_NO = {asset_code} AND TAG_DESC in {tags}"
                 f" AND EVENT_TIME >= TO_DATE('{time_first.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                 f" AND EVENT_TIME <= TO_DATE('{time_last.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
                 f" ORDER BY EVENT_TIME")
        return get_data_ordb(query)

no_gates = 4
df = pull(asset_code,
          [f'Gate {i} Elevation' for i in range(1, no_gates+1)]+['U/S Water Level', 'D/S Water Level', 'Flow Rate','Current Flow'],
          time_first=pd.datetime(2019, 12, 16, 00, 00),
          time_last=pd.datetime.now()) #eventually monthly

USL = df.loc[df["TAG_DESC"] == 'U/S Water Level'].set_index("EVENT_TIME")
DSL = df.loc[df["TAG_DESC"] == 'D/S Water Level'].set_index("EVENT_TIME")
G1 = df.loc[df["TAG_DESC"] == 'Gate 1 Elevation'].set_index("EVENT_TIME")
G2 = df.loc[df["TAG_DESC"] == 'Gate 2 Elevation'].set_index("EVENT_TIME")
G3 = df.loc[df["TAG_DESC"] == 'Gate 3 Elevation'].set_index("EVENT_TIME")
G4 = df.loc[df["TAG_DESC"] == 'Gate 4 Elevation'].set_index("EVENT_TIME")
CF = df.loc[df["TAG_DESC"] == "Current Flow"].set_index("EVENT_TIME")["EVENT_VALUE"] / 86.4
FR = df.loc[df["TAG_DESC"] == "Flow Rate"].set_index("EVENT_TIME")["EVENT_VALUE"]

G_av = 0.25 * (G1["EVENT_VALUE"] + G2["EVENT_VALUE"] + G3["EVENT_VALUE"] + G4["EVENT_VALUE"]).interpolate()

out = pd.merge(USL["EVENT_VALUE"], DSL["EVENT_VALUE"], "inner", on="EVENT_TIME", suffixes=("_USL", "_DSL"))
out = out.join(G_av, how="inner", rsuffix="_G_av")
out.columns = pd.Index(["USL", "DSL", "G_av"])

Qs = []
for idx in out.index.values:
    Q = Q_flume(h1=out.loc[idx, "USL"] - out.loc[idx, "G_av"],
                h2=out.loc[idx, "DSL"] - out.loc[idx, "G_av"],
                alpha=0.738,
                beta=0.282,
                b=4 * 0.937)
    Qs.append(Q) # m3/


out["FG_flow_calc"] = Qs

ax = out.plot()
#plt.show()

first = out.index.values[0]
delta_t = [pd.Timedelta(td - first).total_seconds() for td in out.index.values]
FG_integral = integrate.cumtrapz(y=out["FG_flow_calc"].values * 86.4, x=delta_t) / 100000 #?
FG_integral = pd.Series(data=FG_integral, index=out.index.values[1:]).transpose()
SCOTTS = max(FG_integral)
label = f"FG_flow: INTEGRAL -> {SCOTTS:.1f} ML"
print(label)
#ax2:plt.Axes = ax.twinx()

#FG_integral.plot(ax=ax2)

CF.plot(label="CURRENT FLOW", ax = ax)
delta_t = [pd.Timedelta(td - first).total_seconds() for td in CF.index.values]
CF_integral = integrate.cumtrapz(y=CF.values * 86.4, x=delta_t) / 100000 #?
CF_integral = pd.Series(data=CF_integral, index=CF.index.values[1:]).transpose()
CF = max(CF_integral)
label = f"CF: INTEGRAL -> {CF:.1f} ML"
print(label)
ax.legend()
plt.show()
print()