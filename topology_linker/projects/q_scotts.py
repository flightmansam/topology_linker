"""Collect gate positions and U/S and D/S water level for Scotts from the Hydrology SQL table
and calculate the flow from that period."""
from typing import Union

from scipy import integrate

from topology_linker.res.FGinvestigation.fginvestigation.extraction import get_data_sql
import matplotlib.pyplot as plt
import pandas as pd
from topology_linker.src.utils import Q_flume
asset_code = 'RG-2-698'

def pull(asset_code:str, tags: Union[str, tuple], time_first:pd.datetime, time_last:pd.datetime):
    if isinstance(tags, list):
        tags = tuple(tags)
    if isinstance(tags, str):
        tags = '(' + tags + ')'
    query = (
        f" SELECT EVENT_TIME, Tags.TAG_DESC, EVENT_VALUE"
        f"    FROM EVENTS INNER JOIN Tags ON EVENTS.TAG_ID = Tags.TAG_ID"
        f" WHERE OBJECT_NO = ( select OBJECT_NO from Objects WHERE ASSET_CODE = '{asset_code}') "
        f"    AND TAG_DESC in {tags}"
        f"    AND (EVENT_TIME >= '{time_first.strftime('%Y-%m-%d %H:%M:%S')}')"
        f"    AND (EVENT_TIME <= '{time_last.strftime('%Y-%m-%d %H:%M:%S')}')"
    )
    return get_data_sql(query)

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
    Qs.append(Q) # m3/s

out["FG_flow_calc"] = Qs

ax = out.plot()
#plt.show()

first = out.index.values[0]
delta_t = [pd.Timedelta(td - first).total_seconds() for td in out.index.values]

FG_integral = integrate.cumtrapz(y=out["FG_flow_calc"].values * 86.4, x=delta_t) / 100000
FG_integral = pd.Series(data=FG_integral, index=out.index.values[1:]).transpose()
label = f"FG_flow: INTEGRAL -> {max(FG_integral):.1f} ML"
ax2:plt.Axes = ax.twinx()
ax2.set_y
FG_integral.plot(ax=ax2)

CF.plot(label="CURRENT FLOW", ax = ax)
#FR.plot(label = "FLOW RATE", ax = ax)
ax.legend()
plt.show()
print()