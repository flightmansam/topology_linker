import pandas as pd
import scipy.integrate as integrate
import matplotlib.pyplot as plt

df = pd.read_csv("../res/scotts.csv", parse_dates=["EVENT_TIME"])
df.fillna(0.0, inplace=True)
#df.set_index("EVENT_TIME", inplace=True)
df["EVENT_TIME"] += pd.Timedelta(hours=11)

start = pd.datetime(year=2020, month=1, day=3)
end = pd.datetime(year=2020, month=1, day=4)

df = df.loc[(df["EVENT_TIME"] >= start) & (df["EVENT_TIME"] <= end)]

first = df.head(1)["EVENT_TIME"].values[0]
delta_t = [pd.Timedelta(td - first).total_seconds() for td in df["EVENT_TIME"].values]

FG_integral = integrate.cumtrapz(y=df["FG_flow"].values / 86.4, x=delta_t) / 1000
FG_integral = pd.Series(data=FG_integral, index=df["EVENT_TIME"].values[1:]).transpose()
label = f"FG_flow: INT-> {max(FG_integral):.1f} ML"
ax = FG_integral.plot(label=label)

SL_integral = integrate.cumtrapz(y=df["Q_SL"].values / 86.4, x=delta_t) / 1000
SL_integral = pd.Series(data=SL_integral, index=df["EVENT_TIME"].values[1:]).transpose()
label = f"Q_SL: INT-> {max(SL_integral):.1f} ML"
SL_integral.plot(label=label, ax=ax)
plt.legend()
plt.show()