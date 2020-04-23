
from topology_linker.projects.water_balance import water_balance
import pandas as pd


out = {
    'tabbita':[],
    'tysons':[],
    'l_258_2':[]
}

dates = pd.date_range('2019-06-16', periods=11, freq='M')
for i, date in enumerate(dates[:-1]):
    print(date)
    period_start = pd.datetime(year=date.year, month=date.month, day=16, hour=00)
    date = dates[i+1]
    period_end = pd.datetime(year=date.year, month=date.month, day=17, hour=00)

    export = True
    length=2200
    out['tabbita'].append(
        water_balance("TABBITA-1", '26215', '26228', "../out/TYSONS_LINKED.csv",
                      period_start, period_end,area = length*8, use_regs=False,
                      debug=True, export=export, out=['26228'])[1])

    length = 925+514.41+747.72
    out['tysons'].append(
        water_balance("TYSONS", '26165', '26215', "../out/TYSONS_LINKED.csv",
                      period_start, period_end,area = length*8, use_regs=False,
                      debug=True, export=export, out=['26215'])[1])

    length = 1257.15 + 0.5*(2620.9) #lateral is only 4m
    out['l_258_2'].append(
        water_balance("L258-2", '26137', '26165', "../out/TYSONS_LINKED.csv",
                      period_start, period_end,area = length*8, use_regs=True,
                      debug=True, export=export, out=['26165'])[1])

print(out)

