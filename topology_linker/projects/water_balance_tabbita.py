
from topology_linker.projects.water_balance import water_balance
import pandas as pd

from topology_linker.src.utils import subtract_one_month

period_start = pd.datetime(year=2020, month=1, day=16, hour=00)
period_end = pd.datetime(year=2020, month=2, day=17, hour=00)
length = 925+514.41+747.72
tysons = water_balance("TYSONS", '26165', '26215', "../out/TYSONS_LINKED.csv", period_start, period_end,area = length*8, use_regs=False, debug=True, show=True, out=['26215'])

length=2200
tabbita = water_balance("TABBITA-1", '26215', '26228', "../out/TYSONS_LINKED.csv", period_start, period_end,area = length*8, use_regs=False, debug=True, show=True, out=['26228'])

print()

