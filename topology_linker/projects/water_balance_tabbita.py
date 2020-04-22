
from topology_linker.projects.water_balance import water_balance
import pandas as pd

from topology_linker.src.utils import subtract_one_month

period_start = pd.datetime(year=2020, month=1, day=16, hour=00)
period_end = pd.datetime(year=2020, month=2, day=17, hour=00)
tysons = water_balance("TYSONS", '26165', '26215', "../out/TYSONS_LINKED.csv", period_start, period_end, use_regs=False, debug=True, show=True, out=['26215'])

tabbita = water_balance("TABBITA-1", '26215', '26228', "../out/TYSONS_LINKED.csv", period_start, period_end, use_regs=False, debug=True, show=True, out=['26228'])

print()

