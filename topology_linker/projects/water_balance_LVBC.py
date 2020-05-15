"""A water balance tool.
Takes LVBC (described by a linkage table) and performs a water balance to create a system efficiency report"""
from typing import Tuple

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

from topology_linker.projects.water_balance import water_balance
import pandas as pd

#I made a mistake when naming end and start a long time ago - they actually refer to their opposites
period_start = pd.datetime(year=2019, month=12, day=16, hour=00)
period_end = pd.datetime(year=2020, month=1, day=17, hour=00)

water_balance("LVBC", '29355', '65041', "../out/LINKED.csv", period_start, period_end, area=272747.0, export=True,use_regs=False, show=False, debug=True)


