"""A water balance tool.
Takes LVBC (described by a linkage table) and performs a water balance to create a system efficiency report"""
from typing import Tuple

__author__ = "Samuel Hutchinson @ Murrumbidgee Irrigation"
__email__ = "samuel.hutchinson@mirrigation.com.au"

from topology_linker.projects.water_balance import water_balance
import pandas as pd

#I made a mistake when naming end and start a long time ago - they actually refer to their opposites
period_start = pd.datetime(year=2020, month=5, day=2, hour=00)
period_end = pd.datetime(year=2020, month=6, day=2, hour=00)

while period_start >= pd.datetime(year=2019, month=6, day=2, hour=00):
    #water_balance("LAKES_RD_A", '23635', '63898', "LAKES_RD_A.csv", period_start, period_end, area=18750.0, export=True,use_regs=False, show=False, debug=False)
    #water_balance("LAKES_RD_B", '23635', '63898', "LAKES_RD_B.csv", period_start, period_end, area=18750.0, export=True,use_regs=False, show=False, debug=False)
    water_balance("LAKES_RD_C", '23635', '63898', "LAKES_RD_C.csv", period_start, period_end, area=18750.0, export=True,
                  use_regs=False, show=False, debug=False)
    year, month = period_end.year, period_end.month
    month -= 1
    if month == 0:
        year -= 1
        month = 12

    period_end = pd.datetime(year, month, day=2, hour=0)

    year, month = period_start.year, period_start.month
    month -= 1
    if month == 0:
        year -= 1
        month = 12
    period_start = pd.datetime(year, month, day=2, hour=0)