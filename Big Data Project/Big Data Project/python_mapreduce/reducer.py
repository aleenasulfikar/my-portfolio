#!/usr/bin/env python3
"""
reducer.py - UAE Weather MapReduce Reducer
Compatible with Python 3.5+ (no f-strings)

Receives sorted key-value pairs from mapper.
Outputs per-hour averages per airport.

Output format: AIRPORT_DATE_HOUR#avg_temp_f#avg_humidity#avg_visibility#avg_wind
Example:        OMDB_2023-01-01_00#71.6#53.03#6.21#6.0
"""

import sys

current_key  = None
sum_temp     = 0.0
sum_humidity = 0.0
sum_vis      = 0.0
sum_wind     = 0.0
count        = 0


def emit_result(key, st, sh, sv, sw, n):
    if n == 0:
        return
    print("{0}#{1}#{2}#{3}#{4}".format(
        key,
        round(st / n, 2),
        round(sh / n, 2),
        round(sv / n, 2),
        round(sw / n, 2)
    ))


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) != 2:
        continue

    key        = parts[0].strip()
    values_str = parts[1].strip()

    try:
        vals = values_str.split(',')
        if len(vals) != 4:
            continue
        temp = float(vals[0])
        hum  = float(vals[1])
        vis  = float(vals[2])
        wind = float(vals[3])
    except (ValueError, IndexError):
        continue

    if current_key is not None and key != current_key:
        emit_result(current_key, sum_temp, sum_humidity, sum_vis, sum_wind, count)
        sum_temp = sum_humidity = sum_vis = sum_wind = count = 0

    current_key   = key
    sum_temp     += temp
    sum_humidity += hum
    sum_vis      += vis
    sum_wind     += wind
    count        += 1

# Emit final group
if current_key is not None:
    emit_result(current_key, sum_temp, sum_humidity, sum_vis, sum_wind, count)