#!/usr/bin/env python3
"""
mapper.py - UAE Weather MapReduce Mapper
Compatible with Python 3.5+ (no f-strings)

Reads METAR CSV records from stdin.
Emits: AIRPORT_YYYY-MM-DD_HH TAB temp_f,humidity,visibility,wind_knots
"""

import sys


def safe_float(value, default=0.0):
    try:
        if str(value).strip().upper() == 'M':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


for line in sys.stdin:
    line = line.strip()

    # Skip header row
    if line.startswith('station') or line.startswith('"station"'):
        continue

    # Skip empty lines
    if not line:
        continue

    parts = line.split(',')

    # Need at least 11 columns to reach vsby at index 10
    if len(parts) < 11:
        continue

    try:
        station    = parts[0].strip().strip('"')
        timestamp  = parts[1].strip().strip('"')
        temp_f     = safe_float(parts[2])
        humidity   = safe_float(parts[4])
        wind_knots = safe_float(parts[6])
        visibility = safe_float(parts[10])

        if not station or not timestamp or len(timestamp) < 13:
            continue

        # Build key: AIRPORT_DATE_HOUR e.g. OMDB_2023-01-01_00
        date_hour = timestamp[:13].replace(' ', '_')
        key = "{0}_{1}".format(station, date_hour)

        # Emit key TAB values  (Python 3.5 safe: no f-strings)
        print("{0}\t{1},{2},{3},{4}".format(
            key, temp_f, humidity, visibility, wind_knots))

        sys.stderr.write("EMITTING: {0}\n".format(key))

    except Exception as e:
        sys.stderr.write("MAPPER ERROR: {0} on line: {1}\n".format(
            str(e), line[:80]))
        continue