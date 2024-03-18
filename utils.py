import numpy as np
from datetime import datetime, timedelta
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pymongo

def find_common_prefix(strings):
    if not strings:
        return ""
    
    prefix = strings[0]
    for string in strings[1:]:
        while string[:len(prefix)] != prefix:
            prefix = prefix[:-1]
            if not prefix:
                return ""
    
    return prefix

def extract_common_prefix(strings):
    common_prefix = find_common_prefix(strings)
    if not common_prefix:
        return [""] * len(strings)
    
    extracted_prefixes = []
    for string in strings:
        if string.startswith(common_prefix):
            extracted_prefixes.append(string[len(common_prefix):])
        else:
            extracted_prefixes.append(string)
    
    return extracted_prefixes


def get_caco_entries(client, variable, dict_caco_names, tstart, tstop):
    
    if variable != None:
        # We search for the variable inside all caco collections
        for coll in dict_caco_names.keys():
            for name in dict_caco_names[coll]:
                if variable == name:
                    var_collection = coll
                    break

        # Then we open this collection with the specified query
        query_caco = {"date" : {"$gte" : tstart, "$lte" : tstop}, "name" : variable}
        collection = client.CACO[var_collection].find(query_caco)

        # Getting the values and the dates
        dates, values = [], []
        for entrie in collection:

            tbase = entrie["date"]
            for second in list(entrie["values"]):
                date  = tbase + timedelta(seconds=int(second))
                value = entrie["values"][second]
                dates.append(date)
                values.append(value)
            
            
    else:
        var_collection = None
        dates, values = [], []
                
    
    # Store in a dictionary as output
    dict_out = {
        "name" : variable,
        "collection" : var_collection,
        "time" : dates,
        "value" : values,
    }
    
    return dict_out


def format_time_ticks_axes(ax, lim_m, lim_M, timespan):
    n_seconds = timespan.total_seconds()
    n_mins    = n_seconds / 60
    n_5mins   = n_seconds / 60 * 5
    n_10mins  = n_seconds / 60 * 10
    n_15mins  = n_seconds / 60 * 15
    n_30mins  = n_seconds / 60 * 30
    n_hours   = n_seconds / 3600
    n_days    = n_seconds / 3600 / 24
    n_months  = n_seconds / 3600 / 24 / 30.4
    n_years   = n_seconds / 3600 / 24 / 30.4 / 365

    # Days as axis
    if n_days > 20:
        pass
    elif n_hours > 20:
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%d'))
    elif n_30mins > 20:
        ax.xaxis.set_major_locator(mdates.HourLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_15mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_10mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_5mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_seconds > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))