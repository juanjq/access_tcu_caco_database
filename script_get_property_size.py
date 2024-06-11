import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import timedelta
import pymongo
import pickle
import bson
import subprocess
import sys

# Other external scripts
import lst1_mongodb_example
import utils


dict_caco_states = {0 : "OFF", 1 : "DATA_MONITORING", 2 : "MONITORED", 3 : "SAFE", 4 : "STANDBY",
                    5 : "READY", 6 : "OBSERVING", 7 : "TPOINT", 8 : "UNDEFINED", 9 : "TRANSITIONAL", 10 : "ERROR"}
dict_caco_state_colors = {0 : "k", 1 : "darkviolet", 2 : "darkviolet", 3 : "lime", 4 : "gold",
                          5 : "c", 6 : "darkblue", 7 : "b", 8 : "r", 9 : "darkorange", 10 : "r"}


import argparse
import datetime
import pymongo
import json
import astropy.time
import matplotlib.dates as mdates

from matplotlib import pyplot


def get_entries(mongo_client, property_name, astropy_time=True, tstart=None, tstop=None):
    """
    Retrieves the data base entries for the specified property, 
    optionally converting time to astropy.time.Time instances.
    
    Parameters
    ----------
    mongo_client: pymongo.mongo_client.MongoClient
        MongoDB client to use
    property_name: str
        Name of the data base property to retrieve
    astropy_time: bool, optional
        Whether to convert the time stamps to astropy.time.Time .
        If False, the original (datetime.datetime) instances are kept.
        Defaults to True.
    tstart: datetime.datetime, optional
        Start date for the return entries. If None, tstart condition will be ignored.
        Defaults to None.
    tstop: datetime.datetime, optional
        Stop date for the return entries. If None, tstop condition will be ignored.
        Defaults to None.
        
    Returns
    -------
    entries: dict
        Dictionary containing:
            'name': the name of the property (property_name argument)
            'time': list of time stamps (datetime.datetime or astropy.time.Time)
            'values': list of corresponding values
    """
    
    property_collection = mongo_client['bridgesmonitoring']['properties']
    chunk_collection = mongo_client['bridgesmonitoring']['chunks']
    
    descriptors = property_collection.find({'property_name': property_name})
    
    entries = {
        'name': property_name,
        'time': [],
        'value': []
    }

    for desc in descriptors:
        query = {'pid': desc['_id']}
        
        if tstart is not None:
            query.update({"begin": {"$gte": tstart}})
        
        if tstop is not None:
            query.update({"end": {"$lte": tstop}})
            
        chunks = chunk_collection.find(query)
        
        for chunk in chunks:
            for value in chunk['values']:
                entries['time'].append(value['t'])
                entries['value'].append(value['val'])
            
    if astropy_time:
        isodate = [t.isoformat() for t in entries['time']]
        entries['time'] = astropy.time.Time(isodate, format='isot', scale='utc')
    
    return entries



def main(query_property):

    client_tcu  = pymongo.MongoClient("lst101-int:27017")
    client_caco = pymongo.MongoClient("lst101-int:27018")
    
    variable_tcu = "CameraControl_FSM_state"
    tstart = datetime.datetime.fromisoformat("2024-02-25-00:00:00")
    tstop  = datetime.datetime.fromisoformat("2024-04-01-00:00:00")
    
    
    out_tcu = lst1_mongodb_example.get_entries(client_tcu, variable_tcu, astropy_time=False, tstart=tstart, tstop=tstop)
    date_tcu, value_tcu = np.array(out_tcu["time"]), np.array(out_tcu["value"])
    
    value_str_tcu = np.array([dict_caco_states[v] for v in value_tcu])


    # Creating a timespan dictionary for each key
    timespans_dict = {}
    for key in dict_caco_states.keys():
        timespans_dict[key] = {"tstart" : [], "tstop" : [], "tspan" : []}
    
    before_state = value_tcu[0]
    initial_time = date_tcu[0]
    for i in range(len(value_tcu))[1:]:
        
        actual_state = value_tcu[i]
    
        if before_state != actual_state:
            final_time = date_tcu[i]
    
            timespans_dict[actual_state]["tstart"].append(initial_time)
            timespans_dict[actual_state]["tstop"].append(final_time)
            
            timespans_dict[actual_state]["tspan"].append((final_time - initial_time).total_seconds())
    
            initial_time = date_tcu[i]
            
        before_state = actual_state


    with open("objects/camera_related_tcu_properties.pkl", "rb") as f:
        camera_related_tcu_properties = pickle.load(f)

    with open("objects/dict_tcu_property_bytes.pkl", "rb") as f:
        dict_tcu_property_bytes = pickle.load(f)

    # Creating a dictionary to store the information
    event_dict = {}
    for key in dict_caco_states.keys():
        event_dict[key] = {}
    
    # Taking the main collections and chunks pointers
    # property_collection = client_tcu['bridgesmonitoring']['properties']
    chunk_collection = client_tcu['bridgesmonitoring']['chunks']
    
    _property = query_property
    print(f"Finding data of property {_property}...")
    # descriptors = property_collection.find({'property_name': _property})

    _out_tcu = lst1_mongodb_example.get_entries(client_tcu, _property, astropy_time=False, tstart=tstart, tstop=tstop)
    _time = np.array(_out_tcu["time"]) # , _value = out_tcu["time"], out_tcu["value"]
    timestamps_unix = np.array([t.timestamp() for t in _time])

    for state in dict_caco_states.keys():

        print(f"Extracting for state {state} : {dict_caco_states[state]}")
        # print(f"The number of timespans is -> {len(timespans_dict[state]['tstart'])}")

        # Binning for timestamps
        timebins = np.array([*timespans_dict[state]["tstart"], *timespans_dict[state]["tstop"][-1:]])
        bins_unix = np.array([t.timestamp() for t in timebins])
        
        # Get the counts per bin
        counts, _ = np.histogram(timestamps_unix, bins=bins_unix)

        event_dict[state]["size"] = counts * dict_tcu_property_bytes[_property]

    # Saving the object
    with open(f"objects/tmp/dict_property_{query_property}.pkl", "wb") as f:
        pickle.dump(event_dict, f, pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    input_str = sys.argv[1]
    main(input_str)