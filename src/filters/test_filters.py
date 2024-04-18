import numpy  as np
import pandas as pd

from typing import Union, List

def evt_filter(evts: Union[int, List[int]], dfs: List[pd.core.frame.DataFrame]) -> List[pd.core.frame.DataFrame]:
    #Filter Pandas DataFrame by Event Number (EvtNum)
    filter_dfs = []

    for df in dfs:
        if isinstance(evts, int):
            filter_df = df[df['EvtNum'].values == evts]
        elif isinstance(evts, list):
            filter_df = df[df['EvtNum'].isin(evts)]
        else:
            raise ValueError("Invalid event type. Expected an integer or a list.")

        filter_dfs.append(filter_df)

    return filter_dfs

# SCINTILLATION LIGHT FUNCTIONS
def electrons_from_Scintillation(df, nevents):

    # Take into account not only electrons may produce Scintillation, but for simplicity reasons we leave this like that.

    # Create a dictionary where the key is the event number and the info are the electrons that produced 
    # Scintillation light that produced hits in our PMTs.

    # Initialize the dictionary that's gonna record the info
    electrons_that_produce_scintillation = {}

    # Loop over all events
    for i in nevents:
        temp_df = df[df['EvtNum'].values == i]

        # If the event only has one track, append the track parent in a special way for the code not to break
        if len(temp_df) == 1:
            electrons_that_produce_scintillation[i] = list(temp_df['PrimaryParentID'].values)

        # In every other scenario, append the track parents unique appearances
        else:
            electrons_that_produce_scintillation[i] = list(np.unique(temp_df['PrimaryParentID'].values))

    return electrons_that_produce_scintillation

def real_scintillation_electrons(df, e, nevents):

    # Create a dictionary where the key is the event number and the info are the electron which produced
    # Scintillation light (that produced hits) that comes from the tag gamma

    # Initialize the dictionary
    el = {}

    # Loop over the events in which we have scintillation
    for i in tqdm(nevents, desc="Running real_scintillation_electrons", unit="iter", dynamic_ncols=True):

        # Create a sub-DataFrame with the data of the current event.
        # This speeds up the process 
        temp_df = df[df['EvtNum'].values == i]

        # Fill the dictionary with the track ID of those electrons that come from track 2, 
        # which is the tag gamma. Again, creating a temp DF is faster.
        el_temp = temp_df[temp_df['Id'].isin(e[i])]
        el[i]   = sorted(el_temp[el_temp['ParentId'].values == 2]['Id'].values)

    # Finally, if the dictionary is empty for one of these events, drop that index
    re = {}
    for i in nevents:
        if len(el[i]) == 0:
            pass
        else:
            re[i] = el[i]

    return re

def scintillation_info(e, df):
    events_scint  = []
    counts_scint  = []
    indices_scint = []

    # Loop over the events in which we have electrons from tag gamma
    for i in tqdm(e.keys(), desc="Running scintillation_info", unit="iter", dynamic_ncols=True):
        count = 0

        # Create a sub-DataFrame with the info of this event
        temp_df = df[df['EvtNum'] == i]
        indices = temp_df['PhotonIDParent'].index
        values  = temp_df['PhotonIDParent']

        # Loop over the hit parent of the digihits in the event and the index in the DF
        for j,l in zip(values, indices):
            # Loop over the electrons that come from the tag gamma in this event
            for k in e[i]:
                # If we have DigiHits for the current event and the electron appears in the
                # DigiHits DataFrame, count and append the index
                if temp_df.notna().any()[1] and k in j:
                    count += 1
                    indices_scint.append(l)

        # If we are in a event in which some of the DigiHits are created by the tag gamma,
        # append the events and the counts
        if count != 0:
            events_scint.append(i)
            counts_scint.append(count)

    return events_scint, counts_scint, indices_scint


# CHERENKOV LIGHT FUNCTIONS 
def electrons_from_Cherenkov(df, nevents):

    # Create a dictionary where the key is the event number and the info are the electrons that produced
    # Cherenkov light that produced hits in our PMTs.

    # Initialize the dictionary that's gonna record the info
    electrons_that_produce_cherenkov = {}

    # Loop over all events
    for i in nevents:
        temp_df = df[df['EvtNum'].values == i]

        # If the event only has one track, append the track parent in a special way for the code not to break
        if len(temp_df) == 1:
            electrons_that_produce_cherenkov[i] = list(temp_df['PrimaryParentID'].values)

        # In every other scenario, append the track parents unique appearances
        else:
            electrons_that_produce_cherenkov[i] = list(np.unique(temp_df['PrimaryParentID'].values))

    return electrons_that_produce_cherenkov

def real_cherenkov_electrons(df, e, nevents):

    # Create a dictionary where the key is the event number and the info are the electron which produced
    # Cherenkov light (that produced hits) that comes from the nCapture

    # Initialize the dictionary
    el = {}

    # Loop over the events in which we have nCapture and Cherenkov
    for i in tqdm(nevents, desc="Running real_cherenkov_electrons", unit="iter", dynamic_ncols=True):
        # Create a sub-DataFrame with the data of the current event.
        # This incredibly speeds up the process
        temp_df = df[df['EvtNum'].values == i]

        # Look for the track ID of the nCapture Gamma in this event
        temp_ncID_df = temp_df[(temp_df['Ipnu'].values == 22)]
        ncID         = temp_ncID_df[(temp_ncID_df['CreatorProcessName'].values == "nCapture")]['Id'].values[0]

        # Take only the electrons which have been produced by the nCapture gamma
        temp_df = temp_df[(temp_df['Id'].isin(e[i]))]
        el[i] = sorted(temp_df[(temp_df['ParentId'].values == ncID)]['Id'].values)

    re = {}
    for i in nevents:
        if len(el[i]) == 0:
            pass
        else:
            re[i] = el[i]

    return re

def anyCherenkov_info(e, df):
    counts_anyCher = []
    events_anyCher = []

    for i in tqdm(e.keys(), desc="Running anyCherenkov info", unit="iter", dynamic_ncols=True):
        count = 0

        temp_df = df[df['EvtNum'].values == i]

        for j in temp_df['PhotonIDParent']:
            for k in e[i]:
                if temp_df.notna().any()[1] and k in j:
                    count += 1

        if count != 0:
            events_anyCher.append(i)
            counts_anyCher.append(count)

    return events_anyCher, counts_anyCher

def nCapture_Cherenkov_info(e, df):
    events_nCCher  = []
    counts_nCCher  = []
    indices_nCCher = []

    # Loop over the events in which we have electrons from the nCapture gamma
    for i in tqdm(e.keys(), desc="Running nCapture_Cherenkov_info", unit="iter", dynamic_ncols=True):
        count = 0
        # Create a sub-DataFrame of the current event
        temp_df = df[df['EvtNum'].values == i]

        # Loop over the hit parents that created the DigiHits and the index
        for j, l in zip(temp_df['PhotonIDParent'], temp_df['PhotonIDParent'].index):
            # Loop over the electrons created by the nCapture gamma in this event
            for k in e[i]:
                # If the electron is in the DigiHits DF, append the index and count
                if temp_df.notna().any()[1] and k in j:
                    indices_nCCher.append(l)
                    count += 1

        # If we are in an event we want to append, do it and print info
        if count != 0:
            events_nCCher.append(i)
            counts_nCCher.append(count)

    return events_nCCher, counts_nCCher, indices_nCCher
