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


