import numpy as np
import pandas as pd
import h5py

def adjust_dtype(df, itemsize):
    arrays    = [df[col].to_numpy() for col in df.columns]
    formats   = [array.dtype if array.dtype != 'O' else array.astype("S"+str(itemsize)).dtype for array in arrays]
    rec_array = np.rec.fromarrays(arrays, dtype={'names': df.columns, 'formats': formats})

    return rec_array

def storeMultidimArray(array, dtype):
    array = np.array([i.astype(dtype) for i in array], dtype=np.object_)
    dt_array = h5py.special_dtype(vlen=dtype)

    return array, dt_array

# Definir la ruta del archivo HDF5 de salida
# output_file = "/Users/diiego/software/WCSimDataTools/filtered_ambeSourceData.hdf5"

def write(outputfile):
# Crear el archivo HDF5 y los grupos necesarios
    with h5py.File(output_file, mode='w') as f:
        # Settings
        settings_dset = f.create_dataset("Settings", data=settings_df)

        # wcsimRootOptionsT
        roptions_dset = f.create_dataset("wcsimRootOptionsT", data=adjust_dtype(roptions_df, 50))

        # wcsimGeoT
        wcsimGeoTGeometry_dset = f.create_dataset("/wcsimGeoT/Geometry", data=geometry_df)
        wcsimGeoTPMT_dset      = f.create_dataset("/wcsimGeoT/PMT",      data=pmt_df)

        # wcsimT # Any DF that has a string like column need the adjust_dtype function
        wcsimTCherenkovHitTimes = f.create_dataset("/wcsimT/CherenkovHitTimes", data=adjust_dtype(hittimes_df, 20))
        wcsimTCherenkovHits     = f.create_dataset("/wcsimT/CherenkovHits",     data=cherhits_df)
        wcsimTTracks            = f.create_dataset("/wcsimT/Tracks",            data=adjust_dtype(tracks_df,   20))
        wcsimTTriggers          = f.create_dataset("/wcsimT/Triggers",          data=adjust_dtype(triggers_df, 10))

        # wcsimT/CherenkovDigiHits
        wcsimTCherenkovDigiHitsDigiHits = f.create_dataset("/wcsimT/CherenkovDigiHits/DigiHits", data=df_final_cher)

        # Needed in order to store the multidimensional variable length array photonIDs is (this cannot be done in PyTables as long as I know)
        photonid_df, dt_photonid = storeMultidimArray(photonid_df, np.int32)
        photparn_df, dt_photparn = storeMultidimArray(photparn_df, np.int32)

        wcsimTCherenkovDigiHitsDigiHits = f.create_dataset("/wcsimT/CherenkovDigiHits/PhotonIDs", data=photonid_df, dtype=dt_photonid)
        wcsimTCherenkovDigiHitsDigiHits = f.create_dataset("/wcsimT/CherenkovDigiHits/DigiHitsPhotonIdParent", data=photparn_df, dtype=dt_photparn)
