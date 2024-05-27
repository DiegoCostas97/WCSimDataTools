import sys
import glob
import argparse
import numpy  as np
import tables as tb
import ROOT
import uproot
uproot.default_library = "np"

from .descriptions import *
from os.path       import expandvars, realpath, join
from tqdm import tqdm

def main():

    parser = argparse.ArgumentParser( prog        = "wcsim_root_to_hdf5"
                                    , description = "Converts WCSim .root files into .hdf5"
                                    , epilog      = "Text at the bottom of help")

    parser.add_argument( "--wcsimlib", type=str, nargs="?", default = expandvars("$HOME/WCTE/Software/WCSim/build/")
                       , help = "WCSim library path")

    parser.add_argument("-v", "--verbose", action="store_true")

    parser.add_argument( "infiles", type=str, nargs="+", help = ".root files")
    parser.add_argument( "-o", "--outpath", type=str, nargs="?", help = ".hdf5 file path", default=".")

    args = parser.parse_args()

    ROOT.gSystem.AddDynamicPath(args.wcsimlib)
    ROOT.gSystem.Load          ("libWCSimRoot.dylib" if sys.platform == "darwin" else "libWCSimRoot.so")

    root_fnames  = args.infiles
    hdf5_outpath = realpath(expandvars(args.outpath))

    for f, root_fname in enumerate(root_fnames, 1):

        if args.verbose:
            print(f"Processing file ({f}/{len(root_fnames)}):", root_fname.split("/")[-1])
            print("----------------")

        rootf = ROOT.TFile(root_fname, "read")

        hdf5_fname = join(hdf5_outpath, root_fname.split("/")[-1].split(".root")[0] + ".h5")
        h5f = tb.open_file(hdf5_fname, mode="w", title="wcsim")

        # ----------------
        #     Settings
        # ----------------
        with uproot.open(root_fname) as f:
            table = h5f.create_table("/", "Settings", Settings, "Settings")
            row = table.row
            for key in f["Settings"].keys():
                row[key] = f[f"Settings/{key}"].array()[0]
            row.append()
            table.flush()


        # ----------------
        #     wcsimGeoT
        # ----------------
        geo_group = h5f.create_group("/", "wcsimGeoT", "wcsimGeoT")
        geo_table = h5f.create_table(geo_group, "Geometry", Geometry, "Geometry")
        pmt_table = h5f.create_table(geo_group,      "PMT",      PMT, "PMT")

        tree  = rootf.GetKey("wcsimGeoT").ReadObj()
        tree.GetEvent(0)
        geom  = tree.wcsimrootgeom

        row = geo_table.row
        row["WCCylRadius"] = geom.GetWCCylRadius()
        row["WCCylLength"] = geom.GetWCCylLength()
        row["Geom_Type"]   = geom.GetGeo_Type   ()
        row["WCNumPMT"]    = geom.GetWCNumPMT   ()
        row["WCPMTRadius"] = geom.GetWCPMTRadius()
        row["WCOffset"]    =[geom.GetWCOffset   (i) for i in range(3)]
        row.append()
        geo_table.flush()

        row = pmt_table.row
        for i in range(geom.GetWCNumPMT()):
            pmt = geom.GetPMT(i)
            row["TubeNo"]      =  pmt.GetTubeNo()
            row["mPMTNo"]      =  pmt.GetmPMTNo()
            row["mPMT_PMTNo"]  =  pmt.GetmPMT_PMTNo()
            row["CylLoc"]      =  pmt.GetCylLoc()
            for i in range(3): row[f"Orientation_x{i}"] = pmt.GetOrientation(i)
            for i in range(3): row[   f"Position_x{i}"] = pmt.GetPosition   (i)
            row.append()
        pmt_table.flush()


        # ----------------
        #     wcsimT
        # ----------------
        wcsimT_group  = h5f.create_group("/", "wcsimT", "wcsimT")
        tracks_table  = h5f.create_table(wcsimT_group,            "Tracks",            Tracks, "Tracks")
        triggers_table= h5f.create_table(wcsimT_group,          "Triggers",          Triggers, "Triggers")
        chits_table   = h5f.create_table(wcsimT_group, "CherenkovHits"    ,     CherenkovHits, "CherenkovHits")
        chitts_table  = h5f.create_table(wcsimT_group, "CherenkovHitTimes", CherenkovHitTimes, "CherenkovHitTimes")

        dhits_group   = h5f.create_group  (wcsimT_group, "CherenkovDigiHits", "CherenkovDigiHits")
        dhits_table   = h5f.create_table  ( dhits_group,          "DigiHits", CherenkovDigiHits, "DigiHits")
        photonids_arr = h5f.create_vlarray( dhits_group,         "PhotonIDs",    tb.Int64Atom(), "PhotonIDs")
        hitPhotonId_arr = h5f.create_vlarray( dhits_group, "DigiHitsPhotonIdParent", tb.Int64Atom(), "PhotonIdParent")

        tree  = rootf.GetKey("wcsimT").ReadObj()
        nevents = tree.GetEntries()
        if args.verbose: print("Number of events:",  nevents)

        triggers_row = triggers_table.row

        for event_i in tqdm(range(nevents)):
            tree.GetEvent(event_i)
            ntriggers = tree.wcsimrootevent.GetNumberOfEvents()
            if args.verbose: print(f"Number of triggers in event {event_i}: {ntriggers}".ljust(50))

            hitTimes = 0

            for trigger_i in range(ntriggers):
                # print(f"Processing Trigger: {trigger_i}")
                trigger = tree.wcsimrootevent.GetTrigger(trigger_i)

                header       = trigger.GetHeader()
                run          = header.GetRun()
                date         = header.GetDate()
                evtnum       = header.GetEvtNum()
                subevtnumber = header.GetSubEvtNumber()

                triggers_row["Run"]              = run
                triggers_row["Date"]             = date
                triggers_row["EvtNum"]           = evtnum
                triggers_row["SubEvtNumber"]     = subevtnumber

                triggers_row["Mode"]               = trigger.GetMode()
                triggers_row["Vtxvol"]             = trigger.GetVtxvol()
                for i in range(3): triggers_row[f"Vtx_x{i}"] = trigger.GetVtx(i)
                triggers_row["VecRecNumber"]       = trigger.GetVecRecNumber()
                triggers_row["Jmu"]                = trigger.GetJmu()
                triggers_row["Jp"]                 = trigger.GetJp()
                triggers_row["Npar"]               = trigger.GetNpar()
                triggers_row["NumTubesHit"]        = trigger.GetNumTubesHit()
                triggers_row["NumDigiTubesHit"]    = trigger.GetNumDigiTubesHit()
                triggers_row["Ntrack"]             = trigger.GetNtrack()
                triggers_row["Ncaptures"]          = trigger.GetNcaptures()
                triggers_row["Ncherenkovhits"]     = trigger.GetNcherenkovhits()
                triggers_row["Ncherenkovhittimes"] = trigger.GetNcherenkovhittimes()
                triggers_row["Ncherenkovdigihits"] = trigger.GetNcherenkovdigihits()
                triggers_row["SumQ"]               = trigger.GetSumQ()
                triggers_row["TriggerType"]        = trigger.GetTriggerType()
                # triggers_row["TriggerInfo"]        = trigger.GetTriggerInfo()
                triggers_row.append()


                tracks = trigger.GetTracks()
                CHits  = trigger.GetCherenkovHits()
                CHitsT = trigger.GetCherenkovHitTimes()
                DHits  = trigger.GetCherenkovDigiHits()
                hitTimes = trigger.GetCherenkovHitTimes() if trigger_i == 0 else hitTimes

                # tracks
                row = tracks_table.row
                n   = tracks.GetEntries()
                for i in range(n):
                    t = tracks[i]
                    row["Run"]          = run
                    row["Date"]         = date
                    row["EvtNum"]       = evtnum
                    row["SubEvtNumber"] = subevtnumber
                    row["track"]    = i
                    row["Ipnu"]     = t.GetIpnu()
                    row["Flag"]     = t.GetFlag()
                    row["M"]        = t.GetM()
                    row["P"]        = t.GetP()
                    row["E"]        = t.GetE()
                    row["StartVol"] = t.GetStartvol()
                    row["StopVol" ] = t.GetStopvol()
                    for i in range(3): row[  f"Dir_x{i}"] = t.GetDir  (i)
                    for i in range(3): row[ f"PDir_x{i}"] = t.GetPdir (i)
                    for i in range(3): row[f"Start_x{i}"] = t.GetStart(i)
                    for i in range(3): row[ f"Stop_x{i}"] = t.GetStop (i)
                    row["Parenttype"] = t.GetParenttype()
                    row["ParentId"]   = t.GetParentId()
                    row["Time"]       = t.GetTime()
                    row["Id"]         = t.GetId()
                    row["CreatorProcessName"] = t.GetCreatorProcessName()
                    row.append()
                tracks_table.flush()

                # CHits
                row = chits_table.row
                n   = CHits.GetEntries()
                for i in range(n):
                    h = CHits[i]
                    row["Run"]          = run
                    row["Date"]         = date
                    row["EvtNum"]       = evtnum
                    row["SubEvtNumber"] = subevtnumber
                    row["chit"]       = i
                    row["TubeID"]     = h.GetTubeID()
                    row["mPMTID"]     = h.GetmPMTID()
                    row["mPMT_PMTID"] = h.GetmPMT_PMTID()
                    for i in range(2): row[f"TotalPe_{i}"] = h.GetTotalPe(i)
                    row.append()
                chitts_table.flush()

                # CHitsTT
                row = chitts_table.row
                n   = CHitsT.GetEntries()
                for i in range(n):
                    h = CHitsT[i]
                    row["Run"]          = run
                    row["Date"]         = date
                    row["EvtNum"]       = evtnum
                    row["SubEvtNumber"] = subevtnumber
                    row["chitt"]           = i
                    row["Truetime"]        = h.GetTruetime()
                    row["PrimaryParentID"] = h.GetParentID()
                    row["PhotonStartTime"] = h.GetPhotonStartTime()
                    for i in range(3): row[f"PhotonStartPos_x{i}"] = h.GetPhotonStartPos(i)
                    for i in range(3): row[f"PhotonEndPos_x{i}"]   = h.GetPhotonEndPos  (i)
                    row["PhotonCreatorProcessName"] = h.GetPhotonCreatorProcessName()
                    row.append()
                chitts_table.flush()

                # DHits
                row = dhits_table.row
                n   = DHits.GetEntries()
                # print(f"DHits.GetEntries(): {n}")
                for i in range(n):
                    h = DHits[i]
                    row["Run"]          = run
                    row["Date"]         = date
                    row["EvtNum"]       = evtnum
                    row["SubEvtNumber"] = subevtnumber
                    row["dhit"]       = i
                    row["Q"]          = h.GetQ()
                    row["T"]          = h.GetT()
                    row["TubeID"]     = h.GetTubeId()
                    row["mPMTID"]     = h.GetmPMTId()
                    row["mPMT_PMTID"] = h.GetmPMT_PMTId()
                    row.append()

                    photonids_arr.append(list(h.GetPhotonIds()))
                    temp_hitPhotonId = []
                    trueHitsInfo = []
                    for photon_id in list(h.GetPhotonIds()):
                        trueHitsInfo.append(hitTimes.At(photon_id))

                    for info in trueHitsInfo:
                        temp_hitPhotonId.append(info.GetParentID())

                    hitPhotonId_arr.append(temp_hitPhotonId)

                dhits_table  .flush()
                photonids_arr.flush()
                hitPhotonId_arr.flush()

        triggers_table.flush()

        # -----------------------
        #    wcsimRootOptionsT
        # -----------------------
        options_table = h5f.create_table("/", "wcsimRootOptionsT", Options, "wcsimRootOptionsT")

        tree = rootf.GetKey("wcsimRootOptionsT").ReadObj()
        tree.GetEvent(0)
        options = tree.wcsimrootoptions

        pmttag = "tank" #TODO: review this parameter

        row = options_table.row
        row["DetectorName"]                  = options.GetDetectorName()
        row["SavePi0"]                       = options.GetSavePi0()
        row["PMTQEMethod"]                   = options.GetPMTQEMethod()
        row["PMTCollEff"]                    = options.GetPMTCollEff()
        row["PMTDarkRate"]                   = options.GetPMTDarkRate(pmttag)
        row["ConvRate"]                      = options.GetConvRate   (pmttag)
        row["DarkHigh"]                      = options.GetDarkHigh   (pmttag)
        row["DarkLow"]                       = options.GetDarkLow    (pmttag)
        row["DarkWindow"]                    = options.GetDarkWindow (pmttag)
        row["DarkMode"]                      = options.GetDarkMode   (pmttag)
        row["DigitizerClassName"]            = options.GetDigitizerClassName()
        row["DigitizerDeadTime"]             = options.GetDigitizerDeadTime()
        row["DigitizerIntegrationWindow"]    = options.GetDigitizerIntegrationWindow()
        row["DigitizerTimingPrecision"]      = options.GetDigitizerTimingPrecision()
        row["DigitizerPEPrecision"]          = options.GetDigitizerPEPrecision()
        row["TriggerClassName"]              = options.GetTriggerClassName()
        row["MultiDigitsPerTrigger"]         = options.GetMultiDigitsPerTrigger()
        row["NDigitsThreshold"]              = options.GetNDigitsThreshold()
        row["NDigitsWindow"]                 = options.GetNDigitsWindow()
        row["NDigitsAdjustForNoise"]         = options.GetNDigitsAdjustForNoise()
        row["NDigitsPreTriggerWindow"]       = options.GetNDigitsPreTriggerWindow()
        row["NDigitsPostTriggerWindow"]      = options.GetNDigitsPostTriggerWindow()
        row["TriggerOffset"]                 = options.GetTriggerOffset()
        row["SaveFailuresMode"]              = options.GetSaveFailuresMode()
        row["SaveFailuresTime"]              = options.GetSaveFailuresTime()
        row["SaveFailuresPreTriggerWindow"]  = options.GetSaveFailuresPreTriggerWindow()
        row["SaveFailuresPostTriggerWindow"] = options.GetSaveFailuresPostTriggerWindow()
        row.append()
        options_table.flush()

        h5f.close()
    
    return


