import tables as tb


class Settings(tb.IsDescription):
    GitHash     = tb.StringCol (pos=0, itemsize=50)
    WCDetRadius = tb.Float32Col(pos=1)
    WCDetHeight = tb.Float32Col(pos=2)
    WCDetCentre = tb.Float32Col(pos=3, shape=(3,))
    WCXRotation = tb.Float32Col(pos=4, shape=(3,))
    WCYRotation = tb.Float32Col(pos=5, shape=(3,))
    WCZRotation = tb.Float32Col(pos=6, shape=(3,))


class Tracks(tb.IsDescription):
    Run          = tb.Int64Col  (pos=0)
    Date         = tb.Int64Col  (pos=1)
    EvtNum       = tb.Int64Col  (pos=2)
    SubEvtNumber = tb.Int64Col  (pos=3)

    track        = tb.Int64Col  (pos=4)
    Ipnu         = tb.Int64Col  (pos=5)
    Flag         = tb.Int64Col  (pos=6)
    M            = tb.Float64Col(pos=7)
    P            = tb.Float64Col(pos=8)
    E            = tb.Float64Col(pos=9)
    StartVol     = tb.Int64Col  (pos=10)
    StopVol      = tb.Int64Col  (pos=11)
    Dir_x0       = tb.Float64Col(pos=12)
    Dir_x1       = tb.Float64Col(pos=13)
    Dir_x2       = tb.Float64Col(pos=14)
    PDir_x0      = tb.Float64Col(pos=15)
    PDir_x1      = tb.Float64Col(pos=16)
    PDir_x2      = tb.Float64Col(pos=17)
    Stop_x0      = tb.Float64Col(pos=18)
    Stop_x1      = tb.Float64Col(pos=19)
    Stop_x2      = tb.Float64Col(pos=20)
    Start_x0     = tb.Float64Col(pos=21)
    Start_x1     = tb.Float64Col(pos=22)
    Start_x2     = tb.Float64Col(pos=23)
    Parenttype   = tb.Int64Col  (pos=24)
    ParentId     = tb.Int64Col  (pos=25)
    Time         = tb.Float64Col(pos=26)
    Id           = tb.Int64Col  (pos=27)
    CreatorProcessName = tb.StringCol(pos=28, itemsize=20)


class Triggers(tb.IsDescription):
    Run               = tb.Int64Col(pos=0)
    Date              = tb.Int64Col(pos=1)
    EvtNum            = tb.Int64Col(pos=2)
    SubEvtNumber      = tb.Int64Col(pos=3)

    Mode              = tb.Int64Col  (pos=4)
    Vtxvol            = tb.Int64Col  (pos=5)
    Vtx_x0            = tb.Float64Col(pos=6)
    Vtx_x1            = tb.Float64Col(pos=7)
    Vtx_x2            = tb.Float64Col(pos=8)
    VecRecNumber      = tb.Int64Col  (pos=9)
    Jmu               = tb.Int64Col  (pos=10)
    Jp                = tb.Int64Col  (pos=11)
    Npar              = tb.Int64Col  (pos=12)
    NumTubesHit       = tb.Int64Col  (pos=13)
    NumDigiTubesHit   = tb.Int64Col  (pos=14)
    Ntrack            = tb.Int64Col  (pos=15)
    Ncaptures         = tb.Int64Col  (pos=16)
    Ncherenkovhits    = tb.Int64Col  (pos=17)
    Ncherenkovhittimes = tb.Int64Col (pos=18)
    Ncherenkovdigihits = tb.Int64Col (pos=19)
    SumQ              = tb.Float64Col(pos=20)
    TriggerType       = tb.StringCol (pos=21, itemsize=10)
    # TriggerInfo       = tb.Float64Col(pos=20, shape=10)


class CherenkovHits(tb.IsDescription):
    Run          = tb.Int64Col(pos=0)
    Date         = tb.Int64Col(pos=1)
    EvtNum       = tb.Int64Col(pos=2)
    SubEvtNumber = tb.Int64Col(pos=3)

    chit       = tb.Int64Col(pos=4)
    TubeID     = tb.Int64Col(pos=5)
    mPMTID     = tb.Int64Col(pos=6)
    mPMT_PMTID = tb.Int64Col(pos=7)
    TotalPe_0  = tb.Int64Col(pos=8)
    TotalPe_1  = tb.Int64Col(pos=8)


class CherenkovHitTimes(tb.IsDescription):
    Run          = tb.Int64Col(pos=0)
    Date         = tb.Int64Col(pos=1)
    EvtNum       = tb.Int64Col(pos=2)
    SubEvtNumber = tb.Int64Col(pos=3)

    chitt             = tb.Int64Col  (pos=4)
    Truetime          = tb.Float64Col(pos=5)
    PrimaryParentID   = tb.Int64Col  (pos=6)
    PhotonStartTime   = tb.Float64Col(pos=7)
    PhotonStartPos_x0 = tb.Float64Col(pos=8)
    PhotonStartPos_x1 = tb.Float64Col(pos=9)
    PhotonStartPos_x2 = tb.Float64Col(pos=10)
    PhotonEndPos_x0   = tb.Float64Col(pos=11)
    PhotonEndPos_x1   = tb.Float64Col(pos=12)
    PhotonEndPos_x2   = tb.Float64Col(pos=13)
    PhotonCreatorProcessName = tb.StringCol(pos=14, itemsize=20)


class CherenkovDigiHits(tb.IsDescription):
    Run            = tb.Int64Col(pos=0)
    Date           = tb.Int64Col(pos=1)
    EvtNum         = tb.Int64Col(pos=2)
    SubEvtNumber   = tb.Int64Col(pos=3)

    dhit           = tb.Int64Col  (pos=4)
    Q              = tb.Float64Col(pos=5)
    T              = tb.Float64Col(pos=6)
    TubeID         = tb.Int64Col  (pos=7)
    mPMTID         = tb.Int64Col  (pos=8)
    mPMT_PMTID     = tb.Int64Col  (pos=9)


class Geometry(tb.IsDescription):
    WCCylRadius = tb.Float64Col(pos=0)
    WCCylLength = tb.Float64Col(pos=1)
    Geom_Type   = tb.Float64Col(pos=2)
    WCNumPMT    = tb.Float64Col(pos=3)
    WCPMTRadius = tb.Float64Col(pos=4)
    WCOffset    = tb.Float64Col(pos=5, shape=(3,))


class PMT(tb.IsDescription):
    TubeNo      = tb.Int64Col  (pos=0)
    mPMTNo      = tb.Int64Col  (pos=1)
    mPMT_PMTNo  = tb.Int64Col  (pos=2)
    CylLoc      = tb.Int64Col  (pos=3)

    Orientation_x0 = tb.Float64Col(pos=5)
    Orientation_x1 = tb.Float64Col(pos=6)
    Orientation_x2 = tb.Float64Col(pos=7)
    Position_x0    = tb.Float64Col(pos=8)
    Position_x1    = tb.Float64Col(pos=9)
    Position_x2    = tb.Float64Col(pos=10)


class Options(tb.IsDescription):
    DetectorName                  = tb.StringCol (pos=0, itemsize=50)
    SavePi0                       = tb.BoolCol   (pos=1)
    PMTQEMethod                   = tb.Int64Col  (pos=2)
    PMTCollEff                    = tb.Int64Col  (pos=3)
    PMTDarkRate                   = tb.Float64Col(pos=4)
    ConvRate                      = tb.Float64Col(pos=5)
    DarkHigh                      = tb.Float64Col(pos=6)
    DarkLow                       = tb.Float64Col(pos=7)
    DarkWindow                    = tb.Float64Col(pos=8)
    DarkMode                      = tb.Int64Col  (pos=9)
    DigitizerClassName            = tb.StringCol (pos=10, itemsize=10)
    DigitizerDeadTime             = tb.Int64Col  (pos=11)
    DigitizerIntegrationWindow    = tb.Int64Col  (pos=12)
    DigitizerTimingPrecision      = tb.Float64Col(pos=13)
    DigitizerPEPrecision          = tb.Float64Col(pos=14)
    TriggerClassName              = tb.StringCol (pos=15, itemsize=10)
    MultiDigitsPerTrigger         = tb.BoolCol   (pos=16)
    NDigitsThreshold              = tb.Int64Col  (pos=17)
    NDigitsWindow                 = tb.Int64Col  (pos=18)
    NDigitsAdjustForNoise         = tb.BoolCol   (pos=19)
    NDigitsPreTriggerWindow       = tb.Int64Col  (pos=20)
    NDigitsPostTriggerWindow      = tb.Int64Col  (pos=21)
    TriggerOffset                 = tb.Float64Col(pos=22)
    SaveFailuresMode              = tb.Int64Col  (pos=23)
    SaveFailuresTime              = tb.Float64Col(pos=24)
    SaveFailuresPreTriggerWindow  = tb.Int64Col  (pos=25)
    SaveFailuresPostTriggerWindow = tb.Int64Col  (pos=26)

