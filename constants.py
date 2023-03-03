from config import *
#List of all costants

# Istat API URL
URL_OLD = "https://sdmx.istat.it/SDMXWS/rest/"      # deprecated
URL_NEW = "https://esploradati.istat.it/SDMXWS/rest/"

# URL population, without documentation (it is not clear if it works, and if yes)
URL_POP = "https://sdmx.istat.it/WS_CENSPOP/rest/"

# DataFlow (DF) URL      ==> List of all dataflows with their descriptions.
DF_POP = URL_POP+"dataflow/"
DF_OLD = URL_OLD+"dataflow/"
DF_NEW = URL_NEW+"dataflow/"

# DataFlow (DF) query    ==> /dataflow/{AgencyID}/{ResourceID}/{version}
#   {AgencyID}        => The agency maintaining the resource to be returned
#   {ResourceID}      => ID of the resource to be returned => ID del dataflow (nel URL_OLD e URL_POP It is a number;In URL_POP it is a code)
#   {version}         => Version of the resource to be returned
#   example:  <structure:Dataflow id="101_1015" agencyID="IT1" version="1.0" isFinal="true">
#                                 id={ResourceID}   agencyID '' version ''

# AvailbleConstraint (AC) get all of the keys value option available and their description
AC_OLD = URL_OLD + \
    "availableconstraint/{},{},{}//all?mode=available&references=codelist"
    
AC_NEW = URL_NEW + \
    "availableconstraint/{},{},{}//all?mode=available&references=codelist"  # non funziona
AC_POP = URL_POP + \
    "availableconstraint/{},{},{}//all"  # non funziona

# DataFlow (DF) resource UNNECESSARY
# DF_RESOURCE_NEW = DF_NEW+"IT1/22_289"       # get informaton about a resource (equivalent to DF_NEW)
# DF_RESOURCE_OLD = DF_OLD+"IT1/22_289"       # get informaton about a resource (equivalent to DF_OLD)
# DF_RESOURCE_POP = DF_POP+"IT1/POPINCV"      # not working!? / unnecessary

# DataStructure (DS)  base_url+ datastructure/{AgentID}/{(resource) Structure Ref ID}
DS_NEW = URL_NEW+"datastructure/IT1/{}/"
DS_OLD = URL_OLD+"datastructure/IT1/{}/"
DS_POP = URL_POP+"datastructure/IT1/{}/"

# CodeList (CL) ? get all of the available code    #NOT USE
CL_NEW = URL_NEW + "codelist/IT1/"
CL_OLD = URL_OLD + "codelist/IT1/"  # cl_freq = test
CL_POP = URL_POP + "codelist/IT1/"


# DATA   base_url /data/{AgencyID},ResourceID/ KEY

# Example DATA_NEW = URL_NEW + "data/IT1,22_289/A.001001.JAN.1.Y30.1/"
DATA_NEW = URL_NEW + "data/{},{}/{}"            #{agencyID},{resourceID}/filters (key)
DATA_OLD = URL_OLD + "data/"  
# Example: DATA_OLD = "http://sdmx.istat.it/sdmxws/rest/data/IT1,22_289,1.5/A.001001.JAN.1.Y30.1?startPeriod=2020-01&endPeriod=2020-10&dimensionAtObservation=TIME_PERIOD&detail=full"
DATA_POP = URL_POP + "data/" 

# KEY = query parameters, separated with a single dots "." with a specific order


# AvailableConstraint /{flow}/{key}/{componentID} used to see the key parameters and their description
AV_OLD = URL_OLD+"availableconstraint/"

OLD = [URL_OLD, DF_OLD, AC_OLD, DS_OLD, DATA_OLD]  # data-old TODO
NEW = [URL_NEW, DF_NEW, AC_NEW, DS_NEW, DATA_NEW]
POP = [URL_POP, DF_POP, AC_POP, DS_POP, DATA_POP]  # TODO in seguito
ALL_LINK = [OLD, NEW, POP]


FILTERS = ["CL_FREQ", "CL_ITTER107", "CL_TIPO_DATO15",
           "CL_SEXISTAT1", "CL_ETA1", "CL_STATCIV2"]


#TEST AREA

#TEST = "https://sdmx.istat.it/sdmxws/rest/codelist/IT1/POP_RES1GEN"
#TEST = f"{URL_OLD}availableconstraint/{agencyID},{resourceID},{version}/...../all?mode=available&references=all"
#TEST = f"{URL_NEW}availableconstraint/{agencyID},{resourceID},{version}/...../all?mode=available&references=all"
# TEST = f"{URL_POP}availableconstraint/{agencyID},{resourceID},{version}/...../all?mode=available&references=all"