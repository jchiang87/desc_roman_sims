import os
from collections import defaultdict
import numpy as np
import pandas as pd
from survey_region_ccds import SurveyRegion, OpSimData, CcdRegionFactory, \
    lonlat

ra0, dec0 = 9.5, -44  # ELAIS S1 center.
lon_size = lat_size = 10  # 100 square degree region.
survey_region = SurveyRegion(ra0, dec0, lon_size, lat_size)

mjd_range = (60796., 62621.)  # First 5 years of v3.2 baseline cadence.

fov_radius = 1.8
dra = fov_radius/survey_region.cos_dec
ddec = fov_radius
selection =  (f"where {survey_region.ra_min - dra} < fieldRA "
              f"and fieldRA < {survey_region.ra_max + dra} "
              f"and {survey_region.dec_min - ddec} < fieldDec "
              f"and fieldDec < {survey_region.dec_max + ddec} ")
if mjd_range:
    selection += (f"and {mjd_range[0]} < observationStartMJD "
                  f"and observationStartMJD < {mjd_range[1]} ")
print(selection)

opsim_db_file = os.environ['OPSIM_DB_FILE']
opsim_data = OpSimData(opsim_db_file, selection)
print(len(opsim_data))

data = defaultdict(list)
for i, (visit, band) in enumerate(zip(opsim_data.df['observationId'],
                                      opsim_data.df['filter'])):
    print(i, len(opsim_data))
    factory = opsim_data.ccd_region_factory(visit)
    ccds = factory.select_ccds(survey_region)
    for det_name in ccds:
        data['visit'].append(visit)
        data['band'].append(band)
        data['det_name'].append(det_name)

df = pd.DataFrame(data)
