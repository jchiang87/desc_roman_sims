from desc_roman_sims import GalSimJobGenerator
from desc_roman_sims.parsl.parsl_config import load_wq_config

load_wq_config(memory=9000, port=9122)

imsim_yaml = "/home/jchiang/RomanDESC/imsim-parsl-template.yaml"
visits = [802625]
generator = GalSimJobGenerator(imsim_yaml, visits,
                               nfiles=4, GB_per_CCD=5,
                               GB_per_PSF=8,
                               clean_up_atm_psfs=False)
#generator.run()

psf_visits = [38064, 38065, 38066]
atm_psf_futures = []
for visit in psf_visits:
    atm_psf_futures.extend(generator.get_atm_psf_future(visit))

_ = [_.exception() for _ in atm_psf_futures]
