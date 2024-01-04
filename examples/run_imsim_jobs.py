import socket
from desc_roman_sims import GalSimJobGenerator
from desc_roman_sims.parsl.parsl_config import load_wq_config

hostname = socket.gethostname()
run_dir = f'runinfo/{hostname}'
load_wq_config(memory=17000, port=9122, run_dir=run_dir)

imsim_yaml = "/home/jchiang/RomanDESC/imsim-parsl-template.yaml"
visits = [802625]
generator = GalSimJobGenerator(imsim_yaml, visits,
                               nfiles=4, GB_per_CCD=5,
                               default_det_list=list(range(90, 99)),
                               clean_up_atm_psfs=False)
generator.run()
