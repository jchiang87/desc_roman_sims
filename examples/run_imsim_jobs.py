import sys

from desc_roman_sims import GalSimJobGenerator
from desc_roman_sims.parsl.parsl_config import load_wq_config

load_wq_config(memory=12000, port=9232, monitor=False)

imsim_yaml = "/home/jchiang/RomanDESC/imsim-parsl-template.yaml"
visits = [802625]
generator = GalSimJobGenerator(imsim_yaml, visits, nfiles=4, GB_per_CCD=5,
                               det_num_start=90, det_num_end=98)
futures = {}
for index in range(generator.num_jobs):
    job_future = generator.get_job_future()
    futures[job_future.task_def['func_name']] = job_future


def status():
    for func_name, future in futures.items():
        print(func_name, future.task_status())


status()

if not hasattr(sys, 'ps1'):
    _ = [_.exception() for _ in futures.values()]
