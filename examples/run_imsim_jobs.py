from desc_roman_sims import GalSimJobGenerator
from desc_roman_sims.parsl.parsl_config import load_parsl_config

load_parsl_config("local_wq")

imsim_yaml = "/home/jchiang/RomanDESC/imsim-parsl-template.yaml"
visits = [740000]
generator = GalSimJobGenerator(imsim_yaml, visits, nfiles=4,
                               det_num_start=90, det_num_end=98)
futures = {}
for index in range(generator.num_jobs):
    job_future = generator.get_job_future()
    futures[job_future.task_def['func_name']] = job_future

def status():
    for func_name, future in futures.items():
        print(func_name, future.task_status())

status()
