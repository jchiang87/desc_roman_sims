import os
import parsl
from desc_roman_sims.parsl.parsl_config import load_parsl_config

load_parsl_config("local_wq")

my_bash_app = parsl.bash_app(executors=['work_queue'],
                             cache=True, ignore_for_cache=['stderr', 'stdout'])


class GalSimJobGenerator:
    def __init__(self, imsim_yaml, visits, nfiles=10, nproc=1,
                 det_num_start=0, det_num_end=188, GB_per_CCD=6,
                 log_dir="logging"):
        assert nfiles >= nproc  # This ensures all processes associated with
                                # a galsim instance are occupied to start.
        assert det_num_start < det_num_end

        self.imsim_yaml = imsim_yaml
        self.visits = visits
        self.nfiles = nfiles
        self.nproc = nproc
        self.det_num_start = det_num_start
        self.det_num_end = det_num_end
        self.GB_per_CCD = GB_per_CCD
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        self._visit_index = 0
        self._det_num_first = det_num_start
        self._launched_jobs = 0
        self._num_jobs = None

    @property
    def num_jobs(self):
        if self._num_jobs is None:
            num_dets = self.det_num_end - self.det_num_start + 1
            jobs_per_visit = num_dets // self.nfiles
            if jobs_per_visit*self.nfiles < num_dets:
                jobs_per_visit += 1
            self._num_jobs = len(self.visits)*jobs_per_visit
        return self._num_jobs

    def get_job_future(self, stderr=None, stdout=None):
        if self._launched_jobs > self.num_jobs:
            return None

        if self._det_num_first > self.det_num_end:
            self._visit_index += 1
            self._det_num_first = self.det_num_start

        det_start = self._det_num_first
        det_end = min(det_start + self.nfiles - 1, self.det_num_end)
        run_name = f"galsim_{det_start:03d}_{det_end:03d}"
        if stderr is None:
            stderr = os.path.join(self.log_dir, run_name + ".log")
        if stdout is None:
            stdout = os.path.join(self.log_dir, run_name + ".log")

        current_visit = self.visits[self._visit_index]
        command = (f"galsim -v 2 {self.imsim_yaml} "
                   f"input.opsim_data.visit={current_visit} "
                   f"output.nfiles={self.nfiles} "
                   f"output.nproc={self.nproc} "
                   f"output.det_num.first={self._det_num_first}")

        # Expected resource usage per galsim instance.  Parsl assumes
        # memory has units of MB.
        resource_spec = dict(memory=self.GB_per_CCD*1024*self.nproc,
                             cores=1, disk=0)
        def bash_command(inputs=(), stderr=stderr, stdout=stdout,
                         parsl_resource_specifcation=resource_spec):
            return command
        bash_command.__name__ = run_name

        # The wrapped bash_app function returns a python future when
        # called.
        get_future = my_bash_app(bash_command)

        self._det_num_first += self.nfiles
        self._launched_jobs += 1

        return get_future()


if __name__ == '__main__':
    imsim_yaml = ("/home/jchiang/work/DESC/desc_roman_sims/work/"
                  "imsim-user-skycat.yaml")
    visits = [449053]
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
