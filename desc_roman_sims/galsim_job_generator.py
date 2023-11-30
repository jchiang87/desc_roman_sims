import os
import parsl


__all__ = ['GalSimJobGenerator']


wq_bash_app = parsl.bash_app(executors=['work_queue'],
                             cache=True, ignore_for_cache=['stderr', 'stdout'])


class GalSimJobGenerator:
    def __init__(self, imsim_yaml, visits, nfiles=10, nproc=1,
                 det_num_start=0, det_num_end=188, GB_per_CCD=6, GB_per_PSF=8,
                 verbosity=2, log_dir="logging"):

        # The following line ensures that all processes associated with
        # a galsim instance are occupied to start.
        assert nfiles >= nproc
        assert det_num_start < det_num_end

        self.imsim_yaml = imsim_yaml
        self.visits = visits
        self.nfiles = nfiles
        self.nproc = nproc
        self.det_num_start = det_num_start
        self.det_num_end = det_num_end
        self.GB_per_CCD = GB_per_CCD
        self.GB_per_PSF = GB_per_PSF
        self.verbosity = verbosity
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        self._visit_index = 0
        self._det_num_first = det_num_start
        self._launched_jobs = 0
        self._num_jobs = None

        self._psf_futures = {}

    def get_atm_psf_future(self, visit):
        """
        Use `galsim {self.imsim_yaml} output.nfiles=0` to generate the atm
        psf file.
        """
        run_name = f"{visit}_psf"
        stderr = os.path.join(self.log_dir, run_name + ".log")
        stdout = stderr

        command = (f"time galsim -v 2 {self.imsim_yaml} output.nfiles=0 "
                   f"input.opsim_data.visit={visit}")
        resource_spec = dict(memory=self.GB_per_PSF*1024, cores=1, disk=0)

        def psf_command(command_line, inputs=(), stderr=None, stdout=None,
                        parsl_resource_specification=resource_spec):
            return command_line
        psf_command.__name__ = run_name

        get_future = wq_bash_app(psf_command)
        return get_future(command, inputs=(), stderr=stderr, stdout=stdout)

    @property
    def num_jobs(self):
        if self._num_jobs is None:
            num_dets = self.det_num_end - self.det_num_start + 1
            jobs_per_visit = num_dets // self.nfiles
            if jobs_per_visit*self.nfiles < num_dets:
                jobs_per_visit += 1
            self._num_jobs = len(self.visits)*jobs_per_visit
        return self._num_jobs

    def get_job_future(self):
        if self._launched_jobs > self.num_jobs:
            return None

        if self._det_num_first > self.det_num_end:
            self._visit_index += 1
            self._det_num_first = self.det_num_start

        current_visit = self.visits[self._visit_index]

        if current_visit not in self._psf_futures:
            self._psf_futures[current_visit] \
                = self.get_atm_psf_future(current_visit)
        psf_future = self._psf_futures[current_visit]
        det_start = self._det_num_first
        det_end = min(det_start + self.nfiles - 1, self.det_num_end)
        run_name = f"{current_visit:08d}_{det_start:03d}_{det_end:03d}"

        stderr = os.path.join(self.log_dir, run_name + ".log")
        stdout = stderr

        nfiles = det_end - det_start + 1
        nproc = min(nfiles, self.nproc)
        command = (f"galsim -v {self.verbosity} {self.imsim_yaml} "
                   f"input.opsim_data.visit={current_visit} "
                   f"output.nfiles={nfiles} "
                   f"output.nproc={nproc} "
                   f"output.det_num.first={self._det_num_first}")

        # Expected resource usage per galsim instance.  Parsl assumes
        # memory has units of MB.
        resource_spec = dict(memory=self.GB_per_CCD*1024*self.nproc,
                             cores=1, disk=0)
        print(run_name, resource_spec, flush=True)

        def bash_command(command_line, inputs=(), stderr=None, stdout=None,
                         parsl_resource_specification=resource_spec):
            return command_line
        bash_command.__name__ = run_name

        # The wrapped bash_app function returns a python future when
        # called.
        get_future = wq_bash_app(bash_command)

        self._det_num_first += self.nfiles
        self._launched_jobs += 1

        return get_future(command, inputs=[psf_future], stderr=stderr,
                          stdout=stdout)
