import os
import glob
from collections import defaultdict
import parsl
from galsim.main import ReadConfig


__all__ = ['GalSimJobGenerator']


wq_bash_app = parsl.bash_app(executors=['work_queue'],
                             cache=True, ignore_for_cache=['stderr', 'stdout'])


class GalSimJobGenerator:
    def __init__(self, imsim_yaml, visits, nfiles=10, nproc=1,
                 det_num_start=0, det_num_end=188, GB_per_CCD=6, GB_per_PSF=8,
                 verbosity=2, log_dir="logging", clean_up_atm_psfs=True):

        # The following line ensures that all processes associated with
        # a galsim instance are occupied to start.
        assert nfiles >= nproc
        assert det_num_start < det_num_end

        self.imsim_yaml = imsim_yaml
        self.atm_psf_dir = os.path.dirname(
            ReadConfig(imsim_yaml)[0]['input.atm_psf.save_file']['format'])
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
        self.clean_up_atm_psfs = clean_up_atm_psfs

        self._visit_index = 0
        self._det_num_first = det_num_start
        self._launched_jobs = 0
        self._num_jobs = None

        self._psf_futures = {}
        self._ccd_futures = defaultdict(list)
        self._rm_atm_psf_futures = []

    def find_psf_file(self, visit):
        psf_files = glob.glob(os.path.join(self.atm_psf_dir, f"*{visit}*.pkl"))
        if psf_files:
            return psf_files[0]
        else:
            return None

    def get_atm_psf_future(self, visit):
        """
        Use `galsim {self.imsim_yaml} output.nfiles=0` to generate the atm
        psf file.
        """
        if self.find_psf_file(visit) is not None:
            # atm_psf_file already exists, so return an empty list of
            # prerequisite futures.
            return []
        job_name = f"{visit}_psf"
        stderr = os.path.join(self.log_dir, job_name + ".log")
        stdout = stderr
        resource_spec = dict(memory=self.GB_per_PSF*1024, cores=1, disk=0)

        def psf_command(command_line, inputs=(), stderr=None, stdout=None,
                        parsl_resource_specification=resource_spec):
            return command_line
        psf_command.__name__ = job_name

        get_future = wq_bash_app(psf_command)

        command = (f"time galsim -v 2 {self.imsim_yaml} output.nfiles=0 "
                   f"input.opsim_data.visit={visit}")
        return [get_future(command, stderr=stderr, stdout=stdout)]

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
            handled_visit = self.visits[self._visit_index]

            if self.clean_up_atm_psfs:
                # Create a python_app that removes the atm_psf file
                # for the just-handled visit after the futures for
                # each CCD in that visit have finished rendering.
                @parsl.python_app(executors=['submit-node'])
                def remove_atm_psf(visit, inputs=()):
                    atm_psf_file = self.find_psf_file(visit)
                    print("deleting", atm_psf_file, flush=True)
                    os.remove(atm_psf_file)

                remove_atm_psf.__name__ = f"rm_atm_psf_{handled_visit}"
                self._rm_atm_psf_futures.append(
                    remove_atm_psf(handled_visit,
                                   inputs=self._ccd_futures[handled_visit]))

            self._visit_index += 1
            self._det_num_first = self.det_num_start

        try:
            current_visit = self.visits[self._visit_index]
        except IndexError:
            return None

        if current_visit not in self._psf_futures:
            self._psf_futures[current_visit] \
                = self.get_atm_psf_future(current_visit)
        psf_futures = self._psf_futures[current_visit]
        det_start = self._det_num_first
        det_end = min(det_start + self.nfiles - 1, self.det_num_end)
        job_name = f"{current_visit:08d}_{det_start:03d}_{det_end:03d}"

        stderr = os.path.join(self.log_dir, job_name + ".log")
        stdout = stderr

        # Expected resource usage per galsim instance.  Parsl assumes
        # memory has units of MB.
        resource_spec = dict(memory=self.GB_per_CCD*1024*self.nproc,
                             cores=1, disk=0)
        print(job_name, resource_spec, flush=True)

        def bash_command(command_line, inputs=(), stderr=None, stdout=None,
                         parsl_resource_specification=resource_spec):
            return command_line
        bash_command.__name__ = job_name

        # The wrapped bash_app function returns a python future when
        # called.
        get_future = wq_bash_app(bash_command)

        nfiles = det_end - det_start + 1
        nproc = min(nfiles, self.nproc)
        command = (f"galsim -v {self.verbosity} {self.imsim_yaml} "
                   f"input.opsim_data.visit={current_visit} "
                   f"output.nfiles={nfiles} "
                   f"output.nproc={nproc} "
                   f"output.det_num.first={self._det_num_first}")

        self._det_num_first += self.nfiles
        self._launched_jobs += 1

        ccd_future = get_future(command, inputs=psf_futures, stderr=stderr,
                                stdout=stdout)
        self._ccd_futures[current_visit].append(ccd_future)
        return ccd_future

    def run(self, block=True):
        ccd_futures = []
        print("Generating CCD job futures...", flush=True)
        for index in range(self.num_jobs + 1):
            ccd_future = self.get_job_future()
            if ccd_future is not None:
                ccd_futures.append(ccd_future)

        if block:
            if self._rm_atm_psf_futures:
                print("Waiting for clean-up futures.", flush=True)
                _ = [_.exception() for _ in self._rm_atm_psf_futures]
            else:
                _ = [_.exception() for _ in ccd_futures]
