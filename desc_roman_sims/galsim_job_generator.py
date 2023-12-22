import os
import glob
from collections import defaultdict
import parsl
from galsim.main import ReadConfig


__all__ = ['GalSimJobGenerator']


class GalSimJobGenerator:
    def __init__(self, imsim_yaml, visits, nfiles=10, nproc=1,
                 default_det_list=None, GB_per_CCD=6, GB_per_PSF=8,
                 verbosity=2, log_dir="logging", clean_up_atm_psfs=True,
                 bash_app_executor='work_queue'):

        # The following line ensures that all processes associated with
        # a galsim instance are occupied to start.
        assert nfiles >= nproc

        self.imsim_yaml = imsim_yaml
        config = ReadConfig(imsim_yaml)[0]

        self.output_dir_format = config['output.dir']['format']

        self.atm_psf_dir \
            = os.path.dirname(config['input.atm_psf.save_file']['format'])
        os.makedirs(self.atm_psf_dir, exist_ok=True)
        self.clean_up_atm_psfs = clean_up_atm_psfs

        self.visits = visits
        self.nfiles = nfiles
        self.nproc = nproc

        if default_det_list is None:
            self.target_dets = set(range(189))
        else:
            self.target_dets = set(default_det_list)
        self._assemble_det_lists()

        self.GB_per_CCD = GB_per_CCD
        self.GB_per_PSF = GB_per_PSF
        self.verbosity = verbosity
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        self.bash_app_executor = bash_app_executor
        self.bash_app = parsl.bash_app(executors=[bash_app_executor],
                                       cache=True,
                                       ignore_for_cache=['stderr', 'stdout'])

        self._visit_index = 0
        self.current_visit = self.visits[self._visit_index]
        self._launched_jobs = 0
        self._det_index = 0

        self._psf_futures = {}
        self._ccd_futures = defaultdict(list)
        self._rm_atm_psf_futures = []

    def _assemble_det_lists(self):
        self._det_lists = {}
        for visit in self.visits:
            output_dir = self.output_dir_format % visit
            raw_files = glob.glob(os.path.join(output_dir, 'amp*'))
            finished_dets = []
            for item in raw_files:
                basename = os.path.basename(item)
                index = basename.find('det')
                finished_dets.append(int(basename[index+3:index+6]))
            self._det_lists[visit] \
                = sorted(self.target_dets.difference(finished_dets))
        self.num_jobs = sum([len(_) for _ in self._det_lists.values()])

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
        # Write stderr, stdout to log file in append mode.
        stderr = (os.path.join(self.log_dir, job_name + ".log"), 'a')
        stdout = stderr

        if self.bash_app_executor == "work_queue":
            resource_spec = dict(memory=self.GB_per_PSF*1024, cores=1, disk=0)

            def psf_command(command_line, inputs=(), stderr=None, stdout=None,
                            parsl_resource_specification=resource_spec):
                return command_line
        else:

            def psf_command(command_line, inputs=(), stderr=None, stdout=None):
                return command_line
        psf_command.__name__ = job_name

        get_future = self.bash_app(psf_command)

        command = (f"time galsim -v 2 {self.imsim_yaml} output.nfiles=0 "
                   f"input.opsim_data.visit={visit}")
        return [get_future(command, stderr=stderr, stdout=stdout)]

    def get_job_future(self):
        if self._launched_jobs > self.num_jobs:
            return None

        if self._det_index >= len(self._det_lists[self.current_visit]):
            handled_visit = self.current_visit

            if self.clean_up_atm_psfs:
                # Create a python_app that removes the atm_psf file
                # for the just-handled visit after the futures for
                # each CCD in that visit have finished rendering.
                @parsl.python_app(executors=['thread_pool'])
                def remove_atm_psf(visit, inputs=()):
                    atm_psf_file = self.find_psf_file(visit)
                    print("deleting", atm_psf_file, flush=True)
                    os.remove(atm_psf_file)

                remove_atm_psf.__name__ = f"rm_atm_psf_{handled_visit}"
                self._rm_atm_psf_futures.append(
                    remove_atm_psf(handled_visit,
                                   inputs=self._ccd_futures[handled_visit]))

            self._visit_index += 1
            try:
                self.current_visit = self.visits[self._visit_index]
            except IndexError:
                return None
            self._det_index = 0

        if self.current_visit not in self._psf_futures:
            self._psf_futures[self.current_visit] \
                = self.get_atm_psf_future(self.current_visit)
        psf_futures = self._psf_futures[self.current_visit]
        det_list = self._det_lists[self.current_visit]
        det_start = det_list[self._det_index]
        det_end_index = min(self._det_index + self.nfiles, len(det_list)) - 1
        det_end = det_list[det_end_index]
        job_name = f"{self.current_visit:08d}_{det_start:03d}_{det_end:03d}"

        # Write stderr, stdout to log file in append mode.
        stderr = (os.path.join(self.log_dir, job_name + ".log"), 'a')
        stdout = stderr

        if self.bash_app_executor == "work_queue":
            # Expected resource usage per galsim instance.  Parsl assumes
            # memory has units of MB.
            resource_spec = dict(memory=self.GB_per_CCD*1024*self.nproc,
                                 cores=1, disk=0)
            print(job_name, resource_spec, flush=True)

            def bash_command(command_line, inputs=(), stderr=None, stdout=None,
                             parsl_resource_specification=resource_spec):
                return command_line
        else:

            def bash_command(command_line, inputs=(), stderr=None, stdout=None):
                return command_line
        bash_command.__name__ = job_name

        # The wrapped bash_app function returns a python future when
        # called.
        get_future = self.bash_app(bash_command)

        nfiles = det_end_index - self._det_index + 1
        nproc = min(nfiles, self.nproc)
        my_det_list = ("[" +
                       ", ".join([str(_) for _ in
                                  det_list[self._det_index:det_end_index + 1]])
                       + "]")
        command = (f"galsim -v {self.verbosity} {self.imsim_yaml} "
                   f"input.opsim_data.visit={self.current_visit} "
                   f"output.nfiles={nfiles} "
                   f"output.nproc={nproc} "
                   "output.det_num='{type: List, items: " + my_det_list + "}'")

        self._det_index += self.nfiles
        self._launched_jobs += 1

        ccd_future = get_future(command, inputs=psf_futures, stderr=stderr,
                                stdout=stdout)
        self._ccd_futures[self.current_visit].append(ccd_future)
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
