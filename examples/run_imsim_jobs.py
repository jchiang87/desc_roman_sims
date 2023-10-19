import os
import parsl
from desc_roman_sims.parsl.parsl_config import load_parsl_config

load_parsl_config("local_wq")

my_bash_app = parsl.bash_app(executors=['work_queue'],
                             cache=True, ignore_for_cache=['stderr', 'stdout'])


class CommandGenerator:
    def __init__(self, imsim_yaml, nfiles=10, nproc=1, det_num_first=0,
                 det_num_max=188, log_dir="logging"):
        assert nfiles >= nproc   # To avoid idle cores at the outset
        self.imsim_yaml = imsim_yaml
        self.nfiles = nfiles
        self.nproc = nproc
        self.det_num_max = det_num_max
        self.det_num_first = min(det_num_first, self.det_num_max)
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

    @property
    def num_jobs(self):
        return (self.det_num_max - self.det_num_first + 1) // self.nfiles

    def run_command(self, stderr=None, stdout=None):
        if self.det_num_first > self.det_num_max:
            return None

        run_name = f"galsim_{self.det_num_first:03d}"
        if stderr is None:
            stderr = os.path.join(self.log_dir, run_name + ".log")
        if stdout is None:
            stdout = os.path.join(self.log_dir, run_name + ".log")

        command = (f"galsim -v 2 {self.imsim_yaml} "
                   f"output.nfiles={self.nfiles} "
                   f"output.nproc={self.nproc} "
                   f"output.det_num.first={self.det_num_first}")

        GB_per_CCD = 6.0  # per CCD memory needed by GalSim/imSim
        resource_spec \
            = dict(memory=GB_per_CCD*1024.0*self.nproc,  # parsl assumes MB
                   cores=1, disk=0)
        def bash_command(inputs=(), stderr=stderr, stdout=stdout,
                         parsl_resource_specifcation=resource_spec):
            return command
        bash_command.__name__ = run_name
        app = my_bash_app(bash_command)

        self.det_num_first += self.nfiles

        return app()


if __name__ == '__main__':
    imsim_yaml = "/home/jchiang/work/DESC/desc_roman_sims/work/imsim-user-skycat.yaml"
    generator = CommandGenerator(imsim_yaml, nfiles=3, det_num_first=90,
                                 det_num_max=98)
    futures = {}
    for index in range(generator.num_jobs):
        futures[index] = generator.run_command()

    def all_done():
        return all(_.done() for _ in futures.values())

    print(all_done())
