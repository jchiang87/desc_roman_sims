import os
import parsl
from desc_roman_sims.parsl.parsl_config import load_wq_config


load_wq_config(memory=4000, monitor=False)


_COMMANDS = ("ls", "time", "uname", "galsim --version", "pwd")


my_bash_app = parsl.bash_app(executors=['work_queue'],
                             cache=True, ignore_for_cache=['stderr', 'stdout'])


class BashJobGenerator:
    def __init__(self, commands=_COMMANDS):
        self.commands = commands
        self._counter = 0

    def run_command(self, index=None, stderr=None, stdout=None):
        if index is None:
            index = self._counter
            self._counter += 1
        index %= len(self.commands)
        def bash_command(inputs=(), stderr=stderr, stdout=stdout):
            return self.commands[index]
        bash_command.__name__ = self.commands[index]
        app = my_bash_app(bash_command)
        return app()


if __name__ == '__main__':
    generator = BashJobGenerator()
    futures = {}
    log_dir = 'logging'
    os.makedirs(log_dir, exist_ok=True)
    for index in range(10):
        run_name = f'job_{index:02d}'
        outfile = os.path.join(log_dir, f'{run_name}.log')
        futures[run_name] = generator.run_command(index=index, stdout=outfile,
                                                  stderr=outfile)

    def status():
        for func_name, future in futures.items():
            print(func_name, future.task_status())

    status()

    # Force python to wait for futures to return in non-interactive sessions.
    _ = [_.exception() for _ in futures.values()]
