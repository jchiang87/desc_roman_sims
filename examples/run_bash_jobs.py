import parsl
from parsl_config import load_parsl_config

load_parsl_config("local")


_COMMANDS = ("ls", "time", "uname", "galsim --version")


my_bash_app = parsl.bash_app(executors=['submit-node'],
                             cache=True, ignore_for_cache=['stderr', 'stdout'])


class CommandGenerator:
    def __init__(self, commands=_COMMANDS):
        self.commands = commands
        self._counter = 0

    def run_command(self, index=None, stderr=None, stdout=None):
        if index is None:
            index = self._counter
            self._counter += 1
        index //= len(self.commands)
        def bash_command(inputs=(), stderr=stderr, stdout=stdout):
            return self.commands[index]
        bash_command.__name__ = self.commands[index]
        app = my_bash_app(bash_command)
        return app()


if __name__ == '__main__':
    generator = CommandGenerator()
    futures = {}
    for index in range(10):
        outfile = f'output_{index:02d}.log'
        futures[index] = generator.run_command(index=index, stdout=outfile,
                                               stderr=outfile)

    all_done = lambda : all(_.done() for _ in futures.values())

    print(all_done())
