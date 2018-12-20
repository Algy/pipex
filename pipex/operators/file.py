import glob

from ..pdatastructures import PRecord
from ..pbase import pipe_map, source


class load_text(pipe_map):
    def transform(self, precords):
        for precord in precords:
            file_name = precord.value

            with open(file_name) as f:
                text = f.read()
            yield precord.with_channel("text").with_value(text)j

class load_binary(pipe_map):
    def transform(self, precords):
        for precord in precords:
            file_name = precord.value

            with open(file_name, "rb") as f:
                bin = f.read()
            yield precord.with_channel("binary").with_value(bin)

class glob(source):
    channel_name = 'file_name'
    def __init__(self, pattern='*'):
        self.pattern = pattern

    def generate(self):
        return glob.glob(self.pattern)
