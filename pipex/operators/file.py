import glob

from ..pdatastructures import PRecord
from ..pbase import pipe_map, source


class load_text(pipe_map):
    def transform(self, precords):
        for precord in precords:
            file_name = precord.value

            with open(file_name) as f:
                text = f.read()
            yield PRecord.


class load_binary(pipe_map):
    pass

class load_image(pipe_map):
    def transform(self, precords):
        import numpy
        from PIL import Image

        for precord in precords:
            ...


class glob(source):
    def __init__(self, pattern='*'):
        self.pattern = pattern

    def generate(self):
        return glob.glob(self.pattern)
