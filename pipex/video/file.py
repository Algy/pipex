from ..poperators import pipe_map
from .model import FileVideo


class open(pipe_map):
    def map(self, value):
        return FileVideo(value)
