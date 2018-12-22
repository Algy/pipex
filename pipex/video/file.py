from ..poperators import pipe
from .model import FileVideo


class open(pipe):
    def transform(self, our, precords):
        for precord in precords:
            yield precord.with_channel_item("video", FileVideo(precord.value))
