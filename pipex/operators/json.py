import json

from ..poperators import pipe, pipe_map, sink


class loads(pipe):
    def map(self, value):
        return json.loads(value)

class dumps(pipe):
    def map(self, value):
        return json.dumps(value)
