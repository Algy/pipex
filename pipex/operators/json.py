import json

from ..poperators import pipe, pipe_map, sink


class load_json(pipe):
    def map(self, value):
        return json.loads(value)

class dump_json(pipe):
    def map(self, value):
        return json.dumps(value)
