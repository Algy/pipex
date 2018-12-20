import time

class PAtom:
    def __init__(self, value, format):
        self.value = value
        self.format = format


class PRecord:
    def __init__(
            self,
            *,
            id: str,
            channels: Dict[str, PAtom] = None,
            timestamp: int,
            default_channel: str = 'default'):
        self.id = id
        self.default_channel = default_channel
        self.timestamp = timestamp
        self.channels = channels or {}

    @property
    def value(self):
        try:
            return self.channels[self.default_channel].value
        except KeyError:
            return None

    @value.setter
    def value(self, v):
        channel = self.default_channel
        try:
            self.channels[channel].value = v
        except KeyError:
            self.channels[channel] = PAtom(v, self._infer_format_from_type(channel, v))

    def __getitem__(self, name):
        return self.channels[name]

    def copy(self) -> "PRecord":
        return PRecord(
            id=self.id,
            channels=self.channels.copy(),
            timestamp=self.timestamp,
            default_channel=self.default_channel
        )


    def get(self, name, default=None):
        return self.channels.get(name, default)

    def with_channel(self, channel_name: str) -> "PRecord":
        result = self.copy()
        result.default_channel = channel_name
        return result

    def merge(self, **kwargs) -> "PRecord":
        result = self.copy()
        result.channels.update(kwargs)
        result.timestamp = time.time()
        return result

    def with_value(self, value):
        result = self.copy()
        result.value = value
        return result

    @classmethod
    def from_object(cls, obj, channel_name='default'):
        ret = cls(id=str(id(obj), timestamp=time.time(), default_channel=channel_name)
        ret.value = obj
        return ret

    @classmethod
    def _infer_format_from_type(cls, channel_name: str, value: Any) -> str:
        if isinstance(value, np.ndarray):
            if channel_name.startswith('image') or channel_name.startswith('img'):
                return 'image'
            else:
                return 'np.ndarray'
        elif isinstance(value, (type(None), str, int, float, bool, list)):
            return "data"
        else:
            return "blob"


    def __repr__(self):
        return "<{} id={!r} value={!r} channels={!r}>".format(
            self.id,
            self.__class__.__name__,
            self.value,
            self.channels
        )
