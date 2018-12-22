import time
import numpy as np

from PIL import Image
from base64 import b64encode
from functools import lru_cache
from typing import Dict, Any
from html import escape


def repr_image(value):
    buffer = value._repr_png_()
    src = "data:image/png;base64," + b64encode(buffer).decode()
    return "<img src='{}' style='width: 100%'>".format(src)

def repr_atom(atom) -> str:
    value = atom.value
    if atom.format == 'image' and isinstance(value, np.ndarray):
        try:
            return repr_image(Image.fromarray(value))
        except Exception as e:
            pass
    if hasattr(value, '_repr_png_'):
        return repr_image(value)
    if hasattr(value, '_repr_html_'):
        return value._repr_html_()
    return "<pre>" + escape(repr(atom.value)) + "</pre>"

def repr_channel(channel_name, atom, is_active) -> str:
    if atom is not None:
        content = repr_atom(atom)
    else:
        content = escape(repr(atom))

    return """
    <tr>
        <th>{}{}</th>
        <td>{}</td>
    </tr>
    """.format(channel_name, "*" if is_active else "", content)

def _repr_html_(self):
    channels = [self.active_channel]+ [chan for chan in self.channels if chan != self.active_channel]
    prologue = '''
    <div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe" style="width: 100%; table-layout: auto">
  <thead>
    <tr style="text-align: right;">
      <th>channel</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
'''
    epilogue = '''
  </tbody>
</table>
</div>
    '''

    content = "\n".join(
        repr_channel(channel, self.get_atom(channel), channel == self.active_channel)
        for channel in channels
    )
    return prologue + content + epilogue

def _infer_format_from_type(channel_name: str, value: Any) -> str:
    if isinstance(value, np.ndarray):
        if channel_name.startswith('image') or channel_name.startswith('img'):
            return 'image'
        else:
            return 'numpy.ndarray'
    elif isinstance(value, (type(None), str, int, float, bool, list)):
        return "data"
    else:
        return "blob"



class PAtom:
    __slots__ = ('value', 'format', )
    def __init__(self, *, value: Any, format: str):
        self.value = value
        self.format = format

class PRecord:
    __slots__ = ('id', 'timestamp', 'active_channel', 'channel_atoms')
    def __init__(self, *,
                 id: str,
                 timestamp: float,
                 active_channel: str = 'default',
                 channel_atoms: Dict[str, PAtom] = None):
        self.id = id
        self.timestamp = timestamp
        self.active_channel = active_channel
        self.channel_atoms = channel_atoms or {}

    @property
    def atom(self):
        return self.channel_atoms.get(self.active_channel)

    @property
    @lru_cache(None)
    def value(self):
        atom = self.atom
        if atom is None:
            return None
        return atom.value

    @property
    @lru_cache(None)
    def value_format(self):
        atom = self.atom
        if atom is None:
            return None
        return atom.format

    @property
    def channels(self):
        return self.channel_atoms.keys()

    def get(self, name, default=None):
        atom = self.channel_atoms.get(name)
        if atom is None:
            return default
        return atom.value

    def get_atom(self, name):
        return self.channel_atoms.get(name)

    def __getitem__(self, name):
        atom = self.channel_atoms[name]
        return atom.value

    def with_channel(self, channel_name: str) -> "PRecord":
        return PRecord(
            id=self.id,
            timestamp=self.timestamp,
            active_channel=channel_name,
            channel_atoms=self.channel_atoms,
        )

    def select_channels(self, channels) -> "PRecord":
        channel_atoms = {
            k: v
            for k, v in self.channel_atoms.items()
            if k in channels
        }
        return PRecord(
            id=self.id,
            timestamp=self.timestamp,
            active_channel=self.active_channel,
            channel_atoms=channel_atoms,
        )

    def with_channel_item(self, channel_name: str, value: Any) -> "PRecord":
        channel_atoms = self.channel_atoms.copy()
        channel_atoms[channel_name] = PAtom(value=value, format=_infer_format_from_type(channel_name, value))

        return PRecord(
            id=self.id,
            timestamp=self.timestamp,
            active_channel=channel_name,
            channel_atoms=channel_atoms,
        )


    def merge(self, **kwargs) -> "PRecord":
        channel_atoms = self.channel_atoms.copy()
        for key, value in kwargs.items():
            channel_atoms[key] = PAtom(value=value, format=_infer_format_from_type(key, value))
        return PRecord(
            id=self.id,
            timestamp=time.time(),
            active_channel=self.active_channel,
            channel_atoms=channel_atoms,
        )


    def with_id(self, id: str):
        return PRecord(
            id=id,
            timestamp=self.timestamp,
            active_channel=self.active_channel,
            channel_atoms=self.channel_atoms,
        )

    def with_value(self, value: Any):
        active_channel = self.active_channel
        channel_atoms = self.channel_atoms.copy()
        channel_atoms[active_channel] = PAtom(
            value=value,
            format=_infer_format_from_type(active_channel, value)
        )

        return PRecord(
            id=self.id,
            timestamp=self.timestamp,
            active_channel=self.active_channel,
            channel_atoms=channel_atoms,
        )

    @classmethod
    def from_object(cls, obj, channel_name='default'):
        return cls(
            id=str(id(obj)),
            timestamp=time.time(),
            active_channel=channel_name,
            channel_atoms={
                channel_name: PAtom(
                    value=obj,
                    format=_infer_format_from_type(channel_name, obj),
                )
            }
        )

    def __repr__(self):
        return ("<PRecord id={!r} timestamp={!r} active_channel={!r} channels={!r}>"
                .format(
                    self.id,
                    self.timestamp,
                    self.active_channel,
                    list(self.channels),
                ))


    def _repr_html_(self):
        return _repr_html_(self)
