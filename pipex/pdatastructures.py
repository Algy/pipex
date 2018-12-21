import time
import numpy as np

from functools import lru_cache
from pyrsistent import pmap, PRecord as pyr_PRecord, field as pyr_field, PMap as pyr_PMap
from typing import Dict, Any

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



class PAtom(pyr_PRecord):
    value = pyr_field(mandatory=True) # type: Any
    format = pyr_field(type=str, mandatory=False) # type: str

class PRecord(pyr_PRecord):
    id = pyr_field(type=str, mandatory=True) # type: str
    timestamp = pyr_field(type=float, mandatory=True) # type: float
    active_channel = pyr_field(type=str, initial='default', mandatory=True) # type: str
    channel_atoms = pyr_field(mandatory=False, initial=pmap()) # type: pyr_PMap[str, PAtom]

    @property
    def atom(self):
        return self.channel_atoms.get(self.active_channel)

    @property
    def value(self):
        atom = self.atom
        if atom is None:
            return None
        return atom.value

    @property
    def value_format(self):
        atom = self.atom
        if atom is None:
            return None
        return atom.format

    @property
    def channels(self):
        return self.channel_atoms.keys()

    def get(self, name, default=None):
        return self.channel_atoms.get(name, default)

    def with_channel(self, channel_name: str) -> "PRecord":
        return self.set(active_channel=channel_name)

    def merge(self, **kwargs) -> "PRecord":
        channel_atoms = self.channel_atoms
        e = channel_atoms.evolver()
        for key, value in kwargs.items():
            e[key] = PAtom(value=value, format=_infer_format_from_type(key, value))
        return self.set(channel_atoms=e.persistent())

    def with_value(self, value: Any):
        active_channel = self.active_channel
        channel_atoms = self.channel_atoms
        try:
            return self.set(
                channel_atoms=channel_atoms.set(
                    active_channel,
                    channel_atoms[active_channel].set(value=value)
                )
            )
        except KeyError:
            return self.set(
                channel_atoms=channel_atoms.set(
                    active_channel,
                    PAtom(
                        value=value,
                        format=_infer_format_from_type(active_channel, value)
                    ),
                )
            )

    @classmethod
    def from_object(cls, obj, channel_name='default'):
        return cls(
            id=str(id(obj)),
            timestamp=time.time(),
            active_channel=channel_name,
            channel_atoms=pmap({
                channel_name: PAtom(
                    value=obj,
                    format=_infer_format_from_type(channel_name, obj),
                )
            })
        )
