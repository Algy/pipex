import re
import numpy as np

from subprocess import run, Popen, PIPE
from ..poperators import pipe
from .model import BaseVideo, FileVideo

from typing import Optional

class frames(pipe):
    def __init__(self, fps: Optional[str] = None):
        self.fps = fps

    def detect_shape(self, video):
        _, err = Popen(["ffmpeg", "-i", video.file_name], stderr=PIPE).communicate()
        lines = err.decode().splitlines()
        for line in lines:
            if "Video:" in line:
                for match in re.finditer(r"(\d+)x(\d+)", line):
                    try:
                        shape = (int(match.group(2)), int(match.group(1)))
                        if shape[0] <= 0 or shape[1] <= 0: continue
                        return shape
                    except ValueError:
                        continue
        raise ValueError("Couldn't detect size of {}".format())

    def get_frames(self, video):
        if not isinstance(video, BaseVideo):
            raise TypeError("BaseVideo object expected, got {!r}".format(video))
        elif not isinstance(video, FileVideo):
            raise NotImplementedError("FFmpeg only supports FileVideo currently")

        h, w = self.detect_shape(video)
        LEN = h * w * 3
        proc_args = ["ffmpeg", "-i", video.file_name]
        if self.fps is not None:
            proc_args += ["-vf", "fps={}".format(self.fps)]
        proc_args += [
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "pipe:"
        ]

        proc = Popen(proc_args, stdout=PIPE)
        try:
            while True:
                scene = proc.stdout.read(LEN)
                if len(scene) < LEN:
                    break
                arr = np.frombuffer(scene, dtype=np.uint8)
                arr = arr.reshape(h, w, 3)
                yield arr
            retcode = proc.wait()
            if retcode != 0:
                raise RuntimeError(f"FFmpeg failed with retcode {retcode}")
        except:
            proc.kill()
            raise

    def transform(self, our, precords):
        for precord in precords:
            for arr in self.get_frames(precord.value):
                yield precord.with_channel_item("image_frame", arr)


class select_key_frame(pipe):
    pass
