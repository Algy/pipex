import numpy as np

from os.path import splitext, basename
from ..poperators import pipe, sink
from PIL import Image

class open(pipe):
    def __init__(self, as_numpy_array=True, file_name_as_id=True, ext_stripped_id=True):
        self.as_numpy_array = as_numpy_array
        self.file_name_as_id = file_name_as_id
        self.ext_stripped_id = ext_stripped_id

    def transform(self, our, precords):

        for precord in precords:
            file_name = precord.value
            if self.as_numpy_array:
                with Image.open(file_name) as img:
                    value = np.array(img)
            else:
                img = Image.open(file_name)
                img.load()
                value = img

            new_precord = precord.merge(file_name=file_name).with_channel("image").with_value(value)
            if self.file_name_as_id:
                new_id = basename(file_name)
                if self.ext_stripped_id:
                    new_id = splitext(new_id)[0]
                new_precord = new_precord.with_id(new_id)
            yield new_precord


class save(sink):
    def save(self, precord):
        value = precord.value
        file_name = precord['file_name']
        if isinstance(value, Image.Image):
            image = value
        else:
            image = Image.fromarray(value)
        image.save(file_name)
