from ..poperators import pipe, sink

class open(pipe):
    def __init__(self, as_numpy_array=True):
        self.as_numpy_array = as_numpy_array

    def transform(self, our, precords):
        import numpy as np
        from PIL import Image

        for precord in precords:
            file_name = precord.value
            if self.as_numpy_array:
                with Image.open(file_name) as img:
                    value = np.array(img)
            else:
                img = Image.open(file_name)
                img.load()
                value = img
            yield precord.merge(file_name=file_name).with_channel("image").with_value(value)


class save(sink):
    def process(self, our, precords):
        for precord in precords:
            from PIL import Image
            value = precord.value
            file_name = precord['file_name']
            if isinstance(value, Image.Image):
                image = value
            else:
                image = Image.fromarray(value)
            image.save(file_name)
            yield precord
