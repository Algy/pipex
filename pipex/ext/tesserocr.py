import warnings
import locale

from ..poperators import pipe
from PIL import Image

warnings.warn(
    "Changed locale LC_ALL to 'C' because tesserocr library crashes without setting like that",
    RuntimeWarning
)

locale.setlocale(locale.LC_ALL, "C")
from tesserocr import PyTessBaseAPI

class read(pipe):
    def __init__(self, lang="eng", oem=1, psm=7):
        self.lang = lang
        self.oem = oem
        self.psm = psm

    def transform(self, our, precords):
        # unordered!
        with PyTessBaseAPI(lang=self.lang, oem=self.oem, psm=self.psm) as api:
            for precord in precords:
                img = Image.fromarray(precord.value)
                api.SetImage(img)
                ocr_text = api.GetUTF8Text().strip()
                yield precord.with_channel_item("ocr_text", ocr_text)
