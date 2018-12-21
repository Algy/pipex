import warnings

from ..poperators import pipe

warnings.warn(
    "Changed locale LC_ALL to 'C' because tesserocr library crashes without setting like that",
    RuntimeWarning
)

locale.setlocale(locale.LC_ALL, "C")
from tesserocr import PyTessBaseAPI

class tesseract(pipe):
    def __init__(self, lang="eng", oem=1, psm=7):
        self.lang = lang
        self.oem = oem
        self.psm = psm

    def _ocr(self, batch):
        results = []
        with PyTessBaseAPI(lang=self.lang, oem) as api:
            for arr in batch:
                api.SetImage(Image.fromarray(arr))
                results.append(api.GetUTF8Text())
        return results

    def map(self, our, precords):
        # unordered!
        with PyTessBaseAPI(lang=self.lang, oem=self.oem, psm=self.psm) as api:
            for precord in precord:
                api.SetImage(precord.value)
                ocr_text = api.GetUTF8Text().strip()
                yield precord.merge(ocr_text=ocr_text).with_channel(ocr_text)
