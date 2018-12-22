from ..poperators import pipe

class tqdm(pipe):
    def transform(self, our, precords):
        from tqdm import tqdm
        yield from tqdm(precords)

class tqdm_notebook(pipe):
    def transform(self, our, precords):
        from tqdm import tqdm_notebook
        yield from tqdm_notebook(precords)

class tqdm_gui(pipe):
    def transform(self, our, precords):
        from tqdm import tqdm_gui
        yield from tqdm_gui(precords)
