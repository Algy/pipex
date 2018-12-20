from ..poperators import pipe

class display(pipe):
    def transform(self, our, precords):
        from IPython.display import display

        for precord in precords:
            display(precord.value)
            yield precord
