class PMeta(type):
    def __or__(cls, other):
        if isinstance(other, PMeta):
            other_instance = other()
        else:
            other_instance = other
        return other_instance.bound(cls())

    def __ror__(cls, left):
        return cls().bound(left)


class P(metaclass=PMeta):
    def __init__(self):
        self.arrs = iter([])

    def __iter__(self):
        return iter(self.arrs)

    def bound(self, arrs):
        raise NotImplementedError

    def __or__(self, other):
        if isinstance(other, PMeta):
            other_instance = other()
        else:
            other_instance = other
        return other_instance.bound(self)

    def __ror__(self, other):
        if isinstance(other, PMeta):
            other = other()
        # other | self
        return self.bound(other)

    def __repr__(self, ):
        return f"<{self.__class__.__name__} {self.__dict__!r}>"

class source(P):
    def __init__(self, arrs):
        self.arrs = arrs

    def bound(self, other):
        raise RuntimeError(f"{self!r} cannot be bound to other stream")

class sink(P):
    def process(self, arrs):
        for _ in arrs:
            pass
        pass

    def bound(self, other):
        self.process(other)
        return self

class pipe(P):
    def map(self, arrs):
        raise NotImplementedError

    def bound(self, other):
        self.arrs = self.map(other)
        return self

class tap(pipe):
    def __init__(self, fn):
        super().__init__()
        self.fn = fn

    def map(self, arrs):
        fn = self.fn
        for arr in arrs:
            fn(arr)
            yield arr

class done(sink):
    pass

class sink_each(sink):
    def __init__(self, fn):
        super().__init__()
        self.fn = fn

    def process(self, arrs):
        for t in arrs:
            self.fn(t)
