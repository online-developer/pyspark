from functools import partial

def identity(x):
    return x


def compose(*funcs):
    if not funcs:
        return identity
    else:
        return Compose(funcs)


class Compose(object):
    def __init__(self, funcs):
        funcs = tuple(reversed(funcs))
        self.first = funcs[0]
        self.funcs = funcs[1:]

    def __call__(self, *args, **kwargs):
        ret = self.first(*args, **kwargs)
        for f in self.funcs:
            ret = f(ret)
        return ret

    def __getstate__(self):
        return self.first, self.funcs

    def __setstate__(self, state):
        self.first, self.funcs = state

    @property
    def __name__(self):
        try:
            return '__of__'.join(
                    f.__name__ for f in reversed((self.first,) + self.func)
                    )
        except AttributeError:
            return type(self).__name__


class chain(object):
    def __init__(self, *args, **kwargs):
        if not args:
            raise TypeError('__init__() takes atleast 2 arguments (1 given)')
        func, args = args[0], args[1:]
        if not callable(func):
            raise TypeError('Input must be callable')

        if(
            hasattr(func, 'func')
            and hasattr(func, 'args')
            and hasattr(func, 'keywords')
            and isinstance(func.args, tuple)
            ):
            _kwargs = {}
            if func.keywords:
                _kwargs.update(func.keywords)
            _kwargs.update(kwargs)
            args = func.args + args
            func = func.func

        if kwargs:
            self._partial = partial(func, *args, **kwargs)
        else:
            self._partial = partial(func, *args)

    @property
    def func(self):
        return self._partial.func


    @property
    def args(self):
        return self._partial.args


    @property
    def keywords(self):
        return self._partial.keywords


    def __call__(self, *args, **kwargs):
        try:
            return self._partial(*args, **kwargs)
        except TypeError as ex:
            return self.bind(*args, **kwargs)


    def bind(self, *args, **kwargs):
        return type(self)(self,*args,**kwargs)



