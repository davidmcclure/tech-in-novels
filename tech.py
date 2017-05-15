

import attr
import functools


@attr.s
class Model:

    row = attr.ib()

    def __getattr__(self, key):
        """Alias the underlying row.
        """
        return getattr(self.row, key)


class Novel(Model):

    def full_name(self):
        return '{} {}'.format(self.authFirst, self.authLast)


def with_model(model_cls):
    def outer(f):
        @functools.wraps(f)
        def inner(row):
            return f(model_cls(row))
        return inner
    return outer


@with_model(Novel)
def get_full_name(novel):
    return novel.full_name()
