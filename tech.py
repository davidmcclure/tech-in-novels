

import attr
import functools
import yaml

from collections import Counter


def with_model(model_cls):
    def outer(f):
        @functools.wraps(f)
        def inner(row, *args, **kwargs):
            return f(model_cls(row), *args, **kwargs)
        return inner
    return outer


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

    def count_keywords(self, words):
        """Count occurrences of each keyword in a list.

        Args:
            words (set)
        """
        counts = Counter()

        for t in self.tokens:
            if t.token in words:
                counts[t.token] += 1

        return counts


class WordList(dict):

    @classmethod
    def from_file(cls, path):
        with open(path) as fh:
            return cls(yaml.load(fh))

    def word_set(self):
        """Get a flat set of keywords.

        Returns: set
        """
        return set([w for wl in list(self.values()) for w in wl])


@with_model(Novel)
def get_full_name(novel):
    return novel.full_name()


@with_model(Novel)
def count_keywords(novel, words):
    return novel.count_keywords(words)
