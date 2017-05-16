

import attr
import yaml


@attr.s
class Model:

    row = attr.ib()

    def __getattr__(self, key):
        """Alias the underlying row.
        """
        return getattr(self.row, key)


class Novel(Model):

    def count_keywords(self, keywords):
        """Count occurrences of each keyword in a list.

        Args:
            keywords (Keywords)
        """
        words = keywords.flat_words()

        w_counts = {w: 0 for w in words}

        for t in self.tokens:
            if t.token in words:
                w_counts[t.token] += 1

        c_counts = {c: 0 for c in keywords.keys()}

        for cat, cat_words in keywords.items():
            for word in cat_words:
                c_counts[cat] += w_counts[word]

        return c_counts, w_counts


class Keywords(dict):

    @classmethod
    def from_file(cls, path):
        with open(path) as fh:
            return cls(yaml.load(fh))

    def flat_words(self):
        """Get a flat set of keywords.

        Returns: set
        """
        return set([w for wl in list(self.values()) for w in wl])
