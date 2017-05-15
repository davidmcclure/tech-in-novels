

import click
import csv

from pyspark import SparkContext
from pyspark.sql import SparkSession

from tech import Novel, WordList


class Job:

    def __init__(self, sc=None, spark=None):
        """Initialize or set session and context.

        Args:
            sc (SparkContext)
            spark (SparkSession)
        """
        if not sc:
            sc = SparkContext()

        if not spark:
            spark = SparkSession(sc).builder.getOrCreate()

        self.sc = sc
        self.spark = spark


class CountKeywords(Job):

    def __call__(self, novels_path, words):
        """Count technology keywords in Chicago.

        Args:
            novels_path (str)
            words (set)
        """
        df = self.spark.read.parquet(novels_path)

        counts = (
            df.rdd.map(Novel)
            .map(lambda n: dict(
                _title=n.title,
                _auth_last=n.authLast,
                _auth_first=n.authFirst,
                _year=n.publDate,
                **n.count_keywords(words)
            ))
        )

        return counts.collect()


@click.command()
@click.argument('novels_path', type=click.Path())
@click.argument('words_path', type=click.Path())
@click.argument('word_csv_fh', type=click.File('w'))
def main(novels_path, words_path, word_csv_fh):
    """Count technology keywords in Chicago.
    """
    # Parse word list.
    words = WordList.from_file(words_path)

    # Count words.
    job = CountKeywords()
    counts = job(novels_path, words.word_set())

    fieldnames = list(counts[0].keys())
    writer = csv.DictWriter(word_csv_fh, fieldnames)
    writer.writeheader()

    for row in counts:
        writer.writerow(row)


if __name__ == '__main__':
    main()
