

import click

from pyspark import SparkContext
from pyspark.sql import SparkSession

from tech import Novel, WordList, count_keywords


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
        novels = self.spark.read.parquet(novels_path)

        counts = (
            novels.rdd.map(lambda n: count_keywords(n, words))
            .reduce(lambda a, b: a + b)
        )

        print(counts)


@click.command()
@click.argument('novels_path', type=click.Path())
@click.argument('words_path', type=click.Path())
def main(novels_path, words_path):
    """Count technology keywords in Chicago.
    """
    # Parse word list.
    words = WordList.from_file(words_path)

    # Count words.
    job = CountKeywords()
    job(novels_path, words.word_set())


if __name__ == '__main__':
    main()
