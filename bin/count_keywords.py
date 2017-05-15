

import click

from pyspark import SparkContext
from pyspark.sql import SparkSession

from tech import Novel, get_full_name, count_keywords


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

    def __call__(self, novels_path):
        """Count technology keywords in Chicago.
        """
        novels = self.spark.read.parquet(novels_path)

        words = set(['god', 'man'])

        counts = (
            novels.rdd.map(lambda n: count_keywords(n, words))
            .reduce(lambda a, b: a + b)
        )

        print(counts)


@click.command()
@click.argument('novels_path', type=click.Path())
def main(novels_path):
    """Count technology keywords in Chicago.
    """
    job = CountKeywords()
    job(novels_path)


if __name__ == '__main__':
    main()
