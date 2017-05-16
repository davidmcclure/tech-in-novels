

import click
import csv

from pyspark import SparkContext
from pyspark.sql import SparkSession

from tech import Novel, Keywords


def count_keywords(novel, keywords):
    """Accumulate keyword counts.

    Args:
        novel (Novel)
        keywords (Keywords)

    Returns: (word row, category row)
    """
    c_counts, w_counts = novel.count_keywords(keywords)

    # Shared novel metadata.
    metadata = dict(
        _title=novel.title,
        _auth_last=novel.authLast,
        _auth_first=novel.authFirst,
        _year=novel.publDate,
    )

    c_row = {**metadata, **c_counts}
    w_row = {**metadata, **w_counts}

    return w_row, c_row


@click.command()
@click.argument('novels_path', type=click.Path())
@click.argument('words_path', type=click.Path())
@click.argument('w_csv_fh', type=click.File('w'))
@click.argument('c_csv_fh', type=click.File('w'))
def main(novels_path, words_path, w_csv_fh, c_csv_fh):
    """Count technology keywords in Chicago.
    """
    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    # Parse word list.
    words = Keywords.from_file(words_path)

    # Count keywords.
    rows = (
        spark.read.parquet(novels_path)
        .rdd.map(Novel)
        .map(lambda n: count_keywords(n, words))
        .collect()
    )

    w_rows, c_rows = zip(*rows)

    # Write CSV.

    w_fnames = list(w_rows[0].keys())
    w_writer = csv.DictWriter(w_csv_fh, w_fnames)
    w_writer.writeheader()

    for row in w_rows:
        w_writer.writerow(row)

    c_fnames = list(c_rows[1].keys())
    c_writer = csv.DictWriter(c_csv_fh, c_fnames)
    c_writer.writeheader()

    for row in c_rows:
        c_writer.writerow(row)


if __name__ == '__main__':
    main()
