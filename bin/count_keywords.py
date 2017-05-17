

import click
import csv

from pyspark import SparkContext
from pyspark.sql import SparkSession

from tech_in_novels import Novel, Keywords


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
    keywords = Keywords.from_file(words_path)

    # Count keywords.
    rows = (
        spark.read.parquet(novels_path)
        .limit(10) # TODO|dev
        .rdd.map(Novel)
        .map(lambda n: n.csv_rows(keywords))
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
