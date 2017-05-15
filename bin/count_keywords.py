

import click
import csv

from pyspark import SparkContext
from pyspark.sql import SparkSession

from tech import Novel, WordList


def count_keywords(novel, words):
    """Accumulate keyword counts.

    Args:
        novel (Novel)
        words (WordList)

    Returns: (word row, category row)
    """
    word_counts = novel.count_keywords(words.word_set())

    # Shared novel metadata.
    metadata = dict(
        _title=novel.title,
        _auth_last=novel.authLast,
        _auth_first=novel.authFirst,
        _year=novel.publDate,
    )

    word_row = {**metadata, **word_counts}

    cat_counts = {key: 0 for key in words.keys()}

    # Merge category totals.
    for cat, terms in words.items():
        for term in terms:
            cat_counts[cat] += word_counts[term]

    cat_row = {**metadata, **cat_counts}

    return word_row, cat_row


@click.command()
@click.argument('novels_path', type=click.Path())
@click.argument('words_path', type=click.Path())
@click.argument('word_csv_fh', type=click.File('w'))
@click.argument('cat_csv_fh', type=click.File('w'))
def main(novels_path, words_path, word_csv_fh, cat_csv_fh):
    """Count technology keywords in Chicago.
    """
    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    # Parse word list.
    words = WordList.from_file(words_path)

    # Count keywords.
    counts = (
        spark.read.parquet(novels_path)
        .rdd.map(Novel)
        .map(lambda n: count_keywords(n, words))
        .collect()
    )

    print(counts)

    # Write CSV.

    word_fnames = list(counts[0][0].keys())
    word_writer = csv.DictWriter(word_csv_fh, word_fnames)
    word_writer.writeheader()

    cat_fnames = list(counts[0][1].keys())
    cat_writer = csv.DictWriter(cat_csv_fh, cat_fnames)
    cat_writer.writeheader()

    for word_row, cat_row in counts:
        word_writer.writerow(word_row)
        cat_writer.writerow(cat_row)


if __name__ == '__main__':
    main()
