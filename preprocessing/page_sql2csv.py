import csv
import sys
import gzip

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

# allow large content in the dump
csv.field_size_limit(sys.maxsize)


def is_insert(line):
    """
    Returns true if the line begins a SQL insert statement.
    """
    return line.startswith('INSERT INTO') or False


def get_values(line):
    """
    Returns the portion of an INSERT statement containing values
    """
    return line.partition('` VALUES ')[2]


def values_sanity_check(values):
    """
    Ensures that values from the INSERT statement meet basic checks.
    """
    assert values
    assert values[0] == '('
    # Assertions have not been raised
    return True


def parse_values(values, outfile):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
    )

    writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
    for reader_row in reader:
        for column in reader_row:
            # If our current string is empty...
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            # If our string starts with an open paren
            if column[0] == "(":
                # Assume that this column does not begin
                # a new row.
                new_row = False
                # If we've been filling out a row
                if len(latest_row) > 0:
                    # Check if the previous entry ended in
                    # a close paren. If so, the row we've
                    # been filling out has been COMPLETED
                    # as:
                    #    1) the previous entry ended in a )
                    #    2) the current entry starts with a (
                    if latest_row[-1][-1] == ")":
                        # Remove the close paren.
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                # If we've found a new row, write it out
                # and begin our new one
                if new_row:
                    writer.writerow(latest_row)
                    latest_row = []
                # If we're beginning a new row, eliminate the
                # opening parentheses.
                if len(latest_row) == 0:
                    column = column[1:]
            # Add our column to the row we're working on.
            latest_row.append(column)
        # At the end of an INSERT statement, we'll
        # have the semicolon.
        # Make sure to remove the semicolon and
        # the close paren.
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            writer.writerow(latest_row)


def page_sql_to_csv(sql_gz_path, csv_path):
    with open(csv_path, 'w') as outfile:
        csv.writer(outfile, quoting=csv.QUOTE_MINIMAL).writerow(['page_id', 'page_namespace', 'title', 'page_restrictions', 'page_counter', 'page_is_redirect', 'page_is_new', 'page_random','page_touched', 'page_links_updated', 'page_latest', 'page_len', 'page_no_title_convert','page_content_model','page_lang'])

    for line in gzip.open(sql_gz_path, 'rt', encoding='utf-8', errors='ignore'):
        # Look for an INSERT statement and parse it.
        if is_insert(line):
            values = get_values(line)
            if values_sanity_check(values):
                parse_values(values, open(csv_path, 'a', encoding='utf-8'))



page_sql_gz_path = "./ukwiki-20180701-page.sql.gz"
page_csv_path = "./ukwiki-20180701-page.csv"

page_sql_to_csv(page_sql_gz_path, page_csv_path)
