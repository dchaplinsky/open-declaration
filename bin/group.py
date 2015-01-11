import sys
import os
import csv

from collections import Counter, defaultdict
from format import write_result, load_source
from itertools import groupby

DEBUG_COL_FILENAME = 0  # "Filename" column number
DEBUG_COL_EMAIL = 1
DEBUG_COL_FULLNAME = 2
DEBUG_COL_LINK = 3
DEBUG_COL_NOT_FOUND_IN_USER_TASKS = 4
DEBUG_COL_TASKNAME_IS_AMBIGUOS = 5
DEBUG_COL_NAME_NORMALIZED = 6
DEBUG_COL_NAME_TROUBLESOME = 7

COL_FILENAME = 1  # "Filename" column number
COL_LINK = 312  # Link to the original document
COL_NAME_NORMALIZED = 313


def save_intermediate_results(filename, data):
    with open(filename, 'w', newline='', encoding='utf-8') as dest:
        writer = csv.writer(dest)

        for row in data:
            writer.writerow(row)


def group_by_link_and_name(data):
    print('Grouping the data...')

    grouper = Counter()
    grouper.update(d[COL_NAME_NORMALIZED] for d in data)

    first_pass = defaultdict(list)
    not_used = []
    orphans = []

    # Grouping by normalized name first.
    for d in data:
        name = d[COL_NAME_NORMALIZED]
        if name and grouper[name] > 1:
            first_pass[name].append(d)
        else:
            not_used.append(d)

    print("{} rows was matched into {} groups on a first pass".format(len(data) - len(not_used), len(first_pass.keys())))
    print("{} rows wasn't matched by name. Trying to match them by links to existing groups...".format(len(not_used)))

    # Attaching then the rest by matching their urls to urls of groups
    # created on previous stage
    for d in not_used:
        for k, d2 in first_pass.items():
            if d[COL_LINK] and d[COL_LINK] in [x[COL_LINK] for x in d2]:
                first_pass[k].append(d)
                break
        else:
            orphans.append(d)

    print("{} rows wasn't matched by link to existing groups".format(len(orphans)))

    second_pass = []
    # Grouping the final chunk of unmatched data by urls
    sorted_data = sorted(orphans, key=lambda x: x[COL_LINK])
    stupid_orphans = []
    for key, group in groupby(sorted_data, key=lambda x: x[COL_LINK]):
        if not key:
            stupid_orphans = list(group)
        else:
            second_pass.append(list(group))

    print("So we've grouped them by links only into {} groups".format(len(second_pass)))

    print("{} has no link and also cannot be matched by name".format(len(stupid_orphans)))
    print("{} groups was created".format(len(first_pass.keys()) + len(second_pass) + 1))
    return list(first_pass.values()) + second_pass + [stupid_orphans]


if __name__ == '__main__':
    if len(sys.argv) < 1:
        sys.exit('Usage: {} source-filename'.format(sys.argv[0]))
    source_filename = sys.argv[1]
    if not os.path.exists(source_filename):
        sys.exit('File "{}" does not exist'.format(source_filename))

    header, data = load_source(source_filename)
    grouped_data = group_by_link_and_name(data)

    write_result(header, grouped_data, 1, [COL_NAME_NORMALIZED])
