import sys
import os
import csv

from collections import Counter, defaultdict
from format import write_result, load_source
from itertools import groupby

COL_FILENAME = 0  # "Filename" column number
COL_EMAIL = 1
COL_FULLNAME = 2
COL_LINK = 3
COL_NOT_FOUND_IN_USER_TASKS = 4
COL_TASKNAME_IS_AMBIGUOS = 5
COL_NAME_NORMALIZED = 6
COL_NAME_TROUBLESOME = 7


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

    # Attaching then the rest by matching their urls to urls of groups
    # created on previous stage
    for d in not_used:
        for k, d2 in first_pass.items():
            if d[COL_LINK] and d[COL_LINK] in [x[COL_LINK] for x in d2]:
                first_pass[k].append(d)
                break
        else:
            orphans.append(d)

    second_pass = []
    # Grouping the final chunk of unmatched data by urls
    sorted_data = sorted(orphans, key=lambda x: x[COL_LINK])
    for key, group in groupby(sorted_data, key=lambda x: x[COL_LINK]):
        second_pass.append(list(group))

    return list(first_pass.values()) + second_pass


if __name__ == '__main__':
    if len(sys.argv) < 1:
        sys.exit('Usage: {} source-filename'.format(sys.argv[0]))
    source_filename = sys.argv[1]
    if not os.path.exists(source_filename):
        sys.exit('File "{}" does not exist'.format(source_filename))

    header, data = load_source(source_filename)
    grouped_data = group_by_link_and_name(data)

    write_result(header, grouped_data, 1, [COL_NAME_NORMALIZED])
