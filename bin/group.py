import sys
import os
import csv

from collections import Counter, defaultdict
from itertools import groupby

COL_FILENAME = 0  # "Filename" column number
COL_EMAIL = 1
COL_FULLNAME = 2
COL_LINK = 3
COL_NOT_FOUND_IN_USER_TASKS = 4
COL_TASKNAME_IS_AMBIGUOS = 5
COL_NAME_NORMALIZED = 6
COL_NAME_TROUBLESOME = 7


def load_source(filename):
    header = None
    data = []
    with open(filename, 'r', newline='', encoding='utf-8') as source:
        print('Reading the file "{}"'.format(filename))
        reader = csv.reader(source)
        header = next(reader)  # skip the header but store for later usage
        print('Loading rows...')
        for row in reader:
            data.append(row)
        print('Loaded rows: {}'.format(len(data)))

    return header, data


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

    for d in data:
        name = d[COL_NAME_NORMALIZED]
        if name and grouper[name] > 1:
            first_pass[name].append(d)
        else:
            not_used.append(d)

    print(len(not_used))
    for d in not_used:
        for k, d2 in first_pass.items():
            if d[COL_LINK] in [x[COL_LINK] for x in d2]:
                first_pass[k].append(d)
                break
        else:
            orphans.append(d)

    grouper2 = Counter()
    grouper2.update(d[COL_LINK] for d in orphans)

    # for k, v in grouper2.most_common():
    #     print(k, v)

    save_intermediate_results("orphans.csv", orphans)


if __name__ == '__main__':
    if len(sys.argv) < 1:
        sys.exit('Usage: {} source-filename'.format(sys.argv[0]))
    source_filename = sys.argv[1]
    if not os.path.exists(source_filename):
        sys.exit('File "{}" does not exist'.format(source_filename))

    header, data = load_source(source_filename)
    grouped_data = group_by_link_and_name(data)
