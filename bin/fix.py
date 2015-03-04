import sys
import os
import json
import csv
import re
import string

import Levenshtein

from datetime import datetime
from hashlib import md5
from collections import defaultdict, Counter
from decimal import Decimal


COL_FILENAME = 1  # "Filename" column number
COL_EMAIL = 4  # "Email" column number
COL_NAME = 11  # "Full Name" column number
COL_LINK = 318  # Link to the original document
COL_HASH = 319  # Hash column augmented in the processing
COL_NOT_FOUND_IN_USER_TASKS = 320  # Debug flags
COL_TASKNAME_IS_AMBIGUOS = 321  # Debug flags
COL_NAME_NORMALIZED = 322  # Attempt to normalize lastnames
COL_NAME_TROUBLESOME = 323  # Lastnames troublesomnes flag
DEBUG_COLS = (
    COL_LINK, COL_NAME, COL_FILENAME, COL_EMAIL, COL_NOT_FOUND_IN_USER_TASKS,
    COL_TASKNAME_IS_AMBIGUOS, COL_NAME_NORMALIZED,
    COL_NAME_TROUBLESOME)

USELESS_COLS = (5, 6, 7, 8, 9, 10, COL_HASH, COL_NOT_FOUND_IN_USER_TASKS,
                COL_TASKNAME_IS_AMBIGUOS, COL_NAME_TROUBLESOME)  # No point in processing and writing these into the output
BOOLEAN_COLS = (2, 313, 315)  # Should be treated as booleans
#NON_HASHABLE_COLS = (0, 1, 4, 313, 315)  # Technical fields that shouldn't be used for deduplication, strict version
NON_HASHABLE_COLS = (0, 1, 2, 3, 4, 312, 313, 314, 315, 316, 317)  # Technical fields that shouldn't be used for deduplication
CAPITALIZE_COLS = (13, 14)
YEAR_COLS = (3, 187, 191, 195, 199, 203, 207, 211, 215, 219, 223, 227, 231, 235, 239, 243, 245, 247, 249, 251, 253, 255,
             257, 259, 261, 263, 265, 267, 269, 271)  # Should be treated as year values (not subject to decimal detection)


def title(s):
    chunks = s.split()
    chunks = map(lambda x: string.capwords(x, "-"), chunks)
    return " ".join(chunks)


class ValidationError(Exception):
    pass


def process_source(source_filename, tasks_filename, user_tasks_filename):
    tasks, ambiguous_tasks = parse_tasks(tasks_filename)
    user_tasks = parse_user_tasks(user_tasks_filename)

    header = None
    data = []
    invalid = []
    with open(source_filename, 'r', newline='', encoding='utf-8') as source:
        print('Reading the file "{}"'.format(source_filename))
        reader = csv.reader(source)
        header = next(reader)  # skip the header but store for later usage
        print('Loading rows...')
        for row in reader:
            try:
                data.append(augment(normalize(clean(row)), tasks, ambiguous_tasks, user_tasks))
            except ValidationError:
                invalid.append(row)
        print('Loaded rows: {} and {} invalid'.format(len(data), len(invalid)))

    timestamp = datetime.now()
    write_dest('processed_{:%Y-%m-%d_%H:%M:%S}.csv'.format(timestamp), data, header)
    write_dest('invalid_{:%Y-%m-%d_%H:%M:%S}.csv'.format(timestamp), invalid, header)

    # write_debug_dest('processed_debug.csv', data, header)
    # write_debug_dest('invalid_debug.csv', invalid, header)


def normalize_fname(filename):
    filename = filename.strip().replace(" ", "_").lower()

    filename = re.sub("\.pdf$", "", filename)

    # TODO: check if we should get rid of dekl/decl
    filename = re.sub("\.dekl$", "", filename)
    return filename.rstrip('.').split('/')[-1]


def normalize_email(email):
    return email.lower().strip().replace(" ", "")


def normalize_name(name):
    name = name.replace(";", " ").replace(".", " ").replace(",", " ") \
        .replace("?", " ")

    name = re.sub("\([^)]*\)", "", name)
    name = re.sub("\s+", " ", name)

    return name.strip()


def parse_user_tasks(filename):
    tasks_per_user = {}

    with open(filename, 'r') as tasks:
        print('Reading tasks file "{}"'.format(filename))

        for task_line in tasks:
            users_tasks = json.loads(task_line)
            email = normalize_email(users_tasks["email"])
            data = tasks_per_user.setdefault(email, defaultdict(list))

            for task in users_tasks["files"]:
                task_fname = normalize_fname(task)

                data[task_fname[:3]].append((task_fname, task.strip()))

    return tasks_per_user


def parse_tasks(filename):
    data = defaultdict(list)
    ambiguous_tasks = Counter()

    with open(filename, 'r') as tasks:
        print('Reading tasks file "{}"'.format(filename))
        for task in tasks:
            task_fname = normalize_fname(task)
            ambiguous_tasks.update([task_fname])
            data[task_fname[:3]].append((task_fname, task.strip()))

    return data, ambiguous_tasks


def write_dest(filename, data, header):
    with open(filename, 'w', newline='', encoding='utf-8') as dest:
        writer = csv.writer(dest)
        data.insert(0, header)
        for row in data:
            writer.writerow([str(c) for i, c in enumerate(row) if i not in USELESS_COLS])
    print('Result was written to: {}'.format(filename))


def write_debug_dest(filename, data, header):
    with open(filename, 'w', newline='', encoding='utf-8') as dest:
        writer = csv.writer(dest)

        writer.writerow([str(c) for i, c in enumerate(header) if i in DEBUG_COLS])

        for row in data:
            writer.writerow([str(c) for i, c in enumerate(row) if i in DEBUG_COLS])
    print('Result was written to: {}'.format(filename))


def clean(row):
    """Fixing general issues with the data"""
    cleaned_row = []
    for num, col in enumerate(row):
        col = col.replace('\n', ';')
        col = re.sub(r'\s', ' ', col)
        col = col.strip()
        # Yeah, "Прочерк" it is...
        if col in ('0', 'Прочерк', 'прочерк'):
            col = ''
        if not any(filter(lambda x: x not in ('-', '—'), col)):
            col = ''
        if num in BOOLEAN_COLS:
            col = 'true' if len(col) > 0 else 'false'
        elif col != '':
            if num in CAPITALIZE_COLS:
                col = col[0].upper() + col[1:]
            # People put this in whatever way they want
            col = re.sub(r'["(-\[]приховано[")-\]]', 'приховано', col, flags=re.I)
            if col == 'Приховано':
                col = 'приховано'

            if num in YEAR_COLS and len(col) <= 10:
                # Fix year values
                col = ''.join(filter(lambda x: x.isdigit(), col))
                if len(col) == 2:
                    col = '20{}'.format(col)
            else:
                # Fix decimals
                col = re.sub(r'грн?\.?$', '', col).rstrip()
                if not any(filter(lambda x: not x.isdigit() and x not in (',', '.', ' '), col)) and ', ' not in col:
                    # Remove spaces if it looks like a decimal without any other chars
                    col = col.replace(' ', '')
                col = re.sub(r'(\d+),(\d+)', r'\g<1>.\g<2>', col)
                # Coerce money-looking decimals to a common format
                col = re.sub(r'\d+\.\d{1,2}', lambda x: str(Decimal(x.group(0)).quantize(Decimal('.01'))), col)
                if all(map(lambda x: x.isdigit(), col)):
                    col = '{}.00'.format(col)

            # Fix apostrophes
            col = re.sub(r'([^a-zA-Z\d_])["\'`*]([^a-zA-Z\d_])', r'\g<1>’\g<2>', col)
            # Fix Ukrainian "і"
            col = re.sub(r'([^a-zA-Z\d_])[1i]([^a-zA-Z\d_])', r'\g<1>і\g<2>', col)
        cleaned_row.append(col)

    return cleaned_row


def normalize(row):
    """Convert certain fields to a defined format"""
    # Should have at least one of this
    if row[COL_NAME] == '' and row[COL_FILENAME] == '':
        raise ValidationError

    normalized_row = row.copy()

    normalized_row[COL_NAME] = string.capwords(row[COL_NAME])
    normalized_row[COL_FILENAME] = normalize_fname(row[COL_FILENAME])

    return normalized_row


def augment(row, all_tasks, ambiguous_tasks, user_tasks):
    """Add new helper columns to the dataset, e.g. hash and link to the original document"""
    filename = row[COL_FILENAME]

    # Add some extra rows upfront
    row += [""] * 10
    row[COL_TASKNAME_IS_AMBIGUOS] = False
    row[COL_NOT_FOUND_IN_USER_TASKS] = True

    name_fragments = list(map(title, normalize_name(row[COL_NAME]).split(" ")))
    row[COL_NAME_NORMALIZED] = " ".join(name_fragments[:3])
    row[COL_NAME_TROUBLESOME] = len(name_fragments) != 3

    if row[COL_EMAIL] in user_tasks:
        tasks = user_tasks[row[COL_EMAIL]]

        # Some users got planned tasks and beta tasks as well
        # Beta tasks aren't represented in tasks.json
        if filename[:3] not in tasks:
            tasks = all_tasks
        else:
            row[COL_NOT_FOUND_IN_USER_TASKS] = False
    else:
        tasks = all_tasks

    # Use prefix to narrow down the search space, 3 chars cause surnames (which are the filenames in the majority
    # of cases, others being technical values) aren't likely to be less than this
    if filename[:3] in tasks:
        # Use Jaro-Winkler distance to get the best similarity guess between the normalized manually entered filename
        # and the value from tasks file normalized in the same way
        potential_links = map(lambda x: (Levenshtein.jaro_winkler(filename, x[0]), x[1]), tasks[filename[:3]])
        row[COL_LINK] = max(potential_links)[1]
    elif len(row[COL_NAME]) < 10:
        # If no prefix match just skip it entirely, it's most likely not a real filename
        raise ValidationError

    if ambiguous_tasks[os.path.basename(row[COL_LINK])] > 1:
        row[COL_TASKNAME_IS_AMBIGUOS] = True

    # A helper column for a simple hash-based deduplication attempt
    hashable_string = ':'.join((str(c) for i, c in enumerate(row) if i not in (NON_HASHABLE_COLS + USELESS_COLS)))
    row[COL_HASH] = md5(hashable_string.encode('utf-8')).hexdigest()

    return row


def deduplicate(data):
    """Use hash to try and find complete matches in the dataset"""
    print('Deduplicating data...')
    # Probably not the most efficient way but works fast enough for a small (few thousands) dataset
    prev_hash = ''
    deduped_data = []
    data.sort(key=lambda x: x[COL_HASH])
    for row in data:
        if row[COL_HASH] != prev_hash:
            prev_hash = row[COL_HASH]
            deduped_data.append(row)
    print('Rows after deduplication: {}'.format(len(deduped_data)))

    return deduped_data


def merge_rows(rows):
    pass


if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.exit('Usage: {} source-filename tasks-filename user-tasks-filename'.format(sys.argv[0]))
    source_filename = sys.argv[1]
    if not os.path.exists(source_filename):
        sys.exit('File "{}" does not exist'.format(source_filename))
    tasks_filename = sys.argv[2]
    if not os.path.exists(tasks_filename):
        sys.exit('File "{}" does not exist'.format(tasks_filename))
    user_tasks_filename = sys.argv[3]
    if not os.path.exists(user_tasks_filename):
        sys.exit('File "{}" does not exist'.format(user_tasks_filename))

    process_source(source_filename, tasks_filename, user_tasks_filename)
