"""Merges results of manual processing into a single CSV. Requires 3 CSVs: original, "movables" and positions."""
import sys
import csv

from itertools import groupby

POSITION_COLS = ('Регіон', 'Структура', 'Посада')  # Copy only these columns from the positions doc
GROUP_NUM_NAME = 'Номер групи'


def read_file(filename, encoding='utf-8'):
    with open(filename, 'r', newline='', encoding=encoding) as source:
        print('Reading the file "{}"'.format(filename))
        reader = csv.DictReader(source, delimiter=';')
        return [r for r in reader], reader.fieldnames


def write_result(data, fieldnames, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as dest:
        # "ignore" is set for extras as default "raise" has O(n^2) on fieldnames lookup and thrashes performance,
        # so it's important that all dict keys have a corresponding item in fieldnames.
        writer = csv.DictWriter(dest, fieldnames, delimiter=';', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    print('Result was written to: {}'.format(filename))


def merge_movables(original, movables):
    merged = []
    fieldnames = set()

    for i, row in enumerate(movables):
        # Movables doc is identical in ordering so don't bother with any special handling
        merged.append(original[i])
        for k, v in row.items():
            # Whatever has "ОБЩ" in it + a special one gets copied over to the merge
            if 'ОБЩ' in k or k == 'Результат сверки машин':
                merged[i][k] = v
                fieldnames.add(k)
    print('Merged movables')

    return merged, list(fieldnames)


def merge_positions(original, positions):
    # Positions doc has only one record per group so we have to match by groups and duplicate on other items in it
    orig_by_group = {k: list(v) for k, v in groupby(original, key=lambda x: x[GROUP_NUM_NAME])}
    merged = []
    used_groups = set()
    fieldnames = list(POSITION_COLS)

    for row in positions:
        group_num = row[GROUP_NUM_NAME]
        # Some groups are duplicated in the positions doc
        if group_num not in used_groups:
            orig_rows = orig_by_group[group_num]
            for orig_row in orig_rows:
                for col in POSITION_COLS:
                    orig_row[col] = row[col]
            merged.extend(orig_rows)
            used_groups.add(group_num)
    print('Merged positions')

    return merged, fieldnames


if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.exit('Usage: {} original-filename movables-filename positions-filename'.format(sys.argv[0]))

    original, original_fieldnames = read_file(sys.argv[1])
    movables, _ = read_file(sys.argv[2], encoding='cp1251')
    positions, _ = read_file(sys.argv[3])

    merged, movables_fieldnames = merge_movables(original, movables)
    merged, positions_fieldnames = merge_positions(merged, positions)
    write_result(merged, original_fieldnames + movables_fieldnames + positions_fieldnames, 'merged_declarations.csv')
