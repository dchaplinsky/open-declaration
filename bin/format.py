import sys
import os
import csv

import xlsxwriter

from itertools import groupby
from datetime import datetime


COL_FILENAME = 1  # "Filename" column number
COL_LINK = 312  # Link to the original document
NUM_SHEETS = 10  # Default number of worksheets to spread the groups on
HIGHLIGHT_COLS = (2, 3) + tuple(range(5, 311))


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


def group_by_link(data):
    """Group the records by a link value and return as a dict with link being a key"""
    print('Grouping the data...')
    grouped_data = {}
    sorted_data = sorted(data, key=lambda x: x[COL_LINK])
    for key, group in groupby(sorted_data, key=lambda x: x[COL_LINK]):
        row_data = list(group)
        # Filter groups that have only one record
        if len(row_data) > 1:
            grouped_data[key] = row_data

    return grouped_data


def write_result(header, grouped_data, num_sheets):
    """Format and write data to an XLSX with pagination"""
    filename = 'formatted_{:%Y-%m-%d_%H:%M:%S}.xlsx'.format(datetime.now())
    print('Writing to XLSX workbook "{}"'.format(filename))
    workbook = xlsxwriter.Workbook(filename)

    grouped_data_items = list(grouped_data.items())
    groups_per_sheet = len(grouped_data_items) // num_sheets
    for sheet_num in range(num_sheets):
        worksheet = workbook.add_worksheet('Book{}'.format(sheet_num + 1))
        # Pagination bounds
        lower_bound = groups_per_sheet * sheet_num
        if sheet_num == num_sheets - 1:
            upper_bound = None
        else:
            upper_bound = groups_per_sheet * (sheet_num + 1)
        sheet_groups = grouped_data_items[lower_bound:upper_bound]

        row_pointer = 0  # Current row in the worksheet
        worksheet.write_row(row_pointer, 0, header)
        for key, rows in sheet_groups:
            for row_num, row in enumerate(rows):
                row_pointer += 1
                for col, cell in enumerate(row):
                    format = workbook.add_format()
                    # Highlight columns that contain non-matching cells
                    if col in HIGHLIGHT_COLS:
                        if any([x[col] != cell for i, x in enumerate(rows) if i != row_num]):
                            format.set_bg_color('red')
                    # Handle the group borders
                    format.set_border_color('gray')
                    if row_num == 0:
                        format.set_top(1)
                    elif row_num == len(rows) - 1:
                        format.set_bottom(1)

                    # Filenames might sometimes be detected as numbers and we don't want this
                    if col == COL_FILENAME:
                        worksheet.write_string(row_pointer, col, cell, format)
                    else:
                        worksheet.write(row_pointer, col, cell, format)
            # Add an empty row between the groups
            row_pointer += 1
            worksheet.write_row(row_pointer, 0, [])
        print('Wrote {} rows for sheet {}'.format(row_pointer + 1, sheet_num))

    workbook.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: {} source-filename <num-sheets>'.format(sys.argv[0]))
    source_filename = sys.argv[1]
    if not os.path.exists(source_filename):
        sys.exit('File "{}" does not exist'.format(source_filename))
    num_sheets = NUM_SHEETS
    if len(sys.argv) == 3:
        num_sheets = int(sys.argv[2])

    header, data = load_source(source_filename)
    grouped_data = group_by_link(data)
    write_result(header, grouped_data, num_sheets)