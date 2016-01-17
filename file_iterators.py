__author__ = 'Sid'

import xlrd, unicodecsv

def excel_iterator(excel_path, has_headers=True, bulk_amount=300):
    """
    :param sheet: Sheet we want to iterate
    :param bulk_amount: Amount of rows we want to yield
    :return: generator yielding headers first and then bulk of datarows
    """

    wb_src = xlrd.open_workbook(excel_path)
    sheet_src = wb_src.sheet_by_index(0)

    first_data_row = 0

    if has_headers:
        yield sheet_src.row_values(0)  # Yield first only headers
        first_data_row = 1

    rows_to_yield = []

    for row_index in xrange(first_data_row, sheet_src.nrows):
        row_values = sheet_src.row_values(row_index)
        rows_to_yield.append(row_values)
        if len(rows_to_yield) == bulk_amount:
            yield rows_to_yield
            rows_to_yield = []

    yield rows_to_yield  # Final yield if bulk_amount wasn't achieved

def csv_iterator(csv_path, has_headers=True, bulk_amount=300):
    """
    :param csv_path: Path to csv file
    :param bulk_amount: Amount of rows we want to yield
    :return: generator yielding headers first and then bulk of datarows
    """
    with open(csv_path, 'rb') as csv_file:

        csv_reader = unicodecsv.reader(csv_file)
        if has_headers:
            yield csv_reader.next()  # Return headers first if it has headers

        rows_to_yield = []

        for row in csv_reader:
            rows_to_yield.append(row)

            if len(rows_to_yield) == bulk_amount:
                yield rows_to_yield
                rows_to_yield = []

        yield rows_to_yield  # Final yield if bulk_amount wasn't achieved
