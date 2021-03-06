__author__ = 'Sid'

from collections import Counter
import xlrd, unicodecsv

XLRD_TO_SQL_TYPE = {
    0: 'VARCHAR(255)',
    1: 'VARCHAR(255)',
    2: 'DOUBLE',
    3: 'DATETIME',
    4: 'TINYINT',
}

SQL_TO_PYTHON_TYPE = {
    'VARCHAR(255)': unicode,
    'NUMBER': float,
    'DOUBLE': float,
    'DATETIME': xlrd.xldate.xldate_as_datetime,
    'TINYINT': bool
}

DEFAULT_TYPE = 'VARCHAR(255)'

def reformat_values(values, sql_types, date_mode):
    """
    :param values: Original values
    :param sql_types: target sql types ordered like values
    :param date_mode: workbook's datemode
    :return: list of values cast to their corresponding type
    """

    global SQL_TO_PYTHON_TYPE

    new_values = []

    zipped = zip(values, sql_types)
    for value, target_sql_format in zipped:
        target_python_format = SQL_TO_PYTHON_TYPE[target_sql_format]

        # Try casting to target type
        try:
            if target_sql_format.upper() != 'DATETIME':
                new_value = target_python_format(value)
            else:
                new_value = target_python_format(value, date_mode)  # get a datetime tuple
        except Exception, e:
            print u"Failed casting <{0}> to {1} format\r\nerror: {2}".format(value, target_python_format, e.message)
            new_value = None

        new_values.append(new_value)

    return new_values

def excel_iterator(excel_path, field_types, date_mode, has_headers=True, bulk_amount=300):
    """
    :param sheet: Sheet we want to iterate
    :param bulk_amount: Amount of rows we want to yield
    :return: generator yielding headers first and then bulk of datarows
    """

    wb_src = xlrd.open_workbook(excel_path)
    sheet_src = wb_src.sheet_by_index(0)

    first_data_row = 0

    if has_headers:
        yield sheet_src.row_values(0)  # first yield - only headers
        first_data_row = 1

    rows_to_yield = []

    for row_index in xrange(first_data_row, sheet_src.nrows):
        row_values = sheet_src.row_values(row_index)
        reformatted_values = reformat_values(row_values, field_types, date_mode)
        rows_to_yield.append(reformatted_values)
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

def get_xlrd_xls_cols_types(file_src_path, has_headers=True):
    """
    :param file_src_path: path of xls file
    :param has_headers: boolean if the file contains headers
    :return: list of column types, ordered like columns
    """
    wb = xlrd.open_workbook(file_src_path)
    date_mode = wb.datemode
    sheet = wb.sheet_by_index(0)
    cols_types = get_xlrd_cols_types(sheet)
    return cols_types, date_mode

def get_xlrd_cols_types(sheet, has_headers=True):
    """
    :param sheet: XLRD sheet
    :param has_headers: If csv file has headers, ignore first row
    :return: list of column types, ordered like columns
    """

    cols_types = []

    for col_index in xrange(sheet.ncols):
        col_types = sheet.col_types(col_index)
        if has_headers and col_types is not None:
            col_types = col_types[1:]  # Remove column name type (usually string)
        col_type = _get_xlrd_col_type(col_types)
        cols_types.append(col_type)

    return cols_types

def _get_xlrd_col_type(col_types_lst):
    """
    :param col_lst: List of all the types
    :return: SQL field type of column
    """
    """
    XLRD cell types:
    XL_CELL_EMPTY	0	empty string u''
    XL_CELL_TEXT	1	a Unicode string
    XL_CELL_NUMBER	2	float
    XL_CELL_DATE	3	float
    XL_CELL_BOOLEAN	4	int; 1 means TRUE, 0 means FALSE
    XL_CELL_ERROR	5	int representing internal Excel codes; for a text representation, refer to the supplied dictionary error_text_from_code
    XL_CELL_BLANK	6	empty string u''. Note: this type will appear only when open_workbook(..., formatting_info=True) is used.
    """

    global XLRD_TO_SQL_TYPE, DEFAULT_TYPE

    cleaned_col_lst = [type for type in col_types_lst if type not in [5, 6]]
    if not cleaned_col_lst:
        # Empty column
        return DEFAULT_TYPE  # return VARCHAR2

    counter = Counter(cleaned_col_lst)

    if len(counter.keys()) > 1:
        return DEFAULT_TYPE

    most_common_key = Counter(col_types_lst).most_common(1)[0][0]
    return XLRD_TO_SQL_TYPE[most_common_key]

def get_csv_cols_types_main(csv_path, has_headers=True):
    """
    :param csv_path: Path to csv file
    :param has_headers: If csv file has headers, ignore first row
    :return: list of column types, ordered like columns
    """

    with open(csv_path) as csv_file:
        csv_reader = unicodecsv.reader(csv_file)
        if has_headers:
            csv_reader.next()
        cols_types = get_csv_cols_types(csv_reader)

    return cols_types


def get_csv_cols_types(csv_reader):
    """
    :param col_values_lst:
    :return: SQL field type of column
    """

    cols_types = []
    #TODO: enumerate
    pass




