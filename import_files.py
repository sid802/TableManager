__author__ = 'Sid'

######################
#
# Import csv/xls
# files to a DB
#
######################

import os


class ImportException(Exception):
    def __init__(self, msg):
        super(ImportException, self).__init__(msg)


def file_to_db(file_src_path, table_dst, file_src_ext='xlsx', has_headers=True, db_dst='mysql', schema='gen_infos'):
    """
    :param file_src_path: Path to file we want to import to DB
    :param file_src_ext: File kind (xls/xlsx/csv)
    :param has_headers: Boolean telling if the file contains headers
    :param db_dst: Kind of DB we want to import to
    :param schema: Schema in DB if DB allows it
    :return:
    """
    if 'xls' in file_src_ext:
        excel_to_db(file_src_path, table_dst, has_headers, db_dst, schema)
    elif 'csv' in file_src_ext:
        csv_to_db(file_src_path, table_dst, has_headers, db_dst, schema)
    else:
        raise ImportException("Source file is of unknown format")


def excel_to_db(file_src_path, table_dst, has_headers=True, db_dst='mysql', schema='gen_infos'):
    """
    :param file_src_path: Path to file
    :param db_dst: database kind (Mysql/Oracle/Sqlite etc)
    :param schema: schema if exists
    :return: True/False based on success
    """

    excel_rows_gen = excel_iterator(file_src_path)
    row_gen_to_db(excel_rows_gen, table_dst, has_headers=has_headers)


def csv_to_db(file_src_path, table_dst, has_headers=True, db_dst='mysql', schema='gen_infos'):
    """
    :param file_src_path: Path to file
    :param db_dst: database kind (Mysql/Oracle/Sqlite etc)
    :param schema: schema if exists
    :return: True/False based on success
    """

    csv_rows_gen = csv_iterator(file_src_path)
    row_gen_to_db(csv_rows_gen, table_dst, has_headers=True)


def row_gen_to_db(rows_gen, table_dst, has_headers=True, custom_headers=None, db_dst='mysql', schema='gen_infos'):
    """
    :param rows_gen: Generator to rows we want to import (generator yielding data row)
    :param db_dst: database kind (Mysql/Oracle/Sqlite etc)
    :param schema: schema if db allows
    :return:
    """

    stripped_db = db_dst.lower().strip()
    if has_headers:
        headers = rows_gen.next()  # Will yield headers only if generator has has_headers=True
        if custom_headers:
            headers = custom_headers

    if stripped_db == 'mysql':
        row_gen_to_mysql(rows_gen, headers, table_dst, schema)
    elif stripped_db == 'sqlite':
        pass
    else:
        raise ImportException("Destination DB is unknown")


def row_gen_to_mysql(rows_gen, headers, table_dst, schema='gen_infos'):
    """
    Imports All Rows to table_dst in schema

    :param rows_gen: Data Row Generator
    :param schema: Schema to import to

    """

    from mysql import connector

    sql_type_field_string = ',\n'.join(map(lambda x: "{0} VARCHAR(255)".format(x), headers))

    TABLE_CREATION_QUERY = """
                           CREATE TABLE IF NOT EXISTS {schema}.{table}
                           (
                           {fields}
                           )
                        """.format(schema=schema,
                                   table=table_dst,
                                   fields=sql_type_field_string)

    INSERT_ROW_QUERY = """
                        INSERT INTO {schema}.{table}({header_names})
                        VALUES ({params})
                        """.format(schema=schema,
                                   table=table_dst,
                                   header_names=','.join(headers),
                                   params=','.join(['%s' for header in headers])
                                   )

    db_conn = connector.connect(user='root',
                                password='hujiko',
                                host='127.0.0.1',
                                database=schema)

    cursor = db_conn.cursor()

    cursor.execute(TABLE_CREATION_QUERY)  # Create table if it doesn't exist

    for rows_bulk in rows_gen:
        try:
            cursor.executemany(INSERT_ROW_QUERY, rows_bulk)
        except Exception, e:
            print "Could Not Insert bulk starting with:\n{0}".format(rows_bulk[0])
            print e

    db_conn.commit()

# region row_generators

def excel_iterator(excel_path, has_headers=True, bulk_amount=300):
    """
    :param sheet: Sheet we want to iterate
    :param bulk_amount: Amount of rows we want to yield
    :return: generator yielding headers first and then bulk of datarows
    """
    import xlrd

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
    import unicodecsv

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

# endregion

if __name__ == '__main__':
    file_src = raw_input('Enter file you want to import: ')
    table_dst = raw_input("Enter Destination table: ")
    _, ext = os.path.splitext(file_src)
    file_to_db(file_src, table_dst, file_src_ext=ext[1:])
