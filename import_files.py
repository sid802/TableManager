__author__ = 'Sid'

######################
#
# Import csv/xls
# files to a DB
#
######################

import os
import file_iterators


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

    excel_rows_gen = file_iterators.excel_iterator(file_src_path)
    row_gen_to_db(excel_rows_gen, table_dst, has_headers=has_headers)


def csv_to_db(file_src_path, table_dst, has_headers=True, db_dst='mysql', schema='gen_infos'):
    """
    :param file_src_path: Path to file
    :param db_dst: database kind (Mysql/Oracle/Sqlite etc)
    :param schema: schema if exists
    :return: True/False based on success
    """

    csv_rows_gen = file_iterators.csv_iterator(file_src_path)
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

    sql_type_field_string = ',\n'.join(map(lambda x: "{0} VARCHAR(1000)".format(x), headers))

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
            db_conn.commit()
        except connector.errors.DataError, e:
            # Find faultive row
            db_conn.rollback()  # Cancel rows that were just inserted before faultive
            for row in rows_bulk:
                try:
                    cursor.execute(INSERT_ROW_QUERY, row)
                except Exception, e:
                    print 'Error in row: {0}'.format(row)
            db_conn.commit()

    db_conn.commit()

if __name__ == '__main__':
    file_src = raw_input('Enter file you want to import: ')
    table_dst = raw_input("Enter Destination table: ")
    _, ext = os.path.splitext(file_src)
    file_to_db(file_src, table_dst, file_src_ext=ext[1:])
