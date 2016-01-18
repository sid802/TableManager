__author__ = 'Sid'

######################
#
# Import csv/xls
# files to a DB
#
######################

import os
import file_iterators

TRGT_MYSQL = 1
TRGT_SQLITE = 2

class ImportException(Exception):
    def __init__(self, msg):
        super(ImportException, self).__init__(msg)


def file_to_db(file_src_path, table_dst, file_src_ext='xlsx', has_headers=True, db_dst=TRGT_MYSQL, schema='gen_infos'):
    """
    :param file_src_path: Path to file we want to import to DB
    :param file_src_ext: File kind (xls/xlsx/csv)
    :param has_headers: Boolean telling if the file contains headers
    :param db_dst: Kind of DB we want to import to
    :param schema: Schema in DB if DB allows it
    :return:
    """
    if 'xls' in file_src_ext:
        rows_gen = file_iterators.excel_iterator(file_src_path)
    elif 'csv' in file_src_ext:
        rows_gen = file_iterators.csv_iterator(file_src_path)
    else:
        raise ImportException("Source file is of unknown format")

    row_gen_to_db(rows_gen, table_dst, has_headers=has_headers, db_dst=db_dst, schema=schema)


def row_gen_to_db(rows_gen, table_dst, has_headers=True, custom_headers=None, db_dst='mysql', schema='gen_infos'):
    """
    :param rows_gen: Generator to rows we want to import (generator yielding data row)
    :param db_dst: database kind (Mysql/Oracle/Sqlite etc)
    :param schema: schema if db allows
    :return:
    """

    if has_headers:
        headers = rows_gen.next()  # Will yield headers only if generator has has_headers=True
        if custom_headers:
            headers = custom_headers

    if db_dst == TRGT_MYSQL:
        row_gen_to_mysql(rows_gen, headers, table_dst, schema)
    elif db_dst == TRGT_SQLITE:
        row_gen_to_sqlite(rows_gen, headers, table_dst)
    else:
        raise ImportException("Destination DB is unknown")

def row_gen_to_sqlite(rows_gen, headers, table_dst, db_path=None):
    """
    :param rows_gen: Data Row Generator
    :param headers: Table Headers
    :param table_dst: Table destination name

    Imports All Rows to table_dst in schema
    """

    import sqlite3

    if db_path is None:
        db_path = raw_input("Enter Path to SQLite3 DB: ")
        while not os.path.isfile(db_path):
            db_path = raw_input("Enter Path to SQLite3 DB: ")

    sql_type_field_string = ',\n'.join(map(lambda x: "{0} VARCHAR(1000)".format(x), headers))
    TABLE_CREATION_QUERY = """
                           CREATE TABLE IF NOT EXISTS {table}
                           (
                           {fields}
                           )
                        """.format(table=table_dst,
                                   fields=sql_type_field_string)
    INSERT_ROW_QUERY = """
                        INSERT INTO {table}({header_names})
                        VALUES ({params})
                        """.format(table=table_dst,
                                   header_names=','.join(headers),
                                   params=','.join(['?' for header in headers])
                                   )

    db_conn = sqlite3.connect(db_path)

    cursor = db_conn.cursor()

    cursor.execute(TABLE_CREATION_QUERY)  # Create table if it doesn't exist

    for rows_bulk in rows_gen:
        try:
            cursor.executemany(INSERT_ROW_QUERY, rows_bulk)
            db_conn.commit()
        except Exception, e:
            # Find faultive row
            db_conn.rollback()  # Cancel rows that were just inserted before faultive
            for row in rows_bulk:
                try:
                    cursor.execute(INSERT_ROW_QUERY, row)
                except Exception, e:
                    print 'Error in row: {0}'.format(row)
            db_conn.commit()

    db_conn.commit()



def row_gen_to_mysql(rows_gen, headers, table_dst, schema='gen_infos'):
    """
    :param rows_gen: Data Row Generator
    :param headers: table headers
    :param table_dst: Table destination name
    :param schema: Schema to import to

    Imports All Rows to table_dst in schema
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
    db_dst = raw_input('Enter 1 for mysql, 2 for SQLite:\n')
    while not (db_dst.isdigit() and int(db_dst) in [1, 2]):
        db_dst = raw_input('Enter 1 for mysql, 2 for SQLite:\n')
    db_dst = int(db_dst)
    file_to_db(file_src, table_dst, file_src_ext=ext[1:], db_dst=db_dst)
