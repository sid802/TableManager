#-*- encoding: utf-8 -*-


######################
#
#		Save   tables
#		 into file
#
#
#######################

from xlsxwriter.workbook import Workbook

def encode_string(uni_string):
	if isinstance(uni_string, unicode):
		return uni_string.encode('utf-8')
	return uni_string

def decode_string(string):
	if isinstance(string, str):
		try:
			return string.decode('utf-8')
		except UnicodeDecodeError:
			return string.decode('cp1255')
	return string


def save_as_file(dst_path_no_ext, tables, format='xlsx', one_sheet=True):
	if format.startswith('xls'):
		success = save_as_excel(dst_path_no_ext, tables, format=format, one_sheet=one_sheet)

	return success

def save_as_excel(dst_path_no_ext, tables, format='xlsx', one_sheet=True):
	""" exports tables into excel files. return file creation success """
	if one_sheet:
		dst_func = save_excel_one_sheet
	else:
		dst_func = save_excel_multiple_sheets

	success = dst_func(dst_path_no_ext, tables, format=format)
	return success

def save_excel_one_sheet(dst_path_no_ext, tables, format='xlsx'):
	try:
		final_dst_path = u'{full_path}.{ext}'.format(full_path=dst_path_no_ext, ext=format)
		workbook = Workbook(final_dst_path)
		sheet = workbook.add_worksheet()
		current_row = 0
		for table in tables:
			current_row = export_table_to_sheet(sheet, table, current_row)
		print final_dst_path
		workbook.close()
		
		return True
	except Exception as e:
		print e
		return False

def save_excel_multiple_sheets(dst_path_no_ext, tables, format='xlsx'):
	try:
		workbook = Workbook('{full_path}.{ext}'.format(full_path=dst_path_no_ext, ext=format))
		for table in tables:
			sheet=workbook.add_worksheet()
			export_table_to_sheet(sheet, table)
		workbook.close()
		return True
	except Exception as e:
		print e
		return False

def export_table_to_sheet(sheet, table, current_row = 0):
	""" write table to sheet and return the next empty row"""
	for row in table.rows:
		for cell in row.cells:
			decoded_value = decode_string(cell.value)
			#print current_row + row.index
			#print u"WRITING", decoded_value, u"IN ROW", current_row + row.index, u"CUR_ROW:", current_row, u"ROW INDEX:", row.index, u"IN COLUMN", cell.index
			sheet.write(current_row + row.index, cell.index, decoded_value)

	return current_row + row.index + 1