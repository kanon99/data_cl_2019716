#!/usr/bin/python3
#导入得到的list，每个excel的表名称和当前的第N个sheet,组合成为数据库全名
import xlrd
from ffinally import *
from functools import lru_cache

@lru_cache(maxsize=600)
def get_excelNsheets(Now_N_sheets,j):

    workbook_origin = xlrd.open_workbook('C:\\Users\\Administrator\\PycharmProjects\\excel_all_data\\excelalldata.xlsx')
    sheet0 = workbook_origin.sheets()[0]
    first_row_values = sheet0.row_values(j)
    workbook = xlrd.open_workbook(first_row_values[0])
    create_names_excel = first_row_values[0][9:-5]
    Nsheets = len(workbook.sheet_names())
    sheet = workbook.sheets()[Now_N_sheets]
    nrows_values = sheet.nrows

    #返回sheet对象 和 当前工作簿的名称 , 行数 以及 sheets数量
    return  sheet , create_names_excel , nrows_values , Nsheets
