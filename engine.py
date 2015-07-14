#-------------------------------------------------------------------------------
# Name:        module3
# Purpose:
#
# Author:      MWoods
#
# Created:     10/06/2015
# Copyright:   (c) MWoods 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import xlrd
import csv
import os
import datetime
from collections import defaultdict
import time

#try:
#    import pyodbc as odbc
#except ImportError:
#    import pypyodbc as odbc #Fall back to pure python implementation to handle known error with 0SX 10.9
#import ceODBC as odbc


""""
    References:
        https://www.python.org/dev/peps/pep-0249/#cursor-objects
        https://docs.python.org/2/library/sqlite3.html
        https://www.sqlite.org/cli.html
"""

def ceiling(x):
    if x - int(x) < 0.00001:
        return int(x)
    else:
        return int(x) + 1



class TextFile(object):
    
    def __init__(self, path, delimiter = ",", has_header = True, nrows = -1):
        self.path = path
        self.delimiter = delimiter
        self.nrows = nrows
        self.has_header = has_header
    
    def infer_coltype(self, col_idx):
        
        #run tests on column to estimate data type
        #if col is numeric [0-9]+(.[0-9]+)?
        #if col is date use date regex or below is_date function
        #otherwise keep it as a string
        from dateutil.parser import parse
        
        def is_date(string):
            try: 
                parse(string)
                return "is a date"
            except ValueError:
                return "not a date"

    def get_header(self):
        if not self.has_header:
            return []
        with open(self.path, 'rb') as f:
            csv_reader = csv.reader(f, delimiter=self.delimiter)
            return csv_reader.next()

            
    def get_rows(self):
        with open(self.path, 'rb') as f:
            csv_reader = csv.reader(f, delimiter=self.delimiter)
            if self.has_header:
                csv_reader.next()
            for line in csv_reader:
                yield line

        

class Spreadsheet(object):

    def __init__(self, path, sheet_num = 0, title_rows = 0):
        self.path = path
        self.sheet_num = sheet_num
        self.title_rows = title_rows
        self.col_type_mapping = {}

        print 'Loading Spreadsheet'
        self.wkbk = xlrd.open_workbook(path)

        self._directory = os.path.dirname(path)
        self._file_name, self._ext = os.path.splitext(os.path.basename(path))

    def _map_ctype_to_dtype(self, ctype):
        """handles mapping of clrd ctypes to text descriptions"""
        ctype_mapping = ['null', 'text', 'float', 'date', 'bool', 'err', 'blank']
        return ctype_mapping[ctype]

    def get_dim(self):
        """returns spreadsheet dimensions in terms of rows and columns
           automaticall subtracts title rows and header
        """
        wksht = self.wkbk.sheet_by_index(self.sheet_num)
        return (wksht.nrows - self.title_rows - 1, wksht.ncols)


    def get_col_label_row_num(self):
        return 0 + self.title_rows

    def get_col_type_mapping(self):
        if self.col_type_mapping:
            return self.col_type_mapping
        return self.scan_sheet(self.sheet_num)

    def switch_sheet_num(self, sheet_num):
        if sheet_num in range(S.wkbk.nsheets):
            self.sheet_num = sheet_num
            self.col_type_mapping = {}
        else:
            raise ValueError, 'Sheet not in Range'

    def scan_sheet(self, sheet_num, scan_depth = 250):
        print 'Starting Scan first %d of records'%scan_depth
        header_row_num = self.get_col_label_row_num()

        wksht = self.wkbk.sheet_by_index(sheet_num)

        for col in range(wksht.ncols):
            types = set(wksht.col_types(col, start_rowx = header_row_num + 1, end_rowx = scan_depth))
            col_header = wksht.cell(header_row_num, col)
            header = self.format_cell(col_header.value, col_header.ctype)
            self.col_type_mapping[col] = {'header': header, 'dtype' : map(self._map_ctype_to_dtype, types)}
        return self.col_type_mapping

    def format_cell(self, val, ctype):
        #returns all values as unicode
        if ctype in [0, 6]:
            return u''
        elif ctype == 1: #cell is typed as a string
            val = val.replace(u'\xa0', '') #remove unicode whitespace character showing in some files
            val = val.strip()
        elif ctype == 2: #cell: Float (Number)
            val = int(val) if val.is_integer() else val
            val = unicode(val)
        elif ctype == 3: #ctype 3 is for date
            date = datetime.datetime(*xlrd.xldate_as_tuple(val, self.wkbk.datemode))
            val = date.strftime("%d-%b-%Y").upper()

        #assumes val
        if type(val) == unicode:
            return val
        else:
            return val.decode("utf-8")

    def format_row(self, row):
        return [self.format_cell(cell.value, cell.ctype) for cell in row]

    def export_to_csv(self, verbose = True):
        wksht = self.wkbk.sheet_by_index(self.sheet_num)
        csv_path =  os.path.join(self._directory, ('-'.join([self._file_name, wksht.name]))+'.csv')

        with open(csv_path, 'wb')  as f:
            if verbose: print 'Writing File %s'%(f.name)
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            for rownum in xrange(wksht.nrows):
                wr.writerow(self.format_row(wksht.row(rownum)))
        return csv_path


    def get_formatted_rows(self, skip_header_row = False):
        """generator that will yield all of the rows of data(omits the header)  formatted"""
        first_row = self.title_rows
        if skip_header_row:
            first_row += 1

        wksht = self.wkbk.sheet_by_index(self.sheet_num)
        for rownum in xrange(first_row, wksht.nrows):
            row = wksht.row(rownum)
            yield self.format_row(row)

    def __repr__(self):
        output = self.path+'\n'
        output += 'Dimensions Rows: %d, Cols %d\n'%self.get_dim()
        for col,val in self.get_col_type_mapping().items():
            output += 'Col %d: %s - %s \n'%(col, val['header'], ', '.join(val['dtype']), )
        return output


class SQLDB(object):
    """implements sql operations using the DB-API 2.0 specifications
        can be sub-classed and extended to handle other DB Operations
    """

    def __init__(self, connection, auto_commit = False, run_interactive = True):
        self.conn = connection
        self.curs = self.conn.cursor()
        self.auto_commit = auto_commit
        self.run_interactive = run_interactive

    def close_conn(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def tables_exists(self, table):
        try:
            self.curs.execute("SELECT * FROM %s"%table)
            return True
        except Exception:
            return False


    def create_table(self, table, col_names, col_types, get_sql = False):
        sqlize_col_name = lambda name :  name.replace(' ', '_')
        names_and_types = ', '.join(['%s %s'%(sqlize_col_name(name), type) for name, type in zip(col_names, col_types)])
        sql_string = "CREATE TABLE %(table)s (%(names)s);"%{'table': table,
                                                             'names': names_and_types}
        if get_sql:
            return sql_string
        else:
            self.run_sql(sql_string)

    def drop_table(self, table, get_sql = False):
        sql_string = "DROP TABLE %s" % table

        if get_sql:
            return  sql_string
        else:
            self.run_sql(sql_string)


    def insert_rows(self, table, rows, get_sql=False):

        sql_string = "INSERT INTO %(table)s VALUES (%(params)s);"
        sql_string = sql_string%{'table': table,
                                 'params': ', '.join(['?' for n in range(len(rows[0]))])}

        if get_sql:
            return sql_string
        else:
            self.run_sql(sql_string, rows)

    def delete_rows(self, table, get_sql=False):
        sql_string = "DELETE FROM %s" % table

        if get_sql:
            return sql_string
        else:
            self.run_sql(sql_string)

    def run_sql(self, sql, data = None):
        try:
            if data:
                self.curs.executemany(sql, data)
            else:
                self.curs.execute(sql)
        except Exception as err:
            #TODO: Dump sql to log and error message
            self.conn.rollback()
            if not self.run_interactive:
                self.close_conn()
            raise err
        else:
            if self.auto_commit:
                self.commit()

    def yield_rows(self, table):

        sql_string = "SELECT * FROM %(table)s;"%{'table': table}
        self.run_sql(sql_string)
        i = 0
        while True:
            row = self.curs.fetchone()
            if row:
                yield row
            else:
                break

    def import_file(self, table, source_yield_rows_func, batch_size = 10000, type = 'a'):
        """imports file into table has two types 'a' and 'r', append and replace, respectively.
            takes the source files yield_rows function and uploads the records in batches of size batch_size
        """
        if type == 'r':
            raise  NotImplementedError
        assert type in ['a', 'r']

        process_next_batch = True

        row_gen = source_yield_rows_func()

        while process_next_batch:
            batch = []
            for i, row in enumerate(row_gen):
                if i < (batch_size - 1):
                    batch.append(row)
                else:
                    batch.append(row)
                    break

            if len(batch) < batch_size:
                process_next_batch = False

            if batch:
                self.insert_rows(table, rows = batch)











#TO Be deprecated
class MicrosoftSQLServerDB(object):
    """
        Class for handling the connection to the Microsoft SQL Server Database
        inputs: Server, Database, Credentials
        outputs: Objects to Handle Friendly SQL Commands

    """
    def __init__(self, driver, server, db, un = None, pw = None, trusted_conn = None):
        self.driver, self.server, self.db = driver, server, db
        self.un, self.pw, self.trusted_conn = un, pw, trusted_conn

        self._db_conn = None
        self._db_cursor = None

        if self.trusted_conn == 'yes':
            vals = (self.driver,self.server,self.trusted_conn, self.db)
            conn_string = 'driver=%s;server=%s;Trusted_Connection=%s;database=%s'%vals
            self._db_conn = odbc.connect(conn_string)
        #TODO: Need to handle case where log in is based on UN and PW

        self._db_cursor = self._db_conn.cursor()

        self._meta_data_mem = defaultdict(dict)

    def get_col_data(self, table):

        #need to be reimplemented
        #return [(row.column_name, row.type_name)  for row in self._db_cursor.columns(table)]
        pass

    def get_nrows(self, table):
        if 'nrows' in self._meta_data_mem[table]:
            return self._meta_data_mem[table]['nrows']
        else:
            nrows, = self._db_cursor.execute("SELECT COUNT(1) FROM %s"%table).fetchone()
            self._meta_data_mem[table]['nrows'] = nrows
            return nrows

    def get_ncols(self, table):
        """
        :param table: DB TAble
        :return: number of cols in table
        """
        if 'ncols' in self._meta_data_mem[table]:
            return self._meta_data_mem[table]['ncols']
        else:
            ncols = len(self.get_col_data())
            self._meta_data_mem[table]['ncols'] = ncols
            return ncols

    def upload_csv(self, csv_path, table):
        #TODO: First Run Check that ncols of table match number of cols in DB
        #TODO: Run in chunks of 10K to prevent overwhelming DB using c.executemany()
        pass

    def sql_safe_execute(self, sql, data = None, commit = False):

        print 'Starting sql_safe_execute'
        start = time.time()

        try:
            if data:
                self._db_cursor.executemany(sql, data)
            else:
                self._db_cursor.execute(sql)
        except Exception as err:
            print sql #should go to logs
            self._db_conn.rollback()
            self.close_conn()
            raise err
        else:
            if commit:
                self._db_conn.commit()

        print 'Ending sql_safe_execute', time.time() - start

    def batch_insert(self, table, rows, default_date_column_on_left = False, commit = False):
        print 'Starting Batch Insert'
        start = time.time()

        print 'Starting Formatting Batch'
        start1 = time.time()
        sql = """INSERT INTO %s VALUES """%table

        def _formatter(row):
            tmp = [cell if cell != u'' else None for cell in row]
            if default_date_column_on_left:
                tmp = [None] + tmp
            return tuple(tmp)

        formatted_batch = [_formatter(row) for row in rows]

        sql += '('+'?, '*(len(formatted_batch[0]) - 1)+'?)'
        print 'Ending formatting Batch', time.time() - start1

        self.sql_safe_execute(sql, data= formatted_batch,  commit = commit)

        print 'Committed %d rows to table'%len(rows)

        print 'Ending Batch Insert', time.time() - start

    def upload_spreadsheet(self, spread_obj, table, default_date_column_on_left = False, batch_size = 10000):
        #TODO: Need to add a function to handle padding of left columns
        #TODO: consider using "bulk insert" for large files, use CSV upload to writet to tmp file
        #then upload....

        assert isinstance(spread_obj, Spreadsheet), 'spread_obj is the wrong type of thing'

        xlrows, xlcols = spread_obj.get_dim()
        xls_rows = spread_obj.get_formatted_rows(skip_header_row = True)


        for i in range(0, ceiling(xlrows/float(batch_size))):
            batch = [xls_rows.next() for _ in range(min(xlrows- batch_size*i, batch_size))]
            self.batch_insert(table, batch, default_date_column_on_left = True, commit = True)


    def close_conn(self):
        self._db_conn.close()


if __name__ == "__main__":
    import sqlite3
    conn = sqlite3.connect(":memory:")
    S = SQLDB(conn)
    print S.create_table('persons', ['first', 'last', 'bday'], ['text', 'text', 'text'], get_sql = True)


    S.close()
