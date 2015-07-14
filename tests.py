#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      mwoods
#
# Created:     18/06/2015
# Copyright:   (c) mwoods 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import unittest
import sqlite3
import engine


class DBUnitTest(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.text = engine.TextFile("./tests/USArrests.csv")
        self.sql_eng = engine.SQLDB(self.conn, auto_commit=True, run_interactive=False)
        self.TABLE_NAME = 'us_arrests'

        if self.sql_eng.tables_exists(self.TABLE_NAME):
            self.sql_eng.drop_table(self.TABLE_NAME)

        col_names = ['State', 'Murder', 'Assault', 'UrbanPop', 'Rape']
        col_types = ['text', 'float', 'integer', 'integer', 'float']
        self.sql_eng.create_table('us_arrests', col_names, col_types)


    def test_table_created(self):
        self.assertTrue(self.sql_eng.tables_exists(self.TABLE_NAME), 'table exists')

    def test_insert_and_yield_rows(self):
        rows = self.text.get_rows()
        data = [row for row in rows]

        self.sql_eng.insert_rows(self.TABLE_NAME, rows = data)
        table_rows = [r for r in self.sql_eng.yield_rows(self.TABLE_NAME)]

        self.assertEqual(len(table_rows), len(data), 'test insert rows')

    def test_import_file(self):

        self.sql_eng.import_file(self.TABLE_NAME, self.text.get_rows, batch_size=10)
        data = [row for row in self.text.get_rows()]
        table_rows = [r for r in self.sql_eng.yield_rows(self.TABLE_NAME)]
        print len(table_rows)
        self.assertEqual(len(table_rows), len(data), 'test insert rows')

    def tearDown(self):
        self.sql_eng.drop_table(self.TABLE_NAME)
        self.sql_eng.close_conn()

if __name__ == "__main__":
    unittest.main()

