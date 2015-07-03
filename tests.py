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
import cda


class ExcelUploadTest(unittest.TestCase):

    def setUp(self):
        db = sqlite3.connect(":memory:")
        db.execute("""create table person
                        (firstname text,
                         lastname text,
                         birthdate text,
                         score real)
                    """)

        MSSQLDB

    def tearDown(self):
        db.close()






