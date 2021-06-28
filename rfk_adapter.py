#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import dbf
import datetime
import unittest
from six import string_types
from unittest import TestCase

class FieldError(Exception):
    pass

class RFKAdapter:
    def __init__(self, db_path, table_name, mode='-'):
        self.db_path = db_path
        self.table_name = table_name
        self.table = dbf.Table(db_path + table_name, codepage='cp852', dbf_type='db3')
        self.table.open(mode=dbf.READ_WRITE if mode.lower() == 'w' else dbf.READ_ONLY)

    def __del__(self):
        if hasattr(self, 'table'):
            self.table.close()

    def _prepare_value(self, value):
        """ Prepares read values """
        e = value.decode('cp852') if isinstance(value, bytes) else value
        return e.strip() if isinstance(e, string_types) else e

    def read(self, where=[]):
        """ Read, fetch and filter from RFK table

        e.g. filter: [('OBJ_ULI', lambda x: x == '010')]
        """
        result = []
        fields = self.table._meta.fields
        for field, constr in where:
            if field not in fields:
                raise FieldError('No field with name %s in table %s', (field, self.table_name))
                return
        for record in self.table:
            satisfies = True
            for field, constr in where:
                if not constr(self._prepare_value(record[field])):
                    satisfies = False
                    break
            if not satisfies:
                continue
            else:
                result.append({field: self._prepare_value(record[field]) for field in fields})
        return result

    def write(self, table):
        # infer type information from field header
        # CHAR = b'C' - check constraints
        # CURRENCY = b'Y'
        # DATE = b'D' - * ISO 8601 string 'YYYY-MM-DD' -> datetime
        # DATETIME = b'T' - 2021-06-28T12:38:54Z
        # DOUBLE = b'B'
        # FLOAT = b'F'
        # GENERAL = b'G'
        # INTEGER = b'I'
        # LOGICAL = b'L'
        # MEMO = b'M'
        # NUMERIC = b'N'
        # PICTURE = b'P'
        # TIMESTAMP = b'@'
        # ValueError: unable to coerce <class 'int'>(20) to string
        baza = dbf.Table('../data/ULIZ.DBF', codepage='cp852', dbf_type='db3')
        baza.open(mode=dbf.READ_WRITE)
        baza.append(sample)
        baza.close()

    def update(self, table):
        baza = dbf.Table('../data/ULIZ.DBF', codepage='cp852', dbf_type='db3')
        baza.open(mode=dbf.READ_WRITE)
        with baza[-1]:
            baza[-1]['KUF_ULI'] = '4322'
        baza.close()

class RFKAdapterTest(TestCase):
    def __init__(self, *args, **kwds):
        super(RFKAdapterTest, self).__init__(*args, **kwds)

    def test_000_read_constructor(self):
        """test opening the table for reading"""
        table = RFKAdapter('../data/', 'ULIZ.DBF')
        self.assertEqual(table.table.status, dbf.READ_ONLY)

    def test_001_write_constructor(self):
        """test opening the table for reading"""
        table = RFKAdapter('../data/', 'ULIZ.DBF', 'W')
        self.assertEqual(table.table.status, dbf.READ_WRITE)

    def test_single_filtered_read(self):
        """test reading a single record filtered by a single condition"""
        table = RFKAdapter('../data/', 'ULIZ.DBF')
        result = table.read([('OTP_ULI', lambda x: x == '880')])
        self.assertEqual(result[0]['OTP_ULI'], '880')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')

    def test_multi_filtered_read(self):
        """test reading multiple record filtered by multiple conditions"""
        table = RFKAdapter('../data/', 'ULIZ.DBF')
        result = table.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertGreater(len(result), 0)
        for record in result:
            self.assertEqual(record['OBJ_ULI'], '010')
            self.assertEqual(record['DOK_ULI'], '20')

    def test_missing_filter_key(self):
        """test raising a FieldError on missing field filter key"""
        table = RFKAdapter('../data/', 'ULIZ.DBF')
        self.assertRaises(FieldError, table.read, [('SOK_ULI', lambda x: True)])

    def test_return_all(self):
        """return all the records with empty filter list"""
        table = RFKAdapter('../data/', 'ULIZ.DBF')
        self.assertGreater(len(table.read()), 0)

# '../data/ROBA.DBF'
# '../data/ULIZ.DBF'
# '../data/PART.DBF'
# read('../data/PART.DBF')
# write_uliz()
# update_uliz()
# write_uliz()

if __name__ == '__main__':
    unittest.main(failfast=True)