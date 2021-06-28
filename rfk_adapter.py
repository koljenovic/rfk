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

    def write(self, data):
        """appends a new record to the table"""
        self.table.append(data)

    def update(self, table):
        # @TODO write failing test case first
        baza = dbf.Table('../data/ULIZ.DBF', codepage='cp852', dbf_type='db3')
        baza.open(mode=dbf.READ_WRITE)
        with baza[-1]:
            baza[-1]['KUF_ULI'] = '4322'
        baza.close()

class RFKAdapterTest(TestCase):
    def __init__(self, *args, **kwds):
        self._db_path = '../data/'
        super(RFKAdapterTest, self).__init__(*args, **kwds)

    def _set_up(self, table_name='ULIZ.DBF', mode='r'): 
        self._table = RFKAdapter(self._db_path, table_name, mode)

    def test_000_read_constructor(self):
        """test opening the table for reading"""
        self._table = RFKAdapter(self._db_path, 'ULIZ.DBF')
        self.assertEqual(self._table.table.status, dbf.READ_ONLY)

    def test_001_write_constructor(self):
        """test opening the table for reading"""
        self._table = RFKAdapter(self._db_path, 'ULIZ.DBF', 'W')
        self.assertEqual(self._table.table.status, dbf.READ_WRITE)

    def test_single_filtered_read(self):
        """test reading a single record filtered by a single condition"""
        self._set_up()
        result = self._table.read([('OTP_ULI', lambda x: x == '880')])
        self.assertEqual(result[0]['OTP_ULI'], '880')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')

    def test_multi_filtered_read(self):
        """test reading multiple record filtered by multiple conditions"""
        self._set_up()
        result = self._table.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertGreater(len(result), 0)
        for record in result:
            self.assertEqual(record['OBJ_ULI'], '010')
            self.assertEqual(record['DOK_ULI'], '20')

    def test_missing_filter_key(self):
        """test raising a FieldError on missing field filter key"""
        self._set_up()
        self.assertRaises(FieldError, self._table.read, [('SOK_ULI', lambda x: True)])

    def test_return_all(self):
        """return all the records with empty filter list"""
        self._set_up()
        self.assertGreater(len(self._table.read()), 0)

    def test_appending_row(self):
        """test appending a single row to the table"""
        #@TODO: write more append test cases
        self._set_up('ULIZ.DBF', 'W')
        sample_row = {'OBJ_ULI': '010', 'DOK_ULI': '20', 'SIF_ULI': '00000', 'GOT_ULI': None, 'NAL_ULI': 'ADM', 'DAT_ULI': dbf.DateTime(2021, 6, 14), 'OTP_ULI': '225883', 'NAO_ULI': None, 'DAI_ULI': dbf.DateTime(2021, 6, 14), 'MIS_ULI': None, 'VAL_ULI': dbf.DateTime(2021, 6, 14), 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': None, 'TRG_ULI': None, 'KAS_ULI': 0, 'PUT_ULI': '001', 'NAP_ULI': '', 'LIK_ULI': None, 'FIN_ULI': None, 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': None, 'L2A_ULI': None, 'L3A_ULI': None, 'L4A_ULI': None, 'L5A_ULI': None, 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': None, 'REK_ULI': None, 'STO_ULI': None, 'FRA_ULI': None, 'FRR_ULI': None, 'MJE_ULI': None, 'PAS_ULI': None, 'DAS_ULI': None, 'MTR_ULI': None}
        result = self._table.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
        sample_row['SIF_ULI'] = fresh_row_id
        self._table.write(sample_row)
        result = self._table.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertEqual(result[-1]['SIF_ULI'], fresh_row_id)

# '../data/ROBA.DBF'
# '../data/ULIZ.DBF'
# '../data/PART.DBF'
# read('../data/PART.DBF')
# write_uliz()
# update_uliz()
# write_uliz()

if __name__ == '__main__':
    unittest.main(failfast=True)