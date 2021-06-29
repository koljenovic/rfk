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

class Field:
    def __init__(self, name, ftype, length, decimals, padded=None, pad_char=None):
        self.name = name
        self.ftype = ftype
        self.length = length
        self.decimals = decimals
        self.padded = padded
        self.pad_char = pad_char

    def __repr__(self):
        return '<' + ', '.join([self.name, chr(self.ftype), str(self.length), str(self.decimals), str(self.padded), str(self.pad_char)]) + '>'

    CHAR = dbf.FieldType.CHAR
    CURRENCY = dbf.FieldType.CURRENCY
    DATE = dbf.FieldType.DATE
    DATETIME = dbf.FieldType.DATETIME
    DOUBLE = dbf.FieldType.DOUBLE
    FLOAT = dbf.FieldType.FLOAT
    GENERAL = dbf.FieldType.GENERAL
    INTEGER = dbf.FieldType.INTEGER
    LOGICAL = dbf.FieldType.LOGICAL
    MEMO = dbf.FieldType.MEMO
    NUMERIC = dbf.FieldType.NUMERIC
    PICTURE = dbf.FieldType.PICTURE
    TIMESTAMP = dbf.FieldType.TIMESTAMP

# @TODO: Full type conversion should be seamlesly integrated into this adapter
class RFKAdapter:
    def __init__(self, db_path, table_name, mode='-'):
        self.db_path = db_path
        self.table_name = table_name
        self._table = dbf.Table(db_path + table_name, codepage='cp852', dbf_type='db3')
        self._table.open(mode=dbf.READ_WRITE if mode.lower() == 'w' else dbf.READ_ONLY)
        self._parse_headers()

    def __del__(self):
        if hasattr(self, '_table') and self._table.status != dbf.CLOSED:
            self._table.close()

    @staticmethod
    def _prepare_value(value):
        """prepares read values """
        e = value.decode('cp852') if isinstance(value, bytes) else value
        return e.strip() if isinstance(e, string_types) else e

    @staticmethod
    def _char_to_int(value):
        """determines if the character list encodes an integer value"""
        try:
            return int(value)
        except:
            return None

    @staticmethod
    def _is_char_padded_int(value, length):
        """determines if a char encoded integer is padded

        only positive ints tested by design
        returns both the status and padding char if present
        """
        int_value = RFKAdapter._char_to_int(value)
        if len(value) == length and int_value != None:
            try:
                return str(int_value).rjust(length, value[0]) == value, value[0]
            except:
                return False, None
        return False, None

    def _is_column_padded_int(self, column):
        return None

    def _parse_headers(self):
        """parses the fields from table headers"""
        # @TODO-20: determine padding for each field
        fields = self._table._meta.fields
        self.header_fields = [Field(field, *self._table.field_info(field)[:3]) for field in fields]
            

    def read(self, where=[], raw_flag=False):
        """read, fetch and filter from RFK table

        filter example: [('OBJ_ULI', lambda x: x == '010')]
        """
        # @TODO: should filters support mismatched type values autoconversion (e.g. string/int, date/string date)?
        result = []
        fields = self._table._meta.fields
        for field, constr in where:
            if field not in fields:
                raise FieldError('No field with name %s in table %s', (field, self.table_name))
                return
        for record in self._table:
            satisfies = True
            for field, constr in where:
                if not constr(RFKAdapter._prepare_value(record[field])):
                    satisfies = False
                    break
            if not satisfies:
                continue
            else:
                result.append({field: RFKAdapter._prepare_value(record[field]) if not raw_flag else record[field] for field in fields})
        return result

    def write(self, data):
        """appends a new record to the table"""
        # @TODO: treba paddovati C intove?
        # @TODO: konvertuj datume iz ISO8601
        # @TODO: da li je dobar encoding upisa?
        # @TODO: treba padovati C ne-intove? npr spaceom?
        self._table.append(data)

    def update(self, table):
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
        self._adapter = RFKAdapter(self._db_path, table_name, mode)

    def test_000_read_constructor(self):
        """test opening the table for reading"""
        self._adapter = RFKAdapter(self._db_path, 'ULIZ.DBF')
        self.assertEqual(self._adapter._table.status, dbf.READ_ONLY)

    def test_001_write_constructor(self):
        """test opening the table for reading"""
        self._adapter = RFKAdapter(self._db_path, 'ULIZ.DBF', 'W')
        self.assertEqual(self._adapter._table.status, dbf.READ_WRITE)

    def test_prepare_read_value(self):
        """tests encoding and value conversion for values read from table"""
        # OBJ_ROB,C,3   KLA_ROB,C,2 SIF_ROB,C,4
        # 010 01  0255    Pletenina PIQUE-ČEŠLJANA P/C
        self._set_up()
        value = 'Pletenina PIQUE-ČEŠLJANA P/C'
        raw = value.encode('cp852')
        self.assertEqual(self._adapter._prepare_value(raw), value)
        self.assertEqual(self._adapter._prepare_value('   Škafiškafnjak '), 'Škafiškafnjak')
        self.assertEqual(self._adapter._prepare_value(1234), 1234)

    def test_single_filtered_read(self):
        """test reading a single record filtered by a single condition"""
        self._set_up()
        result = self._adapter.read([('OTP_ULI', lambda x: x == '880')])
        self.assertEqual(result[0]['OTP_ULI'], '880')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')

    def test_single_filtered_raw_read(self):
        """test reading a single raw record filtered by a single condition"""
        self._set_up()
        result = self._adapter.read([('OTP_ULI', lambda x: x == '880')], raw_flag=True)
        self.assertEqual(result[0]['OTP_ULI'], '880                 ')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')
        self.assertEqual(result[0]['KUF_ULI'], '       880')

    def test_multi_filtered_read(self):
        """test reading multiple record filtered by multiple conditions"""
        self._set_up()
        result = self._adapter.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertGreater(len(result), 0)
        for record in result:
            self.assertEqual(record['OBJ_ULI'], '010')
            self.assertEqual(record['DOK_ULI'], '20')

    def test_nonexistent_filter_key(self):
        """test raising a FieldError on nonexistent field filter key"""
        self._set_up()
        self.assertRaises(FieldError, self._adapter.read, [('SOK_ULI', lambda x: True)])

    def test_return_all(self):
        """return all the records with empty filter list"""
        self._set_up()
        self.assertGreater(len(self._adapter.read()), 0)

    def test_record_append(self):
        """test appending a single record to the table"""
        self._set_up('ULIZ.DBF', 'W')
        sample_record = {'OBJ_ULI': '010', 'DOK_ULI': '20', 'SIF_ULI': '00000', 'GOT_ULI': None, 'NAL_ULI': 'ADM', 'DAT_ULI': dbf.DateTime(2021, 6, 14), 'OTP_ULI': '225883', 'NAO_ULI': None, 'DAI_ULI': dbf.DateTime(2021, 6, 14), 'MIS_ULI': None, 'VAL_ULI': dbf.DateTime(2021, 6, 14), 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': None, 'TRG_ULI': None, 'KAS_ULI': 0, 'PUT_ULI': '001', 'NAP_ULI': '', 'LIK_ULI': None, 'FIN_ULI': None, 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': None, 'L2A_ULI': None, 'L3A_ULI': None, 'L4A_ULI': None, 'L5A_ULI': None, 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': None, 'REK_ULI': None, 'STO_ULI': None, 'FRA_ULI': None, 'FRR_ULI': None, 'MJE_ULI': None, 'PAS_ULI': None, 'DAS_ULI': None, 'MTR_ULI': None}
        result = self._adapter.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
        sample_record['SIF_ULI'] = fresh_row_id
        self._adapter.write(sample_record)
        result = self._adapter.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertEqual(result[-1]['SIF_ULI'], fresh_row_id)

    def test_determine_char_field_is_int(self):
        """tests determining if the char field is an integer value"""
        self.assertEqual(RFKAdapter._char_to_int('225'), 225)
        self.assertEqual(RFKAdapter._char_to_int('00225'), 225)
        self.assertRaises(Exception, RFKAdapter._char_to_int(None))
        self.assertEqual(RFKAdapter._char_to_int('Škafiškafnjak'), None)

    def test_determine_char_int_field_is_padded(self):
        """tests determining if an integer char field is padded"""
        self.assertEqual(RFKAdapter._is_char_padded_int('225', 5), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_int('0225', 5), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_int('00225', 5), (True, '0'))
        self.assertEqual(RFKAdapter._is_char_padded_int(' 225', 5), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_int('  225', 5), (True, ' '))
        self.assertEqual(RFKAdapter._is_char_padded_int(' 883', 4), (True, ' '))

    def test_field_string_rep(self):
        """test whether field object gets represented correctly"""
        self._set_up()
        self._adapter._parse_headers()
        self.assertEqual(str(self._adapter.header_fields[0]), '<OBJ_ULI, C, 3, 0, None, None>')

    def test_header_parsing(self):
        """tests if the headers get parsed right"""
        self._set_up()
        outcome = ['<OBJ_ULI, C, 3, 0, None, None>', '<DOK_ULI, C, 2, 0, None, None>', '<SIF_ULI, C, 5, 0, None, None>', '<GOT_ULI, C, 1, 0, None, None>', '<NAL_ULI, C, 3, 0, None, None>', '<DAT_ULI, D, 8, 0, None, None>', '<OTP_ULI, C, 20, 0, None, None>', '<NAO_ULI, C, 50, 0, None, None>', '<DAI_ULI, D, 8, 0, None, None>', '<MIS_ULI, C, 50, 0, None, None>', '<VAL_ULI, D, 8, 0, None, None>', '<DAN_ULI, N, 3, 0, None, None>', '<RBR_ULI, N, 4, 0, None, None>', '<KUF_ULI, C, 10, 0, None, None>', '<ZAD_ULI, C, 3, 0, None, None>', '<PAR_ULI, C, 7, 0, None, None>', '<PRO_ULI, C, 3, 0, None, None>', '<TRG_ULI, C, 3, 0, None, None>', '<KAS_ULI, N, 5, 2, None, None>', '<PUT_ULI, C, 3, 0, None, None>', '<NAP_ULI, M, 10, 0, None, None>', '<LIK_ULI, L, 1, 0, None, None>', '<FIN_ULI, L, 1, 0, None, None>', '<L0_ULI, L, 1, 0, None, None>', '<L1_ULI, L, 1, 0, None, None>', '<L2_ULI, L, 1, 0, None, None>', '<L3_ULI, L, 1, 0, None, None>', '<L4_ULI, L, 1, 0, None, None>', '<L5_ULI, L, 1, 0, None, None>', '<L6_ULI, L, 1, 0, None, None>', '<L7_ULI, L, 1, 0, None, None>', '<L8_ULI, L, 1, 0, None, None>', '<L9_ULI, L, 1, 0, None, None>', '<L1A_ULI, L, 1, 0, None, None>', '<L2A_ULI, L, 1, 0, None, None>', '<L3A_ULI, L, 1, 0, None, None>', '<L4A_ULI, L, 1, 0, None, None>', '<L5A_ULI, L, 1, 0, None, None>', '<N1_ULI, N, 1, 0, None, None>', '<N2_ULI, N, 1, 0, None, None>', '<FIS_ULI, L, 1, 0, None, None>', '<REK_ULI, L, 1, 0, None, None>', '<STO_ULI, L, 1, 0, None, None>', '<FRA_ULI, C, 6, 0, None, None>', '<FRR_ULI, C, 6, 0, None, None>', '<MJE_ULI, C, 2, 0, None, None>', '<PAS_ULI, C, 10, 0, None, None>', '<DAS_ULI, D, 8, 0, None, None>', '<MTR_ULI, C, 7, 0, None, None>']
        self.assertEqual(len(self._adapter.header_fields), len(outcome))
        for i in range(len(self._adapter.header_fields)):
            self.assertEqual(str(self._adapter.header_fields[i]), outcome[i])

    # --- @TODO ---
    def test_determine_char_column_is_int(self):
        """tests determining if the char column is an integer value"""
        self._set_up()
        # OBJ_ULI,C,3 - True
        # DOK_ULI,C,2 - True
        # SIF_ULI,C,5 - True
        # DAT_ULI,D - False
        # KAS_ULI,N,5,2 - False
        # MIS_ULI,C,50 - False (no values whatsoever)
        # FRA_ULI,C,6 - True (some values empty)
        self.assertEqual(self._adapter._is_column_padded_int('OBJ_ULI'), True)

    def test_write_inserting_codepage_strings(self):
        """tests whether codepage specific chars get appended correctly"""
        self.assertEqual(False, True)

    def test_update_inserting_codepage_srtings(self):
        """test whether codepage specific chars get updated correctly"""
        self.assertEqual(False, True)

    def test_appending_mixed_types_record(self):
        """test appending a mismatched types record to the table"""
        self._set_up('ULIZ.DBF', 'W')
        sample_record = {'OBJ_ULI': 10, 'DOK_ULI': 20, 'SIF_ULI': 0, 'GOT_ULI': None, 'NAL_ULI': 'ADM', 'DAT_ULI': dbf.DateTime(2021, 6, 14), 'OTP_ULI': '225883', 'NAO_ULI': None, 'DAI_ULI': '2021-06-14', 'MIS_ULI': None, 'VAL_ULI': dbf.DateTime(2021, 6, 14), 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': None, 'TRG_ULI': None, 'KAS_ULI': 0, 'PUT_ULI': '001', 'NAP_ULI': '', 'LIK_ULI': None, 'FIN_ULI': None, 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': None, 'L2A_ULI': None, 'L3A_ULI': None, 'L4A_ULI': None, 'L5A_ULI': None, 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': None, 'REK_ULI': None, 'STO_ULI': None, 'FRA_ULI': None, 'FRR_ULI': None, 'MJE_ULI': None, 'PAS_ULI': None, 'DAS_ULI': None, 'MTR_ULI': None}
        result = self._adapter.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
        sample_record['SIF_ULI'] = fresh_row_id
        self._adapter.write(sample_record)
        result = self._adapter.read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertEqual(result[-1]['SIF_ULI'], fresh_row_id)

    def test_determine_char_column_is_padded(self):
        """tests determining if an integer char column is padded"""
        self._set_up()
        self.assertEqual(False, True)

    def test_parsing_the_headers(self):
        """tests whether the headers parse right"""
        self._set_up()
        result = self._adapter._parse_headers()
        self.assertEqual(False, True)

    def test_updating_single_record(self):
        """tests updating a single existing record"""
        self.assertEqual(False, True)

    def test_updating_multiple_records(self):
        """tests updating multiple records by a certain criteria"""
        self.assertEqual(False, True)

if __name__ == '__main__':
    unittest.main(failfast=False)