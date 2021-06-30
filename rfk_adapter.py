#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import dbf
import datetime
import unittest
from six import string_types
from unittest import TestCase
from collections import defaultdict

class FieldError(Exception):
    pass

class Field:
    """Field (column) class, both field and column mean the same thing"""
    # @TODO-26: refactor to standardize only one name (Column preferred)
    def __init__(self, name, ftype, length, decimals, is_padded=None, pad=None, ctype=None):
        self.name = name
        self.ftype = ftype # (source) DBF Field type
        self.ctype = ctype # (inferred) Column type
        self.length = length
        self.decimals = decimals
        self.is_padded = is_padded
        self.pad = pad

    def __repr__(self):
        return '<' + ', '.join([self.name, chr(self.ftype), str(self.length), str(self.decimals), str(self.is_padded), str(self.pad), chr(self.ctype) if self.ctype else 'X']) + '>'

    def is_type(self, ftype):
        return self.ftype == ftype

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

# @TODO-XX: Full type conversion should be seamlesly integrated into this adapter
# @TODO-23: Consider implementing a full blown sqlite3 cache
# @TODO-24: Determine mandatory fields
class RFKAdapter:
    def __init__(self, db_path, table_name, mode='-', mock=False):
        self.db_path = db_path
        self.table_name = table_name
        self._table = dbf.Table(db_path + table_name, codepage='cp852', dbf_type='db3')
        self._table.open(mode=dbf.READ_WRITE if mode.lower() == 'w' else dbf.READ_ONLY)
        if not mock:
            self._parse_headers()

    def __del__(self):
        if hasattr(self, '_table') and self._table.status != dbf.CLOSED:
            self._table.close()

    @staticmethod
    def _prepare_value(value):
        """Prepares read values """
        e = value.decode('cp852') if isinstance(value, bytes) else value
        return e.strip() if isinstance(e, string_types) else e

    @staticmethod
    def _char_to_int(value):
        """Returns if the character list encodes an integer value"""
        try:
            return int(value)
        except:
            return None

    @staticmethod
    def _is_char_padded_int(value, length):
        """Returns if a char encoded integer is padded

        Tested only on positive integers. Returns the paddiness and an assumed
        padding character, basically should work for left padded '0' and ' '.
        """
        int_value = RFKAdapter._char_to_int(value)
        if len(value) == length and int_value != None:
            str_value = str(int_value)
            if len(value) != len(str_value) and str_value.rjust(length, value[0]) == value:
                return True, value[0]
        return False, None

    def _is_char_column_int(self, column, skip_empty=True):
        """Returns if the wholu column is integers

        Returns if the column contains only integers. Skips empty values by
        default.
        """
        if not column.is_type(Field.CHAR):
            return False
        all_empty = True
        for record in self.read(raw_flag=True):
            int_value = RFKAdapter._char_to_int(record[column.name])
            if record[column.name].strip():
                all_empty = False
            if int_value == None and (record[column.name].strip() or not skip_empty):
                return False
        return True if not all_empty else None

    def _is_char_column_padded_int(self, column, skip_empty=True):
        """Returns if the whole column is padded integers

        Returns if the column contains padded integers, the padding character
        is determined probabilistically and is not guaranteed to be correct,
        it can be adjusted manually for each field by manipulating the
        `.header_fields`. Skips empty value by default.
        """
        if self._is_char_column_int(column, skip_empty):
            pads = defaultdict(int)
            for record in self.read(raw_flag=True):
                int_value = RFKAdapter._char_to_int(record[column.name])
                if int_value != None:
                    is_padded, pad = RFKAdapter._is_char_padded_int(record[column.name], column.length)
                    if is_padded:
                        pads[pad] += 1
                elif record[column.name].strip() or not skip_empty:
                    return False, None
            if pads:
                pads = sorted(pads.items(), key=lambda x: x[1], reverse=True)
                return True, pads[0][0]
        return False, None

    def _parse_headers(self):
        """Parses the fields from table headers"""
        fields = self._table._meta.fields
        self.header_fields = { field: Field(field, *self._table.field_info(field)[:3]) for field in fields }
        # @TODO-22: persist header fields to sqlite3 cache, update on structural changes
        for field in self.header_fields.values():
            if field.is_type(Field.CHAR):
                if self._is_char_column_int(field):
                    field.ctype = Field.INTEGER
                    field.is_padded, field.pad = self._is_char_column_padded_int(field)

    def read(self, where=[], raw_flag=False):
        """Read, fetch and filter from RFK table

        Filter example: [('OBJ_ULI', lambda x: x == '010')]
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
        """Appends a new record to the table"""
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

    def _set_up(self, table_name='ULIZ.DBF', mode='r', mock=False): 
        self._adapter = RFKAdapter(self._db_path, table_name, mode, mock=mock)

    def test_001_read_constructor(self):
        """test opening the table for reading"""
        self._adapter = RFKAdapter(self._db_path, 'ULIZ.DBF', mock=True)
        self.assertEqual(self._adapter._table.status, dbf.READ_ONLY)

    def test_002_write_constructor(self):
        """test opening the table for reading"""
        self._adapter = RFKAdapter(self._db_path, 'ULIZ.DBF', 'W', mock=True)
        self.assertEqual(self._adapter._table.status, dbf.READ_WRITE)

    def test_prepare_read_value(self):
        """tests encoding and value conversion for values read from table"""
        value = 'Pletenina PIQUE-ČEŠLJANA P/C'
        raw = value.encode('cp852')
        self.assertEqual(RFKAdapter._prepare_value(raw), value)
        self.assertEqual(RFKAdapter._prepare_value('   Škafiškafnjak '), 'Škafiškafnjak')
        self.assertEqual(RFKAdapter._prepare_value(1234), 1234)

    def test_single_filtered_read(self):
        """test reading a single record filtered by a single condition"""
        self._set_up(mock=True)
        result = self._adapter.read([('OTP_ULI', lambda x: x == '880')])
        self.assertEqual(result[0]['OTP_ULI'], '880')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')

    def test_single_filtered_raw_read(self):
        """test reading a single raw record filtered by a single condition"""
        self._set_up(mock=True)
        result = self._adapter.read([('OTP_ULI', lambda x: x == '880')], raw_flag=True)
        self.assertEqual(result[0]['OTP_ULI'], '880                 ')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')
        self.assertEqual(result[0]['KUF_ULI'], '       880')

    def test_multi_filtered_read(self):
        """test reading multiple record filtered by multiple conditions"""
        self._set_up(mock=True)
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
        self._set_up(mock=True)
        self.assertRaises(FieldError, self._adapter.read, [('SOK_ULI', lambda x: True)])

    def test_return_all(self):
        """return all the records with empty filter list"""
        self._set_up(mock=True)
        self.assertGreater(len(self._adapter.read()), 0)

    def test_record_append(self):
        """test appending a single record to the table"""
        self._set_up('ULIZ.DBF', 'W', mock=True)
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
        self.assertEqual(RFKAdapter._char_to_int('00000'), 0)

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
        self._set_up(mock=True)
        outcome = Field('OBJ_ULI', *self._adapter._table.field_info('OBJ_ULI')[:3])
        self.assertEqual(str(outcome), '<OBJ_ULI, C, 3, 0, None, None, X>')
        outcome.is_padded, outcome.pad = self._adapter._is_char_column_padded_int(outcome)
        self.assertEqual(str(outcome), '<OBJ_ULI, C, 3, 0, True, 0, X>')

    def test_header_parsed(self):
        """tests if the headers gets parsed and if it gets parsed right"""
        self._set_up()
        target = { 'OBJ_ULI': '<OBJ_ULI, C, 3, 0, True, 0, I>', 'DOK_ULI': '<DOK_ULI, C, 2, 0, False, None, I>', 'SIF_ULI': '<SIF_ULI, C, 5, 0, True, 0, I>', 'GOT_ULI': '<GOT_ULI, C, 1, 0, False, None, I>', 'NAL_ULI': '<NAL_ULI, C, 3, 0, None, None, X>', 'DAT_ULI': '<DAT_ULI, D, 8, 0, None, None, X>', 'OTP_ULI': '<OTP_ULI, C, 20, 0, None, None, X>', 'NAO_ULI': '<NAO_ULI, C, 50, 0, None, None, X>', 'DAI_ULI': '<DAI_ULI, D, 8, 0, None, None, X>', 'MIS_ULI': '<MIS_ULI, C, 50, 0, None, None, X>', 'VAL_ULI': '<VAL_ULI, D, 8, 0, None, None, X>', 'DAN_ULI': '<DAN_ULI, N, 3, 0, None, None, X>', 'RBR_ULI': '<RBR_ULI, N, 4, 0, None, None, X>', 'KUF_ULI': '<KUF_ULI, C, 10, 0, None, None, X>', 'ZAD_ULI': '<ZAD_ULI, C, 3, 0, True, 0, I>', 'PAR_ULI': '<PAR_ULI, C, 7, 0, True, 0, I>', 'PRO_ULI': '<PRO_ULI, C, 3, 0, False, None, I>', 'TRG_ULI': '<TRG_ULI, C, 3, 0, True, 0, I>', 'KAS_ULI': '<KAS_ULI, N, 5, 2, None, None, X>', 'PUT_ULI': '<PUT_ULI, C, 3, 0, True, 0, I>', 'NAP_ULI': '<NAP_ULI, M, 10, 0, None, None, X>', 'LIK_ULI': '<LIK_ULI, L, 1, 0, None, None, X>', 'FIN_ULI': '<FIN_ULI, L, 1, 0, None, None, X>', 'L0_ULI': '<L0_ULI, L, 1, 0, None, None, X>', 'L1_ULI': '<L1_ULI, L, 1, 0, None, None, X>', 'L2_ULI': '<L2_ULI, L, 1, 0, None, None, X>', 'L3_ULI': '<L3_ULI, L, 1, 0, None, None, X>', 'L4_ULI': '<L4_ULI, L, 1, 0, None, None, X>', 'L5_ULI': '<L5_ULI, L, 1, 0, None, None, X>', 'L6_ULI': '<L6_ULI, L, 1, 0, None, None, X>', 'L7_ULI': '<L7_ULI, L, 1, 0, None, None, X>', 'L8_ULI': '<L8_ULI, L, 1, 0, None, None, X>', 'L9_ULI': '<L9_ULI, L, 1, 0, None, None, X>', 'L1A_ULI': '<L1A_ULI, L, 1, 0, None, None, X>', 'L2A_ULI': '<L2A_ULI, L, 1, 0, None, None, X>', 'L3A_ULI': '<L3A_ULI, L, 1, 0, None, None, X>', 'L4A_ULI': '<L4A_ULI, L, 1, 0, None, None, X>', 'L5A_ULI': '<L5A_ULI, L, 1, 0, None, None, X>', 'N1_ULI': '<N1_ULI, N, 1, 0, None, None, X>', 'N2_ULI': '<N2_ULI, N, 1, 0, None, None, X>', 'FIS_ULI': '<FIS_ULI, L, 1, 0, None, None, X>', 'REK_ULI': '<REK_ULI, L, 1, 0, None, None, X>', 'STO_ULI': '<STO_ULI, L, 1, 0, None, None, X>', 'FRA_ULI': '<FRA_ULI, C, 6, 0, True, 0, I>', 'FRR_ULI': '<FRR_ULI, C, 6, 0, True, 0, I>', 'MJE_ULI': '<MJE_ULI, C, 2, 0, None, None, X>', 'PAS_ULI': '<PAS_ULI, C, 10, 0, None, None, X>', 'DAS_ULI': '<DAS_ULI, D, 8, 0, None, None, X>', 'MTR_ULI': '<MTR_ULI, C, 7, 0, None, None, X>' }
        self.assertEqual(len(self._adapter.header_fields), len(target))
        for k, v in self._adapter.header_fields.items():
            self.assertEqual(str(v), target[k])

    def test_is_field_type(self):
        """tests if field type is determined correctly"""
        self._set_up(mock=True)
        char_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3])
        date_field = Field('DAT_ULI', *self._adapter._table.field_info('DAT_ULI')[:3])
        self.assertEqual(date_field.is_type(Field.CHAR), False)
        self.assertEqual(date_field.is_type(Field.DATE), True)
        self.assertEqual(char_field.is_type(Field.CHAR), True)
        self.assertEqual(char_field.is_type(Field.DATE), False)

    def test_determine_char_column_is_int(self):
        """tests determining if the char column is an integer value"""
        self._set_up(mock=True)
        mock_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, True)
        mock_field = Field('DOK_ULI', *self._adapter._table.field_info('DOK_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, True)
        mock_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, True)
        mock_field = Field('PAR_ULI', *self._adapter._table.field_info('PAR_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, True)
        mock_field = Field('PAR_ULI', *self._adapter._table.field_info('PAR_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field, skip_empty=False)
        self.assertEqual(outcome, False)
        mock_field = Field('DAT_ULI', *self._adapter._table.field_info('DAT_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, False)
        mock_field = Field('KAS_ULI', *self._adapter._table.field_info('KAS_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, False)
        mock_field = Field('MIS_ULI', *self._adapter._table.field_info('MIS_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, None)
        mock_field = Field('MIS_ULI', *self._adapter._table.field_info('MIS_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field, skip_empty=False)
        self.assertEqual(outcome, False)
        mock_field = Field('FRA_ULI', *self._adapter._table.field_info('FRA_ULI')[:3])
        outcome = self._adapter._is_char_column_int(mock_field)
        self.assertEqual(outcome, True)

    def test_determine_char_column_is_padded(self):
        """tests determining if an integer char column is padded"""
        self._set_up(mock=True)
        mock_field = Field('OBJ_ULI', *self._adapter._table.field_info('OBJ_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], True)
        self.assertEqual(outcome[1], '0')
        mock_field = Field('DOK_ULI', *self._adapter._table.field_info('DOK_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], False)
        self.assertEqual(outcome[1], None)
        mock_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], True)
        self.assertEqual(outcome[1], '0')
        mock_field = Field('PAR_ULI', *self._adapter._table.field_info('PAR_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], True)
        self.assertEqual(outcome[1], '0')
        mock_field = Field('PAR_ULI', *self._adapter._table.field_info('PAR_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field, skip_empty=False)
        self.assertEqual(outcome[0], False)
        self.assertEqual(outcome[1], None)
        mock_field = Field('DAT_ULI', *self._adapter._table.field_info('DAT_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], False)
        self.assertEqual(outcome[1], None)
        mock_field = Field('KAS_ULI', *self._adapter._table.field_info('KAS_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], False)
        self.assertEqual(outcome[1], None)
        mock_field = Field('MIS_ULI', *self._adapter._table.field_info('MIS_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], False)
        self.assertEqual(outcome[1], None)
        mock_field = Field('FRA_ULI', *self._adapter._table.field_info('FRA_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field)
        self.assertEqual(outcome[0], True)
        self.assertEqual(outcome[1], '0')
        mock_field = Field('FRA_ULI', *self._adapter._table.field_info('FRA_ULI')[:3])
        outcome = self._adapter._is_char_column_padded_int(mock_field, skip_empty=False)
        self.assertEqual(outcome[0], False)
        self.assertEqual(outcome[1], None)

    # --- @TODO-00 ---
    # def test_write_inserting_codepage_strings(self):
    #     """tests whether codepage specific chars get appended correctly"""
    #     self.assertEqual(False, True)

    # def test_update_inserting_codepage_srtings(self):
    #     """test whether codepage specific chars get updated correctly"""
    #     self.assertEqual(False, True)

    # def test_appending_mixed_types_record(self):
    #     """test appending a mismatched types record to the table"""
    #     self._set_up('ULIZ.DBF', 'W')
    #     sample_record = {'OBJ_ULI': 10, 'DOK_ULI': 20, 'SIF_ULI': 0, 'GOT_ULI': None, 'NAL_ULI': 'ADM', 'DAT_ULI': dbf.DateTime(2021, 6, 14), 'OTP_ULI': '225883', 'NAO_ULI': None, 'DAI_ULI': '2021-06-14', 'MIS_ULI': None, 'VAL_ULI': dbf.DateTime(2021, 6, 14), 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': None, 'TRG_ULI': None, 'KAS_ULI': 0, 'PUT_ULI': '001', 'NAP_ULI': '', 'LIK_ULI': None, 'FIN_ULI': None, 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': None, 'L2A_ULI': None, 'L3A_ULI': None, 'L4A_ULI': None, 'L5A_ULI': None, 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': None, 'REK_ULI': None, 'STO_ULI': None, 'FRA_ULI': None, 'FRR_ULI': None, 'MJE_ULI': None, 'PAS_ULI': None, 'DAS_ULI': None, 'MTR_ULI': None}
    #     result = self._adapter.read([
    #         ('OBJ_ULI', lambda x: x == '010'),
    #         ('DOK_ULI', lambda x: x == '20'),
    #         ])
    #     fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
    #     sample_record['SIF_ULI'] = fresh_row_id
    #     self._adapter.write(sample_record)
    #     result = self._adapter.read([
    #         ('OBJ_ULI', lambda x: x == '010'),
    #         ('DOK_ULI', lambda x: x == '20'),
    #         ])
    #     self.assertEqual(result[-1]['SIF_ULI'], fresh_row_id)

    # def test_updating_single_record(self):
    #     """tests updating a single existing record"""
    #     self.assertEqual(False, True)

    # def test_updating_multiple_records(self):
    #     """tests updating multiple records by a certain criteria"""
    #     self.assertEqual(False, True)

if __name__ == '__main__':
    unittest.main(failfast=True)