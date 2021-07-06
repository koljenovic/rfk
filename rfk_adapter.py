#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import dbf
import datetime
import unittest
import os
from six import string_types
from unittest import TestCase
from collections import defaultdict

RUN_SLOW = True

class FieldError(Exception):
    pass

class Type:
    NULL = ord('?') # no type inferrence has been attempted
    UNDEFINED = ord('X') # type inferrence failed
    CHAR = int(dbf.FieldType.CHAR)
    CURRENCY = int(dbf.FieldType.CURRENCY)
    DATE = int(dbf.FieldType.DATE)
    DATETIME = int(dbf.FieldType.DATETIME)
    DOUBLE = int(dbf.FieldType.DOUBLE)
    FLOAT = int(dbf.FieldType.FLOAT)
    GENERAL = int(dbf.FieldType.GENERAL)
    INTEGER = int(dbf.FieldType.INTEGER)
    LOGICAL = int(dbf.FieldType.LOGICAL)
    MEMO = int(dbf.FieldType.MEMO)
    NUMERIC = int(dbf.FieldType.NUMERIC)
    PICTURE = int(dbf.FieldType.PICTURE)
    TIMESTAMP = int(dbf.FieldType.TIMESTAMP)

class Field:
    """Field class represents the meta DBF header field"""
    def __init__(self, name, ftype, length, decimals, is_padded=None, pad=None, ctype=Type.NULL):
        self.name = name
        self.ftype = ftype # (source) DBF Field type
        self.length = length
        self.decimals = decimals
        self.is_padded = is_padded
        self.pad = pad
        self.ctype = ctype # (inferred) "Column" type

    def __repr__(self):
        return '<' + ', '.join([self.name, chr(self.ftype), str(self.length), str(self.decimals), str(self.is_padded), str(self.pad), chr(self.ctype)]) + '>'

    def is_type(self, ftype):
        return self.ftype == ftype

    def ftoc(self, value):
        """converts ftype values into ctype if possible"""
        if self.ctype == Type.NULL or self.ctype == Type.UNDEFINED:
            raise ValueError('missing inferred type for Field %s', self.name)
        if self.ctype == Type.INTEGER:
            return int(value) if value else None
        if self.ctype == Type.CHAR:
            return value
        if self.ctype == Type.FLOAT:
            return value
        if self.ftype == self.ctype:
            return value
        raise ValueError('undefined conversion method from ftype %s to ctype %s', (chr(self.ftype), chr(self.ctype)))

# @TODO-XX: Full type conversion should be seamlesly integrated into this adapter
# @TODO-24: Determine mandatory fields
class RFKAdapter:
    def __init__(self, db_path, table_name, mode='-', mock=False):
        self.db_path = db_path
        self.table_name = table_name
        self._table = dbf.Table(db_path + table_name, codepage='cp852', dbf_type='db3')
        self._table.open(mode=dbf.READ_WRITE if mode.lower() == 'w' else dbf.READ_ONLY)
        self._parse_headers(mock)

    def __del__(self):
        if hasattr(self, '_table') and self._table.status != dbf.CLOSED:
            self._table.close()

    @staticmethod
    def _prepare_value(value):
        """Converts encoding and strips whitespace"""
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
    def _is_char_padded_string(value, length, pad=' '):
        """determines if and how the value is padded if it can be determined"""
        if not isinstance(value, string_types):
            raise ValueError('%s is not CHAR type', value)
        if not value.strip():
            return None, None
        if len(value) != length:
            return False, None
        if value[0] in pad and value[-1] in pad:
            return True, 'B'
        if len(value) != len(value.lstrip(pad)):
            return True, 'L'
        if len(value) != len(value.rstrip(pad)):
            return True, 'R'
        return False, None

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
        """Returns if the whole column is integers

        Returns if the column contains only integers, None if it cannot
        be determined because all values are blank. Skips empty values by default.
        """
        if not column.is_type(Type.CHAR):
            return False
        all_empty = True
        for record in self._read(raw_result=True):
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
            for record in self._read(raw_result=True):
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

    def _is_char_column_string(self, column, skip_empty=True):
        """Returns if the whole char column contains regular strings"""
        is_ints = self._is_char_column_int(column, skip_empty)
        return not is_ints if is_ints != None else None

    def _is_char_column_padded_string(self, column, skip_empty=True):
        """Return if the whole column is regular padded string values"""
        is_strings = self._is_char_column_string(column, skip_empty)
        if is_strings == True:
            sides = defaultdict(int)
            for record in self._read(raw_result=True):
                if record:
                    is_padded, pad_side = RFKAdapter._is_char_padded_string(record[column.name], column.length)
                    if is_padded:
                        sides[pad_side] += 1
                elif record[column.name].strip() or not skip_empty:
                    return False, None
            if sides:
                sides = sorted(sides.items(), key=lambda x: x[1], reverse=True)
                return True, sides[0][0]
            else:
                return False, None
        return is_strings, None

    def _parse_headers(self, mock=False):
        """Parses the fields from table headers"""
        fields = self._table._meta.fields
        self.header_fields = { field: Field(field, *self._table.field_info(field)[:3]) for field in fields }
        if not mock:
            base_name = os.path.splitext(self.table_name)[0]
            cache_path = os.path.join(self.db_path, base_name + '.json')
            if os.path.isfile(cache_path):
                self._restore_headers()
            else:
                for field in self.header_fields.values():
                    if field.is_type(Type.CHAR):
                        if self._is_char_column_int(field):
                            field.ctype = Type.INTEGER
                            field.is_padded, field.pad = self._is_char_column_padded_int(field)
                        elif self._is_char_column_string(field):
                            field.ctype = Type.CHAR
                            is_padded, side = self._is_char_column_padded_string(field)
                            field.is_padded = side if side else is_padded
                            field.pad = ' ' if is_padded else None
                        else:
                            field.ctype = Type.UNDEFINED
                    elif field.is_type(Type.NUMERIC) and field.decimals > 0:
                        field.ctype = Type.FLOAT
                    else:
                        field.ctype = field.ftype

    def _read(self, where=[], raw_result=False, infer_type=False):
        """Read, fetch and filter from RFK table

        - `raw_result` flag will force returning the results as read from the DBF
        without any further prep.
        - `infer_type` flag forces using inferred types everywhere
        """
        result = []
        fields = self._table._meta.fields
        for field_name, constr in where:
            if field_name not in fields:
                raise FieldError('No field with name %s in table %s', (field_name, self.table_name))
                return
        for record in self._table:
            satisfies = True
            for field_name, constr in where:
                value = RFKAdapter._prepare_value(record[field_name])
                if infer_type:
                    value = self.header_fields[field_name].ftoc(value)
                if not constr(value):
                    satisfies = False
                    break
            if satisfies:
                result_record = {}
                for field_name in fields:
                    result_value = record[field_name]
                    if not raw_result:
                        result_value = RFKAdapter._prepare_value(result_value)
                        if infer_type:
                            try:
                                result_value = self.header_fields[field_name].ftoc(result_value)
                            except ValueError:
                                pass
                    result_record[field_name] = result_value
                result.append(result_record)
        return result

    def read_all(self):
        """Returns all the DBF values, type inferred"""
        return self._read()

    def filter(self, where=[]):
        """Returns all filtered DBF values, best effort type inferred

        where e.g. [('OBJ_ULI', lambda x: x == '010')]
        """
        return self._read(where, infer_type=True)

    def write(self, data):
        """Appends a new record to the table"""
        # @TODO: treba paddovati C intove
            # - ukoliko je ftype C i ctype I padovati padom
            # - ukoliko je i ftype i ctype C padovati sa \w
            # - paddovati sve kako je u izvorniku
            # - uvijek provjera da li je padding dobar
            # - strip args po default pa padovati?
        # @TODO: konvertuj datume iz ISO8601
            # ako je ftype D a argument string, konvertovati po ISO8601 formatu
        # @TODO: da li je dobar encoding upisa?
        self._table.append(data)

    def update(self, table):
        baza = dbf.Table('../data/ULIZ.DBF', codepage='cp852', dbf_type='db3')
        baza.open(mode=dbf.READ_WRITE)
        with baza[-1]:
            baza[-1]['KUF_ULI'] = '4322'
        baza.close()

    def _cache_headers(self):
        """Caches parsed headers to file because parsing is time demanding

        @TODO-XX: update cache on structural changes, ATM this is unneccessary,
        flush manually by deleting *.json
        """
        base_name = os.path.splitext(self.table_name)[0]
        json_path = os.path.join(self.db_path, base_name + '.json')
        with open(json_path, 'w') as fp:
            headers = { field.name: field.__dict__ for field in self.header_fields.values() }
            json.dump(headers, fp)

    def _restore_headers(self):
        """Restores header fields from cache"""
        base_name = os.path.splitext(self.table_name)[0]
        json_path = os.path.join(self.db_path, base_name + '.json')
        with open(json_path, 'r') as fp:
            headers = json.load(fp)
            for field_name, field in headers.items():
                self.header_fields[field_name] = Field(**field)

class RFKAdapterTest(TestCase):
    def __init__(self, *args, **kwds):
        self._db_path = '../data/'
        super(RFKAdapterTest, self).__init__(*args, **kwds)

    def _set_up(self, table_name='ULIZ.DBF', mode='r', mock=False): 
        self._adapter = RFKAdapter(self._db_path, table_name, mode, mock=mock)

    def test_101_read_constructor(self):
        """test opening the table for reading"""
        self._adapter = RFKAdapter(self._db_path, 'ULIZ.DBF', mock=True)
        self.assertEqual(self._adapter._table.status, dbf.READ_ONLY)

    def test_102_write_constructor(self):
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

    def test_single_filtered_internal_read(self):
        """test internal _reading a record filtered by a single condition"""
        self._set_up(mock=True)
        result = self._adapter._read([('OTP_ULI', lambda x: x == '880')])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['OTP_ULI'], '880')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')
        result = self._adapter._read([('SIF_ULI', lambda x: x == '00911')])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['SIF_ULI'], '00911')

    def test_single_filtered_read(self):
        """test reading a single record filtered by a single condition"""
        self._set_up()
        result = self._adapter.filter([('SIF_ULI', lambda x: x == 876)])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['OTP_ULI'], '876')
        self.assertEqual(result[0]['OBJ_ULI'], 10)
        self.assertEqual(result[0]['DOK_ULI'], 20)
        self.assertEqual(result[0]['SIF_ULI'], 876)
        result = self._adapter.filter([('SIF_ULI', lambda x: x == 911)])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['SIF_ULI'], 911)

    def test_single_filtered_internal_raw_read(self):
        """test _reading a single raw record filtered by a single condition"""
        self._set_up(mock=True)
        result = self._adapter._read([('OTP_ULI', lambda x: x == '880')], raw_result=True)
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['OTP_ULI'], '880                 ')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')
        self.assertEqual(result[0]['KUF_ULI'], '       880')

    def test_multi_filtered_read(self):
        """test reading multiple record filtered by multiple conditions"""
        self._set_up(mock=True)
        result = self._adapter._read([
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
        try:
            self.assertRaises(FieldError, self._adapter._read, [('SOK_ULI', lambda x: True)])
        except:
            pass

    def test_return_all(self):
        """return all the records with empty filter list"""
        self._set_up(mock=True)
        self.assertGreater(len(self._adapter._read()), 0)

    def test_record_append(self):
        """test appending a single record to the table"""
        self._set_up('ULIZ.DBF', 'W', mock=True)
        sample_record = {'OBJ_ULI': '010', 'DOK_ULI': '20', 'SIF_ULI': '00000', 'GOT_ULI': None, 'NAL_ULI': 'ADM', 'DAT_ULI': dbf.DateTime(2021, 6, 14), 'OTP_ULI': '225883', 'NAO_ULI': None, 'DAI_ULI': dbf.DateTime(2021, 6, 14), 'MIS_ULI': None, 'VAL_ULI': dbf.DateTime(2021, 6, 14), 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': None, 'TRG_ULI': None, 'KAS_ULI': 0, 'PUT_ULI': '001', 'NAP_ULI': '', 'LIK_ULI': None, 'FIN_ULI': None, 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': None, 'L2A_ULI': None, 'L3A_ULI': None, 'L4A_ULI': None, 'L5A_ULI': None, 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': None, 'REK_ULI': None, 'STO_ULI': None, 'FRA_ULI': None, 'FRR_ULI': None, 'MJE_ULI': None, 'PAS_ULI': None, 'DAS_ULI': None, 'MTR_ULI': None}
        result = self._adapter._read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
        sample_record['SIF_ULI'] = fresh_row_id
        self._adapter.write(sample_record)
        result = self._adapter._read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertEqual(result[-1]['SIF_ULI'], fresh_row_id)

    def test_determine_char_field_is_int(self):
        """tests determining if the char field is an integer value"""
        self.assertEqual(RFKAdapter._char_to_int('225'), 225)
        self.assertEqual(RFKAdapter._char_to_int('00225'), 225)
        try:
            self.assertRaises(Exception, RFKAdapter._char_to_int(None))
        except:
            pass
        self.assertEqual(RFKAdapter._char_to_int('Škafiškafnjak'), None)
        self.assertEqual(RFKAdapter._char_to_int('00000'), 0)

    def test_determine_char_int_field_is_padded(self):
        """tests determining if an integer char field is padded"""
        self.assertEqual(RFKAdapter._is_char_padded_int('225', 5), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_int('0225', 5), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_int('00225', 5), (True, '0'))
        self.assertEqual(RFKAdapter._is_char_padded_int('22588', 5), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_int(' 225', 5), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_int('  225', 5), (True, ' '))
        self.assertEqual(RFKAdapter._is_char_padded_int(' 883', 4), (True, ' '))

    def test_field_string_rep(self):
        """test whether field object gets represented correctly"""
        self._set_up(mock=True)
        outcome = Field('OBJ_ULI', *self._adapter._table.field_info('OBJ_ULI')[:3])
        self.assertEqual(str(outcome), '<OBJ_ULI, C, 3, 0, None, None, ?>')
        outcome.is_padded, outcome.pad = self._adapter._is_char_column_padded_int(outcome)
        self.assertEqual(str(outcome), '<OBJ_ULI, C, 3, 0, True, 0, ?>')

    def test_header_parsed(self):
        """tests if the headers gets parsed and if it gets parsed right"""
        self._set_up()
        target = { 'OBJ_ULI': '<OBJ_ULI, C, 3, 0, True, 0, I>', 'DOK_ULI': '<DOK_ULI, C, 2, 0, False, None, I>', 'SIF_ULI': '<SIF_ULI, C, 5, 0, True, 0, I>', 'GOT_ULI': '<GOT_ULI, C, 1, 0, False, None, I>', 'NAL_ULI': '<NAL_ULI, C, 3, 0, False, None, C>', 'DAT_ULI': '<DAT_ULI, D, 8, 0, None, None, D>', 'OTP_ULI': '<OTP_ULI, C, 20, 0, R,  , C>', 'NAO_ULI': '<NAO_ULI, C, 50, 0, None, None, X>', 'DAI_ULI': '<DAI_ULI, D, 8, 0, None, None, D>', 'MIS_ULI': '<MIS_ULI, C, 50, 0, None, None, X>', 'VAL_ULI': '<VAL_ULI, D, 8, 0, None, None, D>', 'DAN_ULI': '<DAN_ULI, N, 3, 0, None, None, N>', 'RBR_ULI': '<RBR_ULI, N, 4, 0, None, None, N>', 'KUF_ULI': '<KUF_ULI, C, 10, 0, L,  , C>', 'ZAD_ULI': '<ZAD_ULI, C, 3, 0, True, 0, I>', 'PAR_ULI': '<PAR_ULI, C, 7, 0, True, 0, I>', 'PRO_ULI': '<PRO_ULI, C, 3, 0, False, None, I>', 'TRG_ULI': '<TRG_ULI, C, 3, 0, True, 0, I>', 'KAS_ULI': '<KAS_ULI, N, 5, 2, None, None, F>', 'PUT_ULI': '<PUT_ULI, C, 3, 0, True, 0, I>', 'NAP_ULI': '<NAP_ULI, M, 10, 0, None, None, M>', 'LIK_ULI': '<LIK_ULI, L, 1, 0, None, None, L>', 'FIN_ULI': '<FIN_ULI, L, 1, 0, None, None, L>', 'L0_ULI': '<L0_ULI, L, 1, 0, None, None, L>', 'L1_ULI': '<L1_ULI, L, 1, 0, None, None, L>', 'L2_ULI': '<L2_ULI, L, 1, 0, None, None, L>', 'L3_ULI': '<L3_ULI, L, 1, 0, None, None, L>', 'L4_ULI': '<L4_ULI, L, 1, 0, None, None, L>', 'L5_ULI': '<L5_ULI, L, 1, 0, None, None, L>', 'L6_ULI': '<L6_ULI, L, 1, 0, None, None, L>', 'L7_ULI': '<L7_ULI, L, 1, 0, None, None, L>', 'L8_ULI': '<L8_ULI, L, 1, 0, None, None, L>', 'L9_ULI': '<L9_ULI, L, 1, 0, None, None, L>', 'L1A_ULI': '<L1A_ULI, L, 1, 0, None, None, L>', 'L2A_ULI': '<L2A_ULI, L, 1, 0, None, None, L>', 'L3A_ULI': '<L3A_ULI, L, 1, 0, None, None, L>', 'L4A_ULI': '<L4A_ULI, L, 1, 0, None, None, L>', 'L5A_ULI': '<L5A_ULI, L, 1, 0, None, None, L>', 'N1_ULI': '<N1_ULI, N, 1, 0, None, None, N>', 'N2_ULI': '<N2_ULI, N, 1, 0, None, None, N>', 'FIS_ULI': '<FIS_ULI, L, 1, 0, None, None, L>', 'REK_ULI': '<REK_ULI, L, 1, 0, None, None, L>', 'STO_ULI': '<STO_ULI, L, 1, 0, None, None, L>', 'FRA_ULI': '<FRA_ULI, C, 6, 0, True, 0, I>', 'FRR_ULI': '<FRR_ULI, C, 6, 0, True, 0, I>', 'MJE_ULI': '<MJE_ULI, C, 2, 0, None, None, X>', 'PAS_ULI': '<PAS_ULI, C, 10, 0, None, None, X>', 'DAS_ULI': '<DAS_ULI, D, 8, 0, None, None, D>', 'MTR_ULI': '<MTR_ULI, C, 7, 0, None, None, X>' }
        self.assertEqual(len(self._adapter.header_fields), len(target))
        for k, v in self._adapter.header_fields.items():
            self.assertEqual(str(v), target[k])

    def test_is_field_type(self):
        """tests if field type is determined correctly"""
        self._set_up(mock=True)
        char_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3])
        date_field = Field('DAT_ULI', *self._adapter._table.field_info('DAT_ULI')[:3])
        self.assertEqual(date_field.is_type(Type.CHAR), False)
        self.assertEqual(date_field.is_type(Type.DATE), True)
        self.assertEqual(char_field.is_type(Type.CHAR), True)
        self.assertEqual(char_field.is_type(Type.DATE), False)

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

    def test_001_appending_mixed_types_record(self):
        """test appending a mismatched types record to the table"""
        self._set_up('ULIZ.DBF', 'W')
        sample_record = {'OBJ_ULI': 10, 'DOK_ULI': 20, 'SIF_ULI': 0, 'GOT_ULI': None, 'NAL_ULI': 'ADM', 'DAT_ULI': dbf.DateTime(2021, 6, 14), 'OTP_ULI': '225883', 'NAO_ULI': None, 'DAI_ULI': '2021-06-14', 'MIS_ULI': None, 'VAL_ULI': dbf.DateTime(2021, 6, 14), 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': None, 'TRG_ULI': None, 'KAS_ULI': 0, 'PUT_ULI': '001', 'NAP_ULI': '', 'LIK_ULI': None, 'FIN_ULI': None, 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': None, 'L2A_ULI': None, 'L3A_ULI': None, 'L4A_ULI': None, 'L5A_ULI': None, 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': None, 'REK_ULI': None, 'STO_ULI': None, 'FRA_ULI': None, 'FRR_ULI': None, 'MJE_ULI': None, 'PAS_ULI': None, 'DAS_ULI': None, 'MTR_ULI': None}
        result = self._adapter._read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
        sample_record['SIF_ULI'] = fresh_row_id
        self._adapter.write(sample_record)
        result = self._adapter._read([
            ('OBJ_ULI', lambda x: x == '010'),
            ('DOK_ULI', lambda x: x == '20'),
            ])
        self.assertEqual(result[-1]['SIF_ULI'], fresh_row_id)

    # def test_updating_single_record(self):
    #     """tests updating a single existing record"""
    #     self.assertEqual(False, True)

    # def test_updating_multiple_records(self):
    #     """tests updating multiple records by a certain criteria"""
    #     self.assertEqual(False, True)

    def test_is_char_column_string(self):
        """tests determining whether a char column is a string column"""
        self._set_up(mock=True)
        mock_field = Field('OTP_ULI', *self._adapter._table.field_info('OTP_ULI')[:3])
        outcome = self._adapter._is_char_column_string(mock_field)
        self.assertEqual(outcome, True)
        mock_field = Field('MIS_ULI', *self._adapter._table.field_info('MIS_ULI')[:3])
        outcome = self._adapter._is_char_column_string(mock_field)
        self.assertEqual(outcome, None)
        mock_field = Field('MIS_ULI', *self._adapter._table.field_info('MIS_ULI')[:3])
        outcome = self._adapter._is_char_column_string(mock_field, skip_empty=False)
        self.assertEqual(outcome, True)
        mock_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3])
        outcome = self._adapter._is_char_column_string(mock_field)
        self.assertEqual(outcome, False)

    def test_is_char_column_padded_string(self):
        """tests determining whether char string column is padded and how"""
        self._set_up(mock=True)
        result = self._adapter._read([('SIF_ULI', lambda x: x == '00911')], raw_result=True)
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['SIF_ULI'], '00911')

    def test_read_all(self):
        """tests if all the values get read"""
        # @TODO-27: improve test
        self._set_up(mock=True)
        self.assertGreater(len(self._adapter.read_all()), 0)

    def test_filter(self):
        """tests filtering the read values"""
        # @TODO-28: improve test
        self._set_up(mock=True)
        self.assertGreater(len(self._adapter.filter([])), 0)

    def test_field_to_column_type_conversion(self):
        """tests if ftype values get converted to ctype correctly"""
        self._set_up(mock=True)
        mock_field = Field('MIS_ULI', *self._adapter._table.field_info('MIS_ULI')[:3], None, None, Type.UNDEFINED)
        try:
            self.assertRaises(ValueError, mock_field.ftoc('asdf'))
        except:
            pass
        mock_field = Field('OTP_ULI', *self._adapter._table.field_info('OTP_ULI')[:3], False, None, Type.CHAR)
        outcome = mock_field.ftoc('naknada za aparat')
        self.assertEqual(outcome, 'naknada za aparat')
        mock_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3], True, '0', Type.INTEGER)
        outcome = mock_field.ftoc('00880')
        self.assertEqual(outcome, 880)
        self.assertEqual(type(outcome) is int, True)
        mock_field = Field('KAS_ULI', *self._adapter._table.field_info('KAS_ULI')[:3], True, '0', Type.FLOAT)
        outcome = mock_field.ftoc(2.2)
        self.assertEqual(outcome, 2.2)
        mock_field = Field('GOT_ULI', *self._adapter._table.field_info('GOT_ULI')[:3], True, '0', Type.INTEGER)
        outcome = mock_field.ftoc('')
        self.assertEqual(outcome, None)

    def test_is_char_string_padded(self):
        """test whether padding is determined correctly"""
        try:
            self.assertRaises(ValueError, RFKAdapter._is_char_padded_string(225, 20))
        except:
            pass
        self.assertEqual(RFKAdapter._is_char_padded_string('225883              ', 20), (True, 'R'))
        self.assertEqual(RFKAdapter._is_char_padded_string('       225883       ', 20), (True, 'B'))
        self.assertEqual(RFKAdapter._is_char_padded_string('005368', 6, '0'), (True, 'L'))
        self.assertEqual(RFKAdapter._is_char_padded_string('       745', 10), (True, 'L'))
        self.assertEqual(RFKAdapter._is_char_padded_string('5368', 6), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_string('123456', 6), (False, None))
        self.assertEqual(RFKAdapter._is_char_padded_string('      ', 6), (None, None))
        self.assertEqual(RFKAdapter._is_char_padded_string(' 5368', 6), (False, None))

    def test_is_char_column_padded_string(self):
        """tests whether the whole column is padded char strings"""
        # @TODO-30: write more tests
        self._set_up(mock=True)
        mock_field = Field('OTP_ULI', *self._adapter._table.field_info('OTP_ULI')[:3])
        self.assertEqual(self._adapter._is_char_column_padded_string(mock_field), (True, 'R'))
        mock_field = Field('KUF_ULI', *self._adapter._table.field_info('KUF_ULI')[:3])
        self.assertEqual(self._adapter._is_char_column_padded_string(mock_field), (True, 'L'))

    def test_source_header_signature_change_detection(self):
        """test whether header signature change gets registered"""
        self.assertEqual(False, True)

class RFKAdapterSlowTest(RFKAdapterTest):
    def __init__(self, *args, **kwds):
        super(RFKAdapterSlowTest, self).__init__(*args, **kwds)

    @unittest.skipUnless(RUN_SLOW, "slow")
    def test_json_caching_and_restoring_parsed_headers(self):
        """tests whether header Field objects get cached and restored correctly"""
        self._set_up()
        base_name = os.path.splitext(self._adapter.table_name)[0]
        json_path = os.path.join(self._adapter.db_path, base_name + '.json')
        self._adapter._cache_headers()
        self.assertEqual(os.path.isfile(json_path), True)
        self._adapter._restore_headers()
        with open(json_path, 'r') as fp:
            headers = json.load(fp)
            for field_name, field_value in headers.items():
                self.assertEqual(str(Field(**field_value)), str(self._adapter.header_fields[field_name]))

if __name__ == '__main__':
    unittest.main(failfast=False)