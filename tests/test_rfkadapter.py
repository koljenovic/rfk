#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
=========
Copyright
=========

    - Copyright: 2021 MEKOM d.o.o. Visoko -- All rights reserved.
    - Author: Malik Koljenović
    - Contact: malik@mekom.ba

"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../src')

import rfkadapter
import json
import secrets
import unittest
import hashlib
import binascii

from datetime import datetime, date
from rfkadapter import DBFAdapter, RFKAdapter, Field, Type, FieldError, _EXE, HarbourError
from unittest import TestCase

class DBFAdapterTest(TestCase):
    def __init__(self, *args, **kwds):
        self._db_path = os.path.dirname(os.path.realpath(__file__)) + '/data/'
        super(DBFAdapterTest, self).__init__(*args, **kwds)

    def _set_up(self, table_name='ULIZ.DBF', code_page='cp852'): 
        self._adapter = DBFAdapter(self._db_path, table_name, code_page)

    def test_head_read(self):
        headers = json.loads(DBFAdapter._head(self._db_path, 'ULIZ.DBF', 'cp852'))
        self.assertEqual(headers[-1], "<MTR_ULI, C, 7, 0>")

    def test_parse_meta(self):
        self._set_up()
        self._adapter._parse_meta()
        self.assertEqual('MTR_ULI' in self._adapter._meta, True)
        # MEMO should be in _meta
        self.assertEqual('NAP_ULI' in self._adapter._meta, True)

    def test_export(self):
        outcome = DBFAdapter._export(self._db_path, 'ULIZ.DBF', ['ULIZ01', 'ULIZ02', 'ULIZ03'], [], 'cp852')
        self.assertEqual(len(outcome) > 500, True)

    def test_indices_get_found(self):
        """tests whether index files get found correctly"""
        self._set_up()
        self.assertEqual('ULIZ01' in self._adapter.index_files, True)
        self.assertEqual('ULIZ02' in self._adapter.index_files, True)
        self.assertEqual('ULIZ03' in self._adapter.index_files, True)

    def test_parse_memo(self):
        self._set_up()
        target = {'OBJ_ULI': ('OBJ_ULI', 67, 3, 0), 'DOK_ULI': ('DOK_ULI', 67, 2, 0), 'SIF_ULI': ('SIF_ULI', 67, 5, 0), 'GOT_ULI': ('GOT_ULI', 67, 1, 0), 'NAL_ULI': ('NAL_ULI', 67, 3, 0), 'DAT_ULI': ('DAT_ULI', 68, 8, 0), 'OTP_ULI': ('OTP_ULI', 67, 20, 0), 'NAO_ULI': ('NAO_ULI', 67, 50, 0), 'DAI_ULI': ('DAI_ULI', 68, 8, 0), 'MIS_ULI': ('MIS_ULI', 67, 50, 0), 'VAL_ULI': ('VAL_ULI', 68, 8, 0), 'DAN_ULI': ('DAN_ULI', 78, 3, 0), 'RBR_ULI': ('RBR_ULI', 78, 4, 0), 'KUF_ULI': ('KUF_ULI', 67, 10, 0), 'ZAD_ULI': ('ZAD_ULI', 67, 3, 0), 'PAR_ULI': ('PAR_ULI', 67, 7, 0), 'PRO_ULI': ('PRO_ULI', 67, 3, 0), 'TRG_ULI': ('TRG_ULI', 67, 3, 0), 'KAS_ULI': ('KAS_ULI', 78, 5, 2), 'PUT_ULI': ('PUT_ULI', 67, 3, 0), 'NAP_ULI': ('NAP_ULI', 77, 10, 0), 'LIK_ULI': ('LIK_ULI', 76, 1, 0), 'FIN_ULI': ('FIN_ULI', 76, 1, 0), 'L0_ULI': ('L0_ULI', 76, 1, 0), 'L1_ULI': ('L1_ULI', 76, 1, 0), 'L2_ULI': ('L2_ULI', 76, 1, 0), 'L3_ULI': ('L3_ULI', 76, 1, 0), 'L4_ULI': ('L4_ULI', 76, 1, 0), 'L5_ULI': ('L5_ULI', 76, 1, 0), 'L6_ULI': ('L6_ULI', 76, 1, 0), 'L7_ULI': ('L7_ULI', 76, 1, 0), 'L8_ULI': ('L8_ULI', 76, 1, 0), 'L9_ULI': ('L9_ULI', 76, 1, 0), 'L1A_ULI': ('L1A_ULI', 76, 1, 0), 'L2A_ULI': ('L2A_ULI', 76, 1, 0), 'L3A_ULI': ('L3A_ULI', 76, 1, 0), 'L4A_ULI': ('L4A_ULI', 76, 1, 0), 'L5A_ULI': ('L5A_ULI', 76, 1, 0), 'N1_ULI': ('N1_ULI', 78, 1, 0), 'N2_ULI': ('N2_ULI', 78, 1, 0), 'FIS_ULI': ('FIS_ULI', 76, 1, 0), 'REK_ULI': ('REK_ULI', 76, 1, 0), 'STO_ULI': ('STO_ULI', 76, 1, 0), 'FRA_ULI': ('FRA_ULI', 67, 6, 0), 'FRR_ULI': ('FRR_ULI', 67, 6, 0), 'MJE_ULI': ('MJE_ULI', 67, 2, 0), 'PAS_ULI': ('PAS_ULI', 67, 10, 0), 'DAS_ULI': ('DAS_ULI', 68, 8, 0), 'MTR_ULI': ('MTR_ULI', 67, 7, 0)}
        self._adapter._parse_meta()
        for k, v in target.items():
            self.assertEqual(k in self._adapter._meta, True)
            self.assertEqual(v, self._adapter._meta[k])

    def test_field_info(self):
        self._set_up()
        self._adapter._parse_meta()
        mock_meta = self._adapter.field_info('OTP_ULI')
        self.assertEqual(mock_meta[0], Type.CHAR)
        self.assertEqual(mock_meta[1], 20)
        self.assertEqual(mock_meta[2], 0)

    def test_update(self):
        """ basic _update smoke test to establish that input gets parsed OK """
        with open(self._db_path + '/dbfadapter/update.json') as f:
            package = json.load(f)
            outcome = DBFAdapter._update(package, self._db_path, 'ULIZ.DBF', ['ULIZ01', 'ULIZ02', 'ULIZ03'], 'cp852')
            self.assertEqual(outcome > 0, True)

    def test_reindex_fail(self):
        fail_path = os.path.dirname(os.path.realpath(__file__)) + '/data/index/fail/'
        with self.assertRaises(HarbourError):
            DBFAdapter._reindex(fail_path, 'ULIZ.DBF', ['ULIZ01.NTX', 'ULIZ02.NTX', 'ULIZ03.NTX'], 'HR852')

    def test_reindex(self):
        index_path = os.path.dirname(os.path.realpath(__file__)) + '/data/index/'
        DBFAdapter._reindex(index_path, 'ULIZ.DBF', ['ULIZ01.NTX', 'ULIZ02.NTX', 'ULIZ03.NTX'], 'HR852')
        clock = str(os.path.getmtime(index_path + 'ULIZ.DBF'))[:9]
        indices = {'ULIZ01.NTX': {}, 'ULIZ02.NTX': {}, 'ULIZ03.NTX': {}}
        for index in indices:
            indices[index]['size'] = os.path.getsize(index_path + index)
            # print('Size', index, indices[index]['size'], 'bytes')
            # NOTE: intentionally aims to corrupt the index
            if indices[index]['size'] > 0:
                init_digest256 = secrets.randbelow(2**64)
                end_digest256 = secrets.randbelow(2**64)
                with open(index_path + index, 'r+b') as fp:
                    ifh256 = hashlib.sha256()
                    ifh256.update(fp.read())
                    init_digest256 = binascii.hexlify(ifh256.digest())
                    indices[index]['hash'] = init_digest256
                    # print(indices[index]['hash'])
                    for _ in range(256):
                        address = secrets.randbelow(indices[index]['size'])
                        # NOTE: messing the headers up makes indices unrecoverable!
                        if address > 1024:
                            fp.seek(address)
                            fp.write(secrets.randbelow(256).to_bytes(1, byteorder='big'))
                outcome = DBFAdapter._reindex(index_path, 'ULIZ.DBF', ['ULIZ01.NTX', 'ULIZ02.NTX', 'ULIZ03.NTX'], 'HR852')
                self.assertEqual(outcome, True)
                with open(index_path + index, 'rb') as fp:
                    efh256 = hashlib.sha256()
                    fp.seek(0)
                    efh256.update(fp.read())
                    end_digest256 = binascii.hexlify(efh256.digest())
                self.assertEqual(init_digest256, end_digest256)

class RFKAdapterTest(TestCase):
    def __init__(self, *args, **kwds):
        self._db_path = os.path.dirname(os.path.realpath(__file__)) + '/data/'
        super(RFKAdapterTest, self).__init__(*args, **kwds)

    def _set_up(self, table_name='ULIZ.DBF', with_headers=True, code_page='cp852'): 
        self._adapter = RFKAdapter(self._db_path, table_name, code_page, with_headers=with_headers)

    def test_basic_constructor(self):
        """test opening the table for reading"""
        self._adapter = RFKAdapter(self._db_path, 'ULIZ.DBF', code_page='cp852')
        self.assertEqual(self._adapter._table != None, True)

    def test_prepare_read_value(self):
        """tests encoding and value conversion for values read from table"""
        value = 'Pletenina PIQUE-ČEŠLJANA P/C'
        raw = value.encode('cp852')
        self.assertEqual(RFKAdapter._prepare_value(raw), value)
        self.assertEqual(RFKAdapter._prepare_value('   Škafiškafnjak '), 'Škafiškafnjak')
        self.assertEqual(RFKAdapter._prepare_value(1234), 1234)

    def test_single_filtered_internal_read(self):
        """test internal _reading a record filtered by a single condition"""
        self._set_up()
        result = self._adapter._read([('OTP_ULI', 'eq', 880)])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['OTP_ULI'], '880')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')
        result = self._adapter._read([('SIF_ULI', 'eq', 870)])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['SIF_ULI'], '00870')

    def test_single_filtered_read(self):
        """test reading a single record filtered by a single condition"""
        self._set_up()
        result = self._adapter.filter([('SIF_ULI', 'eq', 876)])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['OTP_ULI'], '876')
        self.assertEqual(result[0]['OBJ_ULI'], 10)
        self.assertEqual(result[0]['DOK_ULI'], 20)
        self.assertEqual(result[0]['SIF_ULI'], 876)
        result = self._adapter.filter([('SIF_ULI', 'eq', 870)])
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['SIF_ULI'], 870)

    def test_single_filtered_internal_raw_read(self):
        """test _reading a single raw record filtered by a single condition"""
        self._set_up()
        result = self._adapter._read([('OTP_ULI', 'eq', 880)], raw_result=True)
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['OTP_ULI'], '880')
        self.assertEqual(result[0]['OBJ_ULI'], '010')
        self.assertEqual(result[0]['DOK_ULI'], '20')
        self.assertEqual(result[0]['SIF_ULI'], '00881')
        self.assertEqual(result[0]['KUF_ULI'], '       880')

    def test_multi_filtered_internal_read(self):
        """test reading multiple record filtered by multiple conditions"""
        self._set_up()
        result = self._adapter._read([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ])
        self.assertGreater(len(result), 0)
        for record in result:
            self.assertEqual(record['OBJ_ULI'], '010')
            self.assertEqual(record['DOK_ULI'], '20')

    def test_multi_filtered_read(self):
        """test reading multiple record filtered by multiple conditions"""
        self._set_up()
        result = self._adapter.read([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ])
        self.assertGreater(len(result), 0)
        for record in result:
            self.assertEqual(record['OBJ_ULI'], 10)
            self.assertEqual(record['DOK_ULI'], 20)

    def test_nonexistent_filter_key(self):
        """test raising a FieldError on nonexistent field filter key"""
        self._set_up()
        self.assertRaises(FieldError, lambda: self._adapter._read([('SOK_ULI', 'eq', True)]))

    def test_return_all(self):
        """return all the records with empty filter list"""
        self._set_up()
        self.assertGreater(len(self._adapter._read()), 1000)

    def test_record_append(self):
        """test appending a single record to the table"""
        self._set_up('ULIZ.DBF')
        target_line_len = len([f for f, v in self._adapter.header_fields.items() if not v.is_type(Type.MEMO)])
        sample_record = {'OBJ_ULI': '010', 'DOK_ULI': '20', 'SIF_ULI': '00000', 'GOT_ULI': '', 'NAL_ULI': 'ADM', 'DAT_ULI': '2021-06-14', 'OTP_ULI': 'Škafiškafnjačić', 'NAO_ULI': '', 'DAI_ULI': '2021-06-14', 'MIS_ULI': '', 'VAL_ULI': '2021-06-14', 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': '', 'TRG_ULI': '', 'KAS_ULI': 0, 'PUT_ULI': '001', 'LIK_ULI': '', 'FIN_ULI': '', 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': '', 'L2A_ULI': '', 'L3A_ULI': '', 'L4A_ULI': '', 'L5A_ULI': '', 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': '', 'REK_ULI': '', 'STO_ULI': '', 'FRA_ULI': '', 'FRR_ULI': '', 'MJE_ULI': '', 'PAS_ULI': '', 'DAS_ULI': '', 'MTR_ULI': ''}
        result = self._adapter._read([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ])
        fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
        sample_record['SIF_ULI'] = fresh_row_id
        self._adapter.write(sample_record)
        result = self._adapter._read([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ('SIF_ULI', 'eq', fresh_row_id)
            ])
        self.assertEqual(len(result), 1)
        value = 'Škafiškafnjačić'
        raw = value.encode('cp852')
        for k, v in sample_record.items():
            self.assertEqual(self._adapter.header_fields[k].ctof(result[-1][k]), self._adapter.header_fields[k].ctof(v))

        self.assertRaises(TypeError, lambda: self._adapter.write([]))
        self.assertRaises(FieldError, lambda: self._adapter.write({'MUNI_ULI': '1234'}))

        # NOTE: This is supposed to test that all the fields get included
        # The case will fail if Type.MEMO gets included into the written line,
        # it's also a sort of integration test as Harbour APPEND FROM does not
        # support or expect MEMO fields to be included into the import file.
        fresh_row_id = int(fresh_row_id) + 1
        sample_record = {'OBJ_ULI': 10, 'DOK_ULI': 20, 'SIF_ULI': fresh_row_id, 'NAL_ULI': 'ADM', 'DAT_ULI': '2021-07-29', 'OTP_ULI': 'SAJ/2021/23992', 'DAI_ULI': '2021-07-29', 'VAL_ULI': '2021-07-29', 'DAN_ULI': 0, 'RBR_ULI': 8, 'KUF_ULI': '1397', 'ZAD_ULI': '001', 'PAR_ULI': 35, 'KAS_ULI': 0}
        result_line = self._adapter.write(sample_record)
        self.assertEqual(len(result_line), target_line_len)
        result = self._adapter._read([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ('SIF_ULI', 'eq', fresh_row_id)
            ])
        self.assertEqual(len(result), 1)
        for k, v in sample_record.items():
            self.assertEqual(self._adapter.header_fields[k].ctof(result[-1][k]), self._adapter.header_fields[k].ctof(v))

    def test_determine_char_field_is_int(self):
        """tests determining if the char field is an integer value"""
        self.assertEqual(RFKAdapter._char_to_int('225'), 225)
        self.assertEqual(RFKAdapter._char_to_int('00225'), 225)
        self.assertEqual(RFKAdapter._char_to_int(None), None)
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
        self._set_up()
        outcome = Field('OBJ_ULI', *self._adapter._table.field_info('OBJ_ULI')[:3])
        self.assertEqual(str(outcome), '<OBJ_ULI, C, 3, 0, None, None, ?>')
        outcome.is_padded, outcome.pad = self._adapter._is_char_column_padded_int(outcome)
        self.assertEqual(str(outcome), '<OBJ_ULI, C, 3, 0, True, 0, ?>')

    def test_header_parsed_uliz(self):
        """tests if the headers gets parsed and if it gets parsed right"""
        self._set_up(with_headers=False)
        self._adapter._parse_headers(flush=True)
        target = { 'OBJ_ULI': '<OBJ_ULI, C, 3, 0, True, 0, I>', 'DOK_ULI': '<DOK_ULI, C, 2, 0, False, None, I>', 'SIF_ULI': '<SIF_ULI, C, 5, 0, True, 0, I>', 'GOT_ULI': '<GOT_ULI, C, 1, 0, False, None, I>', 'NAL_ULI': '<NAL_ULI, C, 3, 0, False, None, C>', 'DAT_ULI': '<DAT_ULI, D, 8, 0, None, None, D>', 'OTP_ULI': '<OTP_ULI, C, 20, 0, False, None, C>', 'NAO_ULI': '<NAO_ULI, C, 50, 0, None, None, X>', 'DAI_ULI': '<DAI_ULI, D, 8, 0, None, None, D>', 'MIS_ULI': '<MIS_ULI, C, 50, 0, None, None, X>', 'VAL_ULI': '<VAL_ULI, D, 8, 0, None, None, D>', 'DAN_ULI': '<DAN_ULI, N, 3, 0, None, None, N>', 'RBR_ULI': '<RBR_ULI, N, 4, 0, None, None, N>', 'KUF_ULI': '<KUF_ULI, C, 10, 0, L,  , C>', 'ZAD_ULI': '<ZAD_ULI, C, 3, 0, True, 0, I>', 'PAR_ULI': '<PAR_ULI, C, 7, 0, True, 0, I>', 'PRO_ULI': '<PRO_ULI, C, 3, 0, False, None, I>', 'TRG_ULI': '<TRG_ULI, C, 3, 0, True, 0, I>', 'KAS_ULI': '<KAS_ULI, N, 5, 2, None, None, F>', 'PUT_ULI': '<PUT_ULI, C, 3, 0, True, 0, I>', 'LIK_ULI': '<LIK_ULI, L, 1, 0, None, None, L>', 'FIN_ULI': '<FIN_ULI, L, 1, 0, None, None, L>', 'L0_ULI': '<L0_ULI, L, 1, 0, None, None, L>', 'L1_ULI': '<L1_ULI, L, 1, 0, None, None, L>', 'L2_ULI': '<L2_ULI, L, 1, 0, None, None, L>', 'L3_ULI': '<L3_ULI, L, 1, 0, None, None, L>', 'L4_ULI': '<L4_ULI, L, 1, 0, None, None, L>', 'L5_ULI': '<L5_ULI, L, 1, 0, None, None, L>', 'L6_ULI': '<L6_ULI, L, 1, 0, None, None, L>', 'L7_ULI': '<L7_ULI, L, 1, 0, None, None, L>', 'L8_ULI': '<L8_ULI, L, 1, 0, None, None, L>', 'L9_ULI': '<L9_ULI, L, 1, 0, None, None, L>', 'L1A_ULI': '<L1A_ULI, L, 1, 0, None, None, L>', 'L2A_ULI': '<L2A_ULI, L, 1, 0, None, None, L>', 'L3A_ULI': '<L3A_ULI, L, 1, 0, None, None, L>', 'L4A_ULI': '<L4A_ULI, L, 1, 0, None, None, L>', 'L5A_ULI': '<L5A_ULI, L, 1, 0, None, None, L>', 'N1_ULI': '<N1_ULI, N, 1, 0, None, None, N>', 'N2_ULI': '<N2_ULI, N, 1, 0, None, None, N>', 'FIS_ULI': '<FIS_ULI, L, 1, 0, None, None, L>', 'REK_ULI': '<REK_ULI, L, 1, 0, None, None, L>', 'STO_ULI': '<STO_ULI, L, 1, 0, None, None, L>', 'FRA_ULI': '<FRA_ULI, C, 6, 0, True, 0, I>', 'FRR_ULI': '<FRR_ULI, C, 6, 0, True, 0, I>', 'MJE_ULI': '<MJE_ULI, C, 2, 0, None, None, X>', 'PAS_ULI': '<PAS_ULI, C, 10, 0, None, None, X>', 'DAS_ULI': '<DAS_ULI, D, 8, 0, None, None, D>', 'MTR_ULI': '<MTR_ULI, C, 7, 0, None, None, X>' }
        self.assertEqual(len(self._adapter.header_fields), len(target))
        for k, v in self._adapter.header_fields.items():
            self.assertEqual(str(v), target[k])
        self.assertEqual(os.path.isfile(self._adapter._cache_path), True)

    def test_flush_headers(self):
        self._set_up()
        self.assertEqual(os.path.isfile(self._adapter._cache_path), True)
        self._adapter._flush_headers()
        self.assertEqual(self._adapter.header_fields, None)
        self.assertEqual(os.path.isfile(self._adapter._cache_path), False)

    def test_header_parsed_adob(self):
        """tests if the headers gets parsed and if it gets parsed right"""
        self._set_up('ADOB.DBF', with_headers=False)
        self._adapter._parse_headers(flush=True)
        target = { 'DOK_ADO': '<DOK_ADO, C, 2, 0, None, None, X>', 'PAR_ADO': '<PAR_ADO, C, 7, 0, None, None, X>', 'DAT_ADO': '<DAT_ADO, D, 8, 0, None, None, D>', 'OBJ_ADO': '<OBJ_ADO, C, 3, 0, None, None, X>', 'ULI_ADO': '<ULI_ADO, C, 4, 0, None, None, X>', 'VAL_ADO': '<VAL_ADO, D, 8, 0, None, None, D>', 'OPI_ADO': '<OPI_ADO, C, 30, 0, None, None, X>', 'IZN_ADO': '<IZN_ADO, N, 15, 2, None, None, F>', 'DUP_ADO': '<DUP_ADO, C, 1, 0, None, None, X>' }
        for k, v in self._adapter.header_fields.items():
            self.assertEqual(str(v), target[k])

    def test_is_field_type(self):
        """tests if field type is determined correctly"""
        self._set_up()
        char_field = Field('SIF_ULI', *self._adapter._table.field_info('SIF_ULI')[:3])
        date_field = Field('DAT_ULI', *self._adapter._table.field_info('DAT_ULI')[:3])
        self.assertEqual(date_field.is_type(Type.CHAR), False)
        self.assertEqual(date_field.is_type(Type.DATE), True)
        self.assertEqual(char_field.is_type(Type.CHAR), True)
        self.assertEqual(char_field.is_type(Type.DATE), False)

    def test_determine_char_column_is_int(self):
        """tests determining if the char column is an integer value"""
        self._set_up()
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
        self._set_up()
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

    def test_appending_mixed_types_record(self):
        """test appending a mismatched types record to the table"""
        self._set_up('ULIZ.DBF')
        # @TODO: testirati dodavanje afrikata i coded karaktera
        sample_record = {'OBJ_ULI': 10, 'DOK_ULI': 20, 'SIF_ULI': 0, 'GOT_ULI': None, 'NAL_ULI': 'ADM', 'DAT_ULI': '2021-07-07', 'OTP_ULI': '225883', 'NAO_ULI': None, 'DAI_ULI': '2021-06-14', 'MIS_ULI': None, 'VAL_ULI': datetime(2021, 6, 14), 'DAN_ULI': 0, 'RBR_ULI': 2, 'KUF_ULI': '1234', 'ZAD_ULI': '001', 'PAR_ULI': '0196552', 'PRO_ULI': None, 'TRG_ULI': None, 'KAS_ULI': 0, 'PUT_ULI': '001', 'LIK_ULI': None, 'FIN_ULI': None, 'L0_ULI': False, 'L1_ULI': False, 'L2_ULI': False, 'L3_ULI': False, 'L4_ULI': False, 'L5_ULI': False, 'L6_ULI': False, 'L7_ULI': False, 'L8_ULI': False, 'L9_ULI': False, 'L1A_ULI': None, 'L2A_ULI': None, 'L3A_ULI': None, 'L4A_ULI': None, 'L5A_ULI': None, 'N1_ULI': 0, 'N2_ULI': 0, 'FIS_ULI': None, 'REK_ULI': None, 'STO_ULI': None, 'FRA_ULI': None, 'FRR_ULI': None, 'MJE_ULI': None, 'PAS_ULI': None, 'DAS_ULI': None, 'MTR_ULI': None}
        result = self._adapter._read([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ])
        fresh_row_id = str(max([int(record['SIF_ULI']) for record in result]) + 1).zfill(5)
        sample_record['SIF_ULI'] = int(fresh_row_id)
        self._adapter.write(sample_record)
        result = self._adapter.read([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ])
        self.assertEqual(result[-1]['SIF_ULI'], int(fresh_row_id))
        # NOTE: Ensures indexes get updated as well
        clock = str(os.path.getmtime(self._db_path + 'ULIZ.DBF'))[:9]
        self.assertEqual(str(os.path.getmtime(self._db_path + 'ULIZ01.NTX'))[:9], clock)
        self.assertEqual(str(os.path.getmtime(self._db_path + 'ULIZ02.NTX'))[:9], clock)
        self.assertEqual(str(os.path.getmtime(self._db_path + 'ULIZ02.NTX'))[:9], clock)

    def test_updating_single_record(self):
        """tests updating a single existing record"""
        self._set_up('ULIZ.DBF', 'W')
        today = Field.dtoiso(date.today())
        randval = str(secrets.randbelow(10**6))
        success = self._adapter.update(
            { 'DAT_ULI': today, 'KUF_ULI': randval},
            [('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ('SIF_ULI', 'eq', 915),
            ])
        self.assertEqual(success, True)
        outcome = self._adapter.filter([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ('SIF_ULI', 'eq', 915),
            ])
        self.assertNotEqual(outcome, [])
        self.assertEqual(outcome[-1]['DAT_ULI'], today)
        self.assertEqual(outcome[-1]['KUF_ULI'], randval)

    def test_dtoiso(self):
        self.assertEqual(Field.dtoiso('20020812'), '2002-08-12')
        self.assertEqual(Field.dtoiso(date(2002, 8, 12)), '2002-08-12')

    def test_isotod(self):
        self.assertEqual(Field.isotod('2002-08-12'), '20020812')
        self.assertEqual(Field.isotod(date(2002, 8, 12)), '20020812')
        self.assertEqual(Field.isotod(''), '')
        self.assertEqual(Field.isotod(None), '')

    def test_updating_multiple_records(self):
        """tests updating multiple records by a certain criteria"""
        self._set_up('ULIZ.DBF')
        randval = str(secrets.randbelow(10**6))
        success = self._adapter.update(
            { 'KUF_ULI': randval},
            [('OBJ_ULI', 'eq', '010'),
            ('DOK_ULI', 'eq', '20'),
            ('SIF_ULI', 'gte', '800'),
            ('SIF_ULI', 'lt', '810'),
            ])
        self.assertNotEqual(success, None)
        self.assertEqual(success > 0, True)
        outcome = self._adapter.filter([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ('SIF_ULI', 'gte', 800),
            ('SIF_ULI', 'lt', 810),
            ])
        self.assertNotEqual(outcome, [])
        for record in outcome:
            self.assertEqual(record['KUF_ULI'], randval)
        # CASE: no object exists for filter
        success = self._adapter.update(
            { 'KUF_ULI': 12345},
            [('OBJ_ULI', 'eq', '32123'),
            ])
        self.assertEqual(success, 0)

    def test_is_char_column_string(self):
        """tests determining whether a char column is a string column"""
        self._set_up()
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
        self._set_up()
        result = self._adapter._read([('SIF_ULI', 'eq', 911)], raw_result=True)
        self.assertNotEqual(result, [])
        self.assertEqual(result[0]['SIF_ULI'], '00911')

    def test_read_all(self):
        """tests if all the values get read"""
        self._set_up()
        self.assertGreater(len(self._adapter.read_all()), 1000)

    def test_filter(self):
        """tests filtering the read values"""
        self._set_up()
        outcome = self._adapter.filter([])
        self.assertGreater(len(outcome), 0)
        outcome = self._adapter.filter(
            [('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ('SIF_ULI', 'eq', 915)])
        self.assertEqual(len(outcome), 1)
        self.assertEqual(outcome[0]['OBJ_ULI'], 10)
        self.assertEqual(outcome[0]['DOK_ULI'], 20)
        self.assertEqual(outcome[0]['SIF_ULI'], 915)

    def test_field_to_column_type_conversion(self):
        """tests if ftype values get converted to ctype correctly"""
        self._set_up()
        mock_field = Field('MIS_ULI', *self._adapter._table.field_info('MIS_ULI')[:3], None, None, Type.UNDEFINED)
        self.assertRaises(ValueError, lambda: mock_field.ftoc('asdf'))
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
        mock_field = self._adapter.header_fields['DAT_ULI']
        outcome = mock_field.ftoc('20210709')
        self.assertEqual(outcome, '2021-07-09')
        mock_field = self._adapter.header_fields['L9_ULI']
        outcome = mock_field.ftoc('F')
        self.assertEqual(outcome, False)
        outcome = mock_field.ftoc('T')
        self.assertEqual(outcome, True)
        mock_field = self._adapter.header_fields['FIN_ULI']
        outcome = mock_field.ftoc(True)
        self.assertEqual(outcome, True)
        outcome = mock_field.ftoc(False)
        self.assertEqual(outcome, False)

    def test_is_char_string_padded(self):
        """test whether padding is determined correctly"""
        self.assertRaises(ValueError, lambda: RFKAdapter._is_char_padded_string(225, 20))
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
        self._set_up()
        mock_field = Field('OTP_ULI', *self._adapter._table.field_info('OTP_ULI')[:3])
        self.assertEqual(self._adapter._is_char_column_padded_string(mock_field), (False, None))
        mock_field = Field('KUF_ULI', *self._adapter._table.field_info('KUF_ULI')[:3])
        self.assertEqual(self._adapter._is_char_column_padded_string(mock_field), (True, 'L'))
        mock_field = self._adapter.header_fields['NAO_ULI']
        self.assertEqual(self._adapter._is_char_column_padded_string(mock_field), (None, None))
        mock_field = self._adapter.header_fields['NAL_ULI']
        self.assertEqual(self._adapter._is_char_column_padded_string(mock_field), (False, None))

    def test_padding(self):
        mock_field = Field('MOCK_ULI', Type.CHAR, 10, 0, 'L', ' ', Type.CHAR)
        self.assertEqual(mock_field._pad('ASDF'), '      ASDF')
        self.assertEqual(mock_field._pad('256', 8), '     256')
        self.assertEqual(mock_field._pad(' 256 ', 8), '    256 ')
        self.assertEqual(mock_field._pad('256', 8, ' ', 'L'), '     256')
        self.assertEqual(mock_field._pad('', 8), '        ')
        self.assertEqual(mock_field._pad('225', 6, ' ', 'R'), '225   ')
        self.assertEqual(mock_field._pad('225', 6, '0'), '000225')
        self.assertEqual(mock_field._pad('225', 6, '0', True), '000225')

    def test_column_to_field_type_conversion(self):
        """tests if ctype values get converted to native padded ftype correctly"""
        self._set_up()
        mock_field = self._adapter.header_fields['SIF_ULI']
        outcome = mock_field.ctof(123)
        self.assertEqual(outcome, '00123')
        mock_field = self._adapter.header_fields['SIF_ULI']
        outcome = mock_field.ctof('123')
        self.assertEqual(outcome, '00123')
        mock_field = self._adapter.header_fields['OTP_ULI']
        outcome = mock_field.ctof('123')
        self.assertEqual(outcome, '123')
        outcome = mock_field.ctof(123)
        self.assertEqual(outcome, '123')
        outcome = mock_field.ctof('Škafiškafnjak')
        self.assertEqual(outcome, 'Škafiškafnjak')
        mock_field = self._adapter.header_fields['DAT_ULI']
        outcome = mock_field.ctof('2021-07-09')
        self.assertEqual(outcome, '2021-07-09')
        outcome = mock_field.ctof('2021-07-09')
        self.assertEqual(outcome, '2021-07-09')
        outcome = mock_field.ctof('')
        self.assertEqual(outcome, '')
        mock_field = self._adapter.header_fields['MIS_ULI']
        outcome = mock_field.ctof(None)
        self.assertEqual(outcome, None)
        mock_field = self._adapter.header_fields['KUF_ULI']
        outcome = mock_field.ctof('1234')
        self.assertEqual(outcome, '      1234')
        mock_field = self._adapter.header_fields['RBR_ULI']
        outcome = mock_field.ctof('1234')
        self.assertEqual(outcome, 1234)
        mock_field = self._adapter.header_fields['FIN_ULI']
        outcome = mock_field.ctof('T')
        self.assertEqual(outcome, True)
        mock_field = self._adapter.header_fields['FIN_ULI']
        outcome = mock_field.ctof('F')
        self.assertEqual(outcome, False)
        mock_field = self._adapter.header_fields['FIN_ULI']
        outcome = mock_field.ctof(True)
        self.assertEqual(outcome, True)
        outcome = mock_field.ctof(False)
        self.assertEqual(outcome, False)

    def test_convert_conditions(self):
        """tests if convenience style where conditions get translated correctly"""
        self._set_up()
        outcome = self._adapter._convert_conditions([('SIF_ULI', 'eq', '123')])
        outcome = outcome[0]
        self.assertEqual(outcome['column_name'], 'SIF_ULI')
        self.assertEqual(outcome['comparator'], 'eq')
        self.assertEqual(outcome['value'], '00123')
        self.assertRaises(ValueError, lambda: self._adapter._convert_conditions([('SIF_ULI', 'x', '123')]))
        self.assertRaises(ValueError, lambda: self._adapter._convert_conditions([('SIF_ULI', 'si', '123')]))
        self.assertRaises(ValueError, lambda: self._adapter._convert_conditions([('SIF_ULI', 's', '123')]))
        self.assertRaises(ValueError, lambda: self._adapter._convert_conditions([('SIF_ULI', 're', '123')]))
        outcome = self._adapter._convert_conditions([('DAT_ULI', 'eq', '2021-07-09')])
        outcome = outcome[0]
        self.assertEqual(outcome['column_name'], 'DAT_ULI')
        self.assertEqual(outcome['comparator'], 'eq')
        self.assertEqual(outcome['value'], '2021-07-09')

    def test_field_strtoc(self):
        """tests if string values get converted to column typed value correctly"""
        self._set_up()
        mock_field = self._adapter.header_fields['SIF_ULI']
        outcome = mock_field.strtoc('123')
        self.assertEqual(outcome, 123)
        mock_field = self._adapter.header_fields['DAT_ULI']
        outcome = mock_field.strtoc('2021-07-09')
        self.assertEqual(outcome, '2021-07-09')
        mock_field = self._adapter.header_fields['MIS_ULI']
        outcome = mock_field.strtoc(None)
        self.assertEqual(outcome, None)
        mock_field = self._adapter.header_fields['MIS_ULI']
        outcome = mock_field.strtoc('')
        self.assertEqual(outcome, None)
        mock_field = self._adapter.header_fields['KUF_ULI']
        outcome = mock_field.strtoc('1234')
        self.assertEqual(outcome, '1234')
        mock_field = self._adapter.header_fields['KAS_ULI']
        outcome = mock_field.strtoc('2.2')
        self.assertEqual(outcome, 2.2)

    def test_where(self):
        """test where convenience method"""
        self._set_up()
        outcome = self._adapter.where([('OTP_ULI', 'eq', '880')])
        self.assertNotEqual(outcome, [])
        self.assertEqual(outcome[0]['SIF_ULI'], 881)
        self.assertEqual(outcome[0]['OTP_ULI'], '880')
        outcome = self._adapter.where([
            ('OBJ_ULI', 'eq', 10),
            ('DOK_ULI', 'eq', 20),
            ])
        self.assertGreater(len(outcome), 0)
        for record in outcome:
            self.assertEqual(record['OBJ_ULI'], 10)
            self.assertEqual(record['DOK_ULI'], 20)

    def test_json_caching_and_restoring_parsed_headers(self):
        """tests whether header Field objects get cached and restored correctly"""
        self._set_up(with_headers=False)
        self._adapter._parse_headers(flush=True)
        self.assertEqual(os.path.isfile(self._adapter._cache_path), True)
        self._adapter.header_fields = None
        self._adapter._restore_headers()
        with open(self._adapter._cache_path, 'r') as fp:
            headers = json.load(fp)
            for field_name, field_value in headers.items():
                self.assertEqual(str(Field(**field_value)), str(self._adapter.header_fields[field_name]))

    def test_column_to_export_conversion(self):
        """tests if ctype values get converted to export format correctly"""
        self._set_up()
        mock_field = self._adapter.header_fields['L2_ULI']
        outcome = mock_field.ctox(True)
        self.assertEqual(outcome, 'T')
        outcome = mock_field.ctox(False)
        self.assertEqual(outcome, 'F')
        mock_field = self._adapter.header_fields['OBJ_ULI']
        outcome = mock_field.ctox(10)
        self.assertEqual(outcome, '"010"')
        mock_field = self._adapter.header_fields['DOK_ULI']
        outcome = mock_field.ctox(20)
        self.assertEqual(outcome, '"20"')
        mock_field = self._adapter.header_fields['SIF_ULI']
        outcome = mock_field.ctox(123)
        self.assertEqual(outcome, '"00123"')
        mock_field = self._adapter.header_fields['NAL_ULI']
        outcome = mock_field.ctox('ADM')
        self.assertEqual(outcome, '"ADM"')
        mock_field = self._adapter.header_fields['OTP_ULI']
        outcome = mock_field.ctox('225883')
        self.assertEqual(outcome, '"225883"')
        mock_field = self._adapter.header_fields['KUF_ULI']
        outcome = mock_field.ctox('1234')
        self.assertEqual(outcome, '"      1234"')
        mock_field = self._adapter.header_fields['DAT_ULI']
        outcome = mock_field.ctox('2021-07-09')
        self.assertEqual(outcome, '20210709')
        mock_field = self._adapter.header_fields['VAL_ULI']
        outcome = mock_field.ctox('2021-07-09')
        self.assertEqual(outcome, '20210709')
        mock_field = self._adapter.header_fields['MIS_ULI']
        outcome = mock_field.ctox(None)
        self.assertEqual(outcome, '""')

    def test_reindex(self):
        self._set_up()
        self.assertEqual(self._adapter.reindex(), True)

if __name__ == '__main__':
    unittest.main(failfast=True)