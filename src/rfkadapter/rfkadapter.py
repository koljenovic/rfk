#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
=========
Copyright
=========

    - Copyright: 2021 MEKOM d.o.o. Visoko -- All rights reserved.
    - Author: Malik Koljenović
    - Contact: malik@mekom.ba

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name of MEKOM d.o.o. Visoko nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED ''AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import json
import os
import re
import tempfile
import subprocess 
import csv

from collections import defaultdict
from datetime import date, datetime

class FieldError(Exception):
    pass

class HarbourError(Exception):
    pass

class FileError(Exception):
    pass

class Type:
    NULL = ord('?') # no type inferrence has been attempted
    UNDEFINED = ord('X') # type inferrence failed
    CHAR = ord('C')
    CURRENCY = ord('Y')
    DATE = ord('D')
    DATETIME = ord('T')
    DOUBLE = ord('B')
    FLOAT = ord('F')
    GENERAL = ord('G')
    INTEGER = ord('I')
    LOGICAL = ord('L')
    MEMO = ord('M')
    NUMERIC = ord('N')
    PICTURE = ord('P')
    TIMESTAMP = ord('@')

_FNAME, _COMP, _VALUE = 0, 1, 2

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

    @staticmethod
    def dtoiso(value):
        if isinstance(value, date) or isinstance(value, datetime):
            return value.isoformat()
        else:
            return date(int(value[:4]), int(value[4:6]), int(value[6:8])).isoformat()

    @staticmethod
    def isotod(value):
        if isinstance(value, date) or isinstance(value, datetime):
            return value.strftime('%Y%m%d')
        else:
            return date.fromisoformat(value).strftime('%Y%m%d')

    def _pad(self, value, length=None, pad=None, side=None):
        length = self.length if not length else length
        pad = self.pad if not pad else pad
        side = self.is_padded if not side else side
        if len(value) >= length:
            return value
        if side == 'L' or side == True:
            return pad * (length - len(value)) + value
        elif side == 'R':
            return value + pad * (length - len(value))
        raise Exception('Undefined padding side %s.' % side)

    def strtoc(self, value):
        """best effort converts str values into ctype"""
        if self.ctype == Type.INTEGER:
            return int(value) if value else None
        if self.ctype in [Type.FLOAT, Type.CURRENCY, Type.DOUBLE, Type.NUMERIC]:
            return float(value) if value else None
        return value if value else None

    def ftoc(self, value):
        """converts ftype values into ctype if possible"""
        if self.ctype == Type.NULL or self.ctype == Type.UNDEFINED:
            raise ValueError('missing inferred type for Field %s' % self.name)
        if self.ctype == Type.LOGICAL:
            if isinstance(value, str):
                return True if value == 'T' else False
            return value
        if self.ctype == Type.INTEGER:
            return int(value) if value else None
        if self.ctype == Type.CHAR:
            return value
        if self.ctype == Type.FLOAT:
            return value
        if self.ctype == Type.DATE:
            return Field.dtoiso(value) if value and isinstance(value, str) and len(value) == 8 else value
        if self.ftype == self.ctype:
            return value
        raise ValueError('undefined conversion method from ftype %s to ctype %s' % (chr(self.ftype), chr(self.ctype)))

    def ctof(self, value):
        """converst ctype values back into native ftype with padding and all"""
        if value != None:
            if self.ftype == Type.INTEGER:
                return int(value)
            if self.ftype == Type.NUMERIC:
                return float(value)
            if self.ftype == Type.CHAR:
                if self.is_padded:
                    return self._pad(str(value))
                else:
                    return str(value)
            if self.ftype == Type.DATE:
                if isinstance(value, str):
                    if len(value) == 8:
                        return Field.dtoiso(value)
                    if len(value) == 10:
                        return value
                raise ValueError('Incorrect date value: %s' % value)
            if self.ftype == Type.LOGICAL:
                if isinstance(value, str):
                    return True if value == 'T' else False
        return value

    @staticmethod
    def quote(value, char='"'):
        return char + value.replace(char, f'\\{char}') + char

    def ctox(self, value):
        """converst ctype values into export csv compatible values"""
        if self.ftype == Type.LOGICAL:
            return 'T' if value else 'F'
        if self.ftype == Type.CHAR:
            if self.ctype == Type.INTEGER:
                if isinstance(value, int):
                    if self.is_padded and self.is_padded != 'R':
                        return Field.quote(self._pad(str(value)))
                if value != None:
                    return Field.quote(str(value))
            if self.ctype == Type.CHAR:
                if isinstance(value, str):
                    if self.is_padded and self.is_padded != 'R':
                        return Field.quote(self._pad(value))
                    else:
                        return Field.quote(value)
        if self.ftype == Type.DATE:
            if isinstance(value, str):
                return Field.isotod(value)
            if isinstance(value, date) or isinstance(value, datetime):
                return Field.isotod(value)
        try:
            return str(value) if value != None else Field.quote('')
        except:
            return Field.quote('')

class MetaRecord:
    def __init__(self, header_fields, record):
        self._header_fields = header_fields
        self._record = record

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in self._header_fields:
                return self._record[self._header_fields.index(key)]
            else:
                raise IndexError
        elif isinstance(key, int):
            if key < len(self._header_fields):
                return self._record[key]
            else:
                raise IndexError
        raise TypeError

class DBFIterator:
    def __init__(self, records, header_fields):
        self._records = records
        self._header_fields = header_fields

    def __iter__(self):
        return self

    def __next__(self):
        if len(self._records) > 0:
            return MetaRecord(self._header_fields, self._records.pop(0))
        else:
            raise StopIteration

_EXE = 'dbfadapter'

class DBFAdapter:
    def __init__(self, db_path, table_name, mode='-', index_suffix='ntx'):
        if os.path.isfile(db_path + table_name):
            self._table = self
            self.db_path = db_path
            self.table_name = table_name
            self.index_suffix = index_suffix
            self.index_files = self._locate_index_files()
            self._records = None
            self._meta = None
            self._filter = []
        else:
            raise FileError('No such database exists.')

    @staticmethod
    def _head(db_path, table_name, _EXE=_EXE):
        table_name = table_name.split('.')[0]
        headers, f, fd, fname = None, None, None, None
        try:
            fd, fname = tempfile.mkstemp(prefix='dbfh', suffix='.json', text=True)
            ext = subprocess.run([_EXE, "head", db_path, table_name, fname, '//noalert'], timeout=10, text=True, capture_output=True)
            if ext.returncode != 0:
                raise HarbourError(ext.stderr)
            f = os.fdopen(fd, 'r')
            headers = f.read()
        finally:
            if f:
                f.close()
            if fname:
                os.remove(fname)
        return headers

    @staticmethod
    def _export(db_path, table_name, index_files, where=[], _EXE=_EXE):
        table_name = table_name.split('.')[0]
        records, f, fd, fname = [], None, None, None
        try:
            fd, fname = tempfile.mkstemp(prefix='dbfx', suffix='.cson', text=True)
            with open(fname, 'w') as f:
                json.dump(where, f)
            ext = subprocess.run([_EXE, "export", db_path, table_name, fname, *index_files, '//noalert'], timeout=10, text=True, capture_output=True)
            if ext.returncode != 0:
                raise HarbourError(ext.stderr)
            with open(fname, 'r') as f:
                csvf = csv.reader(f)
                records = [r for r in csvf]
        finally:
            if f:
                f.close()
            if fname:
                os.remove(fname)
        return records[:-1]

    @staticmethod
    def _append(line, db_path, table_name, index_files, _EXE=_EXE):
        table_name = table_name.split('.')[0]
        f, fd, fname = None, None, None
        try:
            fd, fname = tempfile.mkstemp(prefix='dbfa', suffix='.csv', text=True)
            f = os.fdopen(fd, 'w')
            f.write(','.join(line))
            f.close()
            ext = subprocess.run([_EXE, "append", db_path, table_name, fname, *index_files, '//noalert'], timeout=10, text=True, capture_output=True)
            if ext.returncode != 0:
                raise HarbourError(ext.stderr)
        finally:
            if f:
                f.close()
            if fname:
                os.remove(fname)
        return True

    @staticmethod
    def _update(update_package, db_path, table_name, index_files, _EXE=_EXE):
        table_name = table_name.split('.')[0]
        f, fd, fname, updated_count = None, None, None, None
        try:
            fd, fname = tempfile.mkstemp(prefix='dbfu', suffix='.json', text=True)
            f = os.fdopen(fd, 'w')
            data = json.dump(update_package, f)
            f.close()
            ext = subprocess.run([_EXE, "update", db_path, table_name, fname, *index_files, '//noalert'], timeout=10, text=True, capture_output=True)
            updated_count = ext.stdout.split('\n')
            updated_count = updated_count[0].split(':') if len(updated_count) > 0 else None
            updated_count = updated_count[1] if len(updated_count) > 0 else None
            if ext.returncode != 0:
                raise HarbourError(ext.stderr)
        finally:
            if f:
                f.close()
            if fname:
                os.remove(fname)
        return int(updated_count)

    def _locate_index_files(self):
        """Finds index files for db if they exist, **VERY MUCH** case sensitive"""
        indices = []
        with os.scandir(self.db_path) as d:
            for e in d:
                if e.is_file():
                    if e.name.startswith(self.table_name.split('.')[0]):
                        if e.name.lower().endswith('.' + self.index_suffix.lower()):
                            indices.append(e.name.split('.')[0])
        return indices

    def _parse_meta(self):
        header = json.loads(DBFAdapter._head(self.db_path, self.table_name))
        header = [x[1:-1].split(',') for x in header]
        header = [[y.strip() for y in x] for x in header]
        self._meta = { x[0]: (x[0], ord(x[1]), int(x[2]), int(x[3])) for x in header }

    def __iter__(self):
        if not self._meta:
            self._parse_meta()
        records = DBFAdapter._export(self.db_path, self.table_name, self.index_files, self._filter)
        return DBFIterator(records, [k for k, v in self._meta.items() if v[1] != Type.MEMO] )

    def field_info(self, field_name):
        return self._meta[field_name][1:] if self._meta else None

# @TODO-EP-002: Determine mandatory header fields
class RFKAdapter(DBFAdapter):
    def __init__(self, db_path, table_name, mode='-', index_suffix='ntx', with_headers=True):
        super(RFKAdapter, self).__init__(db_path, table_name, mode, index_suffix)
        self._table = self
        base_name = os.path.splitext(self.table_name)[0]
        self._cache_path = os.path.join(self.db_path, base_name + '.json')
        if with_headers:
            self._parse_headers()

    @staticmethod
    def _prepare_value(value):
        """Converts encoding and strips whitespace"""
        e = value.decode('cp852') if isinstance(value, bytes) else value
        return e.strip() if isinstance(e, str) else e

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
        if not type(value) in [str, bytes]:
            raise ValueError('%s is not CHAR type' % value)
        value = value.decode('cp852') if isinstance(value, bytes) else value
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
        value = value.decode('cp852') if isinstance(value, bytes) else value
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

    def _parse_headers(self, flush=False):
        """Parses the fields from table headers"""
        if flush and os.path.isfile(self._cache_path):
            self._flush_headers()
        self._parse_meta()
        self.header_fields = { _name: Field(_name, _prop[1], _prop[2], _prop[3]) for _name, _prop in self._table._meta.items() if _prop[1] != Type.MEMO }
        if os.path.isfile(self._cache_path):
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
            self._cache_headers()

    def _read(self, where=[], raw_result=False, infer_type=False):
        """Read, fetch and filter from RFK table

        - `raw_result` flag will force returning the results as read from the DBF
        without any further prep.
        - `infer_type` flag forces using inferred types everywhere
        """
        result = []
        fields = [k for k, v in self._meta.items() if v[1] != Type.MEMO]

        self._filter = self._convert_conditions(where)

        for record in self._table:
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

    def read(self, where=[]):
        """Opinionated type inferred read"""
        return self._read(where=where, infer_type=True)

    def read_all(self):
        """Returns all the DBF values, type inferred"""
        return self._read(infer_type=True)

    def filter(self, where=[]):
        """Returns all filtered DBF values, best effort type inferred

        where e.g. [('OBJ_ULI', lambda x: x == '010')]
        """
        return self._read(where, infer_type=True)

    def _convert_conditions(self, conditions):
        """Converts between convenience and JSON filter condition styles

        < (lt), > (gt), <= (lte), >= (gte), == (eq), != (neq)
        si - parcijalno uparivanje stringova bez obzira na mala i velika slova
        s - parcijalno uparivanje stringova uz razlikovanje malih i velikih slova
        x - uparivanje stringova korištenjem regularnih izraza (regex)
        """
        _where = []
        _comparators = ['lt', 'gt', 'lte', 'gte', 'eq', 'neq', 'si', 's', 'x']
        for condition in conditions:
            if len(condition) != 3:
                raise TypeError('Invalid filter. (NOTE: Old style lambda filters are deprecated, use convenience filters)')
            if condition[_FNAME] not in self.header_fields.keys():
                raise FieldError('No field with name %s in table %s' % (condition[_FNAME], self.table_name))
            if condition[_COMP] in ['si', 's', 'x'] and self.header_fields[condition[_FNAME]].ctype != Type.CHAR:
                raise ValueError('invalid comparator for non string column type')
            if condition[_COMP] in _comparators:
                _where.append({
                    'column_name': condition[_FNAME],
                    'comparator': condition[_COMP],
                    'value': self.header_fields[condition[_FNAME]].ctof(condition[_VALUE])})
            else:
                raise ValueError('Invalid filter condition.')
        return _where

    def where(self, where=[]):
        """Convenience filter method

        Written in order to facilitate external calls with conditions
        styled like: [('COL', 'gt', 'VALUE'), ('COL', 'lt', 'VALUE')]
        conditions get type inferred
        """
        return self.filter(where)

    def write(self, data):
        """Appends a new record to the table"""
        line = []
        for _, field in self.header_fields.items():
            if field.name in data:
                if field.ftype != Type.MEMO:
                    line.append(field.ctox(data[field.name]))
            else:
                line.append(field.ctox(None))
        self._append(line, self.db_path, self.table_name, self.index_files)

    def update(self, what, where):
        """Updates existing records, returns True on success"""
        _what = {}
        _dict_where = self._convert_conditions(where)
        if not self.header_fields:
            raise ValueError('header_fields not initialized, run self._parse_headers first!')
        for field_name, new_value in what.items():
            if field_name not in self.header_fields.keys():
                raise FieldError('No field with name %s in table %s to update' % (field_name, self.table_name))
            _what[field_name] = self.header_fields[field_name].ctof(new_value)
        return DBFAdapter._update({'what': _what, 'where': _dict_where}, self.db_path, self.table_name, self.index_files)

    def _cache_headers(self):
        """Caches parsed headers to file because parsing is time demanding

        @TODO-EP-001: update cache on structural changes, ATM this is unneccessary,
        flush manually by deleting *.json or calling _parse_headers with flush_cache
        """
        with open(self._cache_path, 'w') as fp:
            headers = { field.name: field.__dict__ for field in self.header_fields.values() }
            json.dump(headers, fp)

    def _flush_headers(self):
        self.header_fields = None
        if os.path.isfile(self._cache_path):
            os.remove(self._cache_path)

    def _restore_headers(self):
        """Restores header fields from cache"""
        with open(self._cache_path, 'r') as fp:
            headers = json.load(fp)
            self.header_fields = {}
            for field_name, field in headers.items():
                self.header_fields[field_name] = Field(**field)