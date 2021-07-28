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
import mdbf as dbf
import os
import re

from collections import defaultdict
from datetime import date

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
        if self.ctype == Type.INTEGER:
            return int(value) if value else None
        if self.ctype == Type.CHAR:
            return value
        if self.ctype == Type.FLOAT:
            return value
        if self.ctype == Type.DATE:
            return value.isoformat() if value else value
        if self.ftype == self.ctype:
            return value
        raise ValueError('undefined conversion method from ftype %s to ctype %s' % (chr(self.ftype), chr(self.ctype)))

    def ctof(self, value):
        """converst ctype values back into native ftype with padding and all"""
        if self.ftype == Type.CHAR:
            if self.ctype == Type.INTEGER:
                if isinstance(value, int):
                    if self.is_padded:
                        return self._pad(str(value))
                    else:
                        return str(value)
            elif self.ctype == Type.CHAR:
                if self.is_padded and isinstance(value, str):
                    return self._pad(value)
        if self.ftype == Type.DATE:
            if isinstance(value, str):
                return date.fromisoformat(value)
        return value

# @TODO-EP-002: Determine mandatory header fields
class RFKAdapter:
    def __init__(self, db_path, table_name, mode='-'):
        self.db_path = db_path
        self.table_name = table_name
        self._table = dbf.Table(db_path + table_name, codepage='cp852') # dbf_type='db3'
        self._table.open(mode=dbf.READ_WRITE if mode.lower() == 'w' else dbf.READ_ONLY)
        self._parse_headers()

    def __del__(self):
        if hasattr(self, '_table') and self._table.status != dbf.CLOSED:
            self._table.close()

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

    def _parse_headers(self):
        """Parses the fields from table headers"""
        self.header_fields = { _name: Field(_name, _prop[0], _prop[2], _prop[4]) for _name, _prop in self._table._meta.items() }
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
                raise FieldError('No field with name %s in table %s' % (field_name, self.table_name))
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

    def _convert_condition(self, column, comparator, constraint):
        """Converts between convenience and filter condition styles

        < (lt), > (gt), <= (lte), >= (gte), == (eq), != (neq)
        si - parcijalno uparivanje stringova bez obzira na mala i velika slova
        s - parcijalno uparivanje stringova uz razlikovanje malih i velikih slova
        x - uparivanje stringova korištenjem regularnih izraza (regex)
        """
        _map = {
            'lt': lambda x: x < self.header_fields[column].strtoc(constraint),
            'gt': lambda x: x > self.header_fields[column].strtoc(constraint),
            'lte': lambda x: x <= self.header_fields[column].strtoc(constraint),
            'gte': lambda x: x >= self.header_fields[column].strtoc(constraint),
            'eq': lambda x: x == self.header_fields[column].strtoc(constraint),
            'neq': lambda x: x != self.header_fields[column].strtoc(constraint),
            'si': lambda x: x.lower().find(constraint.lower()) >= 0,
            's': lambda x: x.find(constraint) >= 0,
            'x': lambda x: re.search(constraint, x) != None,
        }
        if comparator in ['si', 's', 'x'] and self.header_fields[column].ctype != Type.CHAR:
            raise ValueError('invalid compararator for non string column type')
        if comparator in _map:
            return (column, _map[comparator])
        else:
            raise ValueError('invalid comparator %s' % comparator)

    def where(self, conditions=[]):
        """Convenience filter method

        Written in order to facilitate external calls with conditions
        styled like: [('COL', 'gt', 'VALUE'), ('COL', 'lt', 'VALUE')]
        conditions get type inferred and operators go lambdas
        """
        converted_conditions = [self._convert_condition(*c) for c in conditions]
        return self.filter(converted_conditions)

    def write(self, data):
        """Appends a new record to the table"""
        for k, v in data.items():
            data[k] = self.header_fields[k].ctof(v)
        self._table.append(data)

    def update(self, what, where):
        """Updates existing records, returns True on success"""
        _where = []
        for c in where:
            if len(c) == 2:
                _where.append(c)
            if len(c) == 3:
                _where.append(self._convert_condition(*c))
        fields = self._table._meta.fields
        for field_name, constr in _where:
            if field_name not in fields:
                raise FieldError('No field with name %s in table %s' % (field_name, self.table_name))
        for record in self._table:
            satisfies = True
            for field_name, constr in _where:
                value = RFKAdapter._prepare_value(record[field_name])
                value = self.header_fields[field_name].ftoc(value)
                if not constr(value):
                    satisfies = False
                    break
            if satisfies:
                for k, v in what.items():
                    with record:
                        record[k] = self.header_fields[k].ctof(v)
        return True

    def _cache_headers(self):
        """Caches parsed headers to file because parsing is time demanding

        @TODO-EP-001: update cache on structural changes, ATM this is unneccessary,
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