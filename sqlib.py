#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    - TODO   : TODO something
    - User   : keming.cao
    - Date   : 2014-02-02
    - Time   : 11:18:32
    - Contact: keming.cao@gmail.com
'''
import sqlite3 as sqlite


class Connection(object):

    """
        A lightweight wrapper for sqlite3 connections.
    """

    def __init__(self, database=":memory:", isolation_level=None):
        self._isolation_level = isolation_level  # None = autocommit
        self._database = database
        self._db = None
        try:
            self.reconnect()
        except:
            raise sqlite.OperationalError

    def executescript(self, query):
        cursor = self._cursor()
        try:
            cursor.executescript(query)
            return True
        except Exception as e:
            print e
            return False
        finally:
            cursor.close()

    def execute_rowcount(self, query, *args, **kwargs):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, args, kwargs)
            return cursor.rowcount
        finally:
            cursor.close()

    def execute(self, query, *args, **kwargs):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, args, kwargs)
            return True
        except:
            return False
        finally:
            cursor.close()

    def execute_lastrowid(self, query, *args, **kwargs):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, args, kwargs)
            return cursor.lastrowid
        finally:
            cursor.close()

    def get(self, query, *args, **kwargs):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, args, kwargs)
            column_names = [d[0] for d in cursor.description]
            row = cursor.fetchone()
            if row:
                return Row(zip(column_names, row))
            else:
                return None
        finally:
            cursor.close()

    def query(self, query, *args, **kwargs):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, args, kwargs)
            column_names = [d[0] for d in cursor.description]

            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def iter(self, query, *args, **kwargs):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, args, kwargs)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def _cursor(self):
        return self._db.cursor()

    def reconnect(self):
        self.close()
        self._db = sqlite.connect(self._database)
        self._db.isolation_level = self._isolation_level

        return self._db

    def _execute(self, cursor, query, args, kwargs):
        try:
            return cursor.execute(query, args or kwargs)
        except Exception as e:
            self.close()
            raise e


class Row(dict):

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
