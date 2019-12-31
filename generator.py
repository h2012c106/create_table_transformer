import re
import json
import jpype
import codecs
from wrapcache import wrapcache

ADDITIONAL_ROWS_JSON_KEY = 'additional_rows'
DEPRECATED_ROWS_JSON_KEY = 'deprecated_rows'
NEW_KEYS_JSON_KEY = 'new_keys'
USE_OLD_KEYS_JSON_KEY = 'use_old_keys'
PRIMARY_TO_UNIQUE = 'primary_to_unique'
OVERWRITE_TABLE_JSON_KEY = 'overwrite_old_table'
DISABLE_AUTOINCREMENT = 'disable_autoincrement'


@wrapcache(timeout=60)
def read_config(config_file):
    with codecs.open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


class Generator:
    def __init__(self, config_file):
        assert jpype.isJVMStarted()
        self._config = read_config(config_file)
        self._row_parser_class = jpype.JClass('com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser')

    def need_overwrite_table(self):
        default_res = False
        res = self._config.get(OVERWRITE_TABLE_JSON_KEY, default_res)
        if isinstance(res, bool):
            return res
        else:
            return default_res


class RowGenerator(Generator):
    def __init__(self, config_file):
        Generator.__init__(self, config_file)

    def _get_additional_rows(self):
        rows = self._config.get(ADDITIONAL_ROWS_JSON_KEY, None)
        if rows is None or not isinstance(rows, list):
            return []
        else:
            return rows

    def _row_sql_to_jclass(self, row_sql, parent):
        row_parser = self._row_parser_class(row_sql)
        column = row_parser.parseColumn()
        column.setParent(parent)
        return column

    def get_additional_rows(self, parent):
        return [self._row_sql_to_jclass(row, parent) for row in self._get_additional_rows()]

    def _backtick_str(self, s):
        mtch = re.match('^`.*?`$', s)
        if mtch is None:
            assert '`' not in s
            s = '`{s}`'.format(s=s)
        return s

    def get_deprecated_rows(self):
        rows_name = self._config.get(DEPRECATED_ROWS_JSON_KEY, None)
        if isinstance(rows_name, str):
            res = [rows_name]
        elif rows_name is None or not isinstance(rows_name, list):
            res = []
        else:
            res = rows_name
        return set([self._backtick_str(row_name) for row_name in res])

    def disable_autoincrement(self, create, row_list):
        if self._config.get(DISABLE_AUTOINCREMENT, True):
            for col in row_list:
                col.setAutoIncrement(False)
            create.getTableOptions().remove('AUTO_INCREMENT')


class KeyGenerator(Generator):
    def __init__(self, config_file):
        Generator.__init__(self, config_file)
        self._key_class = jpype.JClass('com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey')
        self._primary_key_class = jpype.JClass('com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey')
        self._unique_key_class = jpype.JClass('com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique')

    def _get_new_keys(self):
        rows = self._config.get(NEW_KEYS_JSON_KEY, None)
        if rows is None or not isinstance(rows, list):
            return []
        else:
            return rows

    def _key_sql_to_jclass(self, key_sql, parent):
        print key_sql
        key_parser = self._row_parser_class(key_sql)
        try:
            key = key_parser.parsePrimaryKey()
            key.setParent(parent)
            return key
        except Exception:
            pass
        try:
            key = key_parser.parseUnique()
            key.setParent(parent)
            return key
        except Exception:
            pass
        # not pk, either uk, try to make it to uk then convert to key
        try:
            key_sql = 'UNIQUE {key_sql}'.format(key_sql=key_sql)
            key_parser = self._row_parser_class(key_sql)
            tmp_unique_key = key_parser.parseUnique()
            key = self._key_class()
            key.setName(tmp_unique_key.getName())
            for column in tmp_unique_key.getColumns():
                column.setParent(key)
                key.getColumns().add(column)
            key.setKeyBlockSize(tmp_unique_key.getKeyBlockSize())
            key.setIndexType(tmp_unique_key.getIndexType())
            return key
        except Exception:
            raise Exception('Unknown key type')

    def get_new_keys(self, parent):
        return [self._key_sql_to_jclass(key, parent) for key in self._get_new_keys()]

    def use_old_key(self):
        res = self._config.get(USE_OLD_KEYS_JSON_KEY, False)
        if isinstance(res, bool):
            return res
        elif isinstance(res, int) and res in (0, 1):
            return False if res == 0 else True
        else:
            return False

    def primary_to_unique(self, key_list):
        if self._config.get(PRIMARY_TO_UNIQUE, True):
            new_key_list = []
            for key in key_list:
                if isinstance(key, self._primary_key_class):
                    parent = key.getParent()
                    new_key_list.append(self._key_sql_to_jclass(key.toString().replace('PRIMARY', 'UNIQUE'), parent))
                else:
                    new_key_list.append(key)
