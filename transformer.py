# -*- coding: UTF-8 -*-

import os
import jpype
import generator
import collections


def _get_jars_path(lib_path):
    res = []
    for file_name in os.listdir(lib_path):
        if os.path.splitext(file_name)[1] == '.jar':
            res.append(lib_path + file_name)
    return [os.path.abspath(path) for path in res]


class JavaEnv:
    def __init__(self):
        self.java_lib_path = './lib/'

    def _pre_transform(self):
        jvm_path = jpype.getDefaultJVMPath()
        jars_path = _get_jars_path(self.java_lib_path)
        jvm_arg = "-Djava.class.path={jars_path}".format(jars_path=":".join(jars_path))
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
        jpype.startJVM(jvm_path, jvm_arg, convertStrings=True)

    def _set_init_class(self):
        assert False, 'No inherit!'

    def _transform(self, create_table):
        assert False, 'No inherit!'

    def _validate(self, create):
        assert False, 'No inherit!'

    def _post_transform(self):
        jpype.shutdownJVM()

    def main(self, create_table_list):
        self._pre_transform()
        self._set_init_class()
        try:
            res = []
            for create_table in create_table_list:
                create, sql = self._transform(create_table)
                res.append((sql, self._validate(create)))
            return res
        finally:
            self._post_transform()


class Transformer(JavaEnv):
    def __init__(self, config_file):
        JavaEnv.__init__(self)
        self._config_file = config_file

    def _set_init_class(self):
        self._statement_parser_class = jpype.JClass('com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser')
        self._row_class = jpype.JClass('com.alibaba.druid.sql.ast.statement.SQLColumnDefinition')
        self._key_class = jpype.JClass('com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey')

        self._row_generator = generator.RowGenerator(self._config_file)
        self._key_generator = generator.KeyGenerator(self._config_file)

    def _transform(self, create_table):
        parser = self._statement_parser_class(create_table)
        create = parser.parseCreateTable()
        create_detail_list = create.getTableElementList()

        # deal with key and row
        row_list = []
        key_list = []
        for create_detail in create_detail_list:
            if isinstance(create_detail, self._row_class) \
                    and create_detail.getNameAsString() not in self._row_generator.get_deprecated_rows():
                row_list.append(create_detail)
            elif isinstance(create_detail, self._key_class) and self._key_generator.use_old_key():
                key_list.append(create_detail)
        row_list += self._row_generator.get_additional_rows(create)
        key_list += self._key_generator.get_new_keys(create)
        create_detail_list.clear()
        for row in row_list:
            create_detail_list.add(row)
        for key in key_list:
            create_detail_list.add(key)

        # deal with auto increment
        self._row_generator.disable_autoincrement(create, row_list)
        # deal with pk
        self._key_generator.primary_to_unique(key_list)

        # deal with table drop
        # choosing either of two generator is okay
        if self._row_generator.need_overwrite_table():
            sql = u'DROP TABLE IF EXISTS {table_name};\n{create_sql};'.format(table_name=create.getName().toString(),
                                                                              create_sql=create.toString())
        else:
            # not my spell error
            create.setIfNotExiists(True)
            sql = u'{create_sql};'.format(create_sql=create.toString())

        return create, sql

    def _find_duplicate_list(self, src_list):
        counter = dict(collections.Counter(src_list))
        return [key for key, value in counter.items() if value > 1]

    def _validate_row_duplicate(self, create):
        create_detail_list = create.getTableElementList()
        row_list = [detail for detail in create_detail_list if isinstance(detail, self._row_class)]
        row_name_duplicate_list = self._find_duplicate_list([row.getNameAsString() for row in row_list])
        if len(row_name_duplicate_list) == 0:
            return True, None
        else:
            return False, 'duplicate row name: {li}'.format(li=row_name_duplicate_list)

    def _validate_key_duplicate(self, create):
        create_detail_list = create.getTableElementList()
        key_list = [detail for detail in create_detail_list if isinstance(detail, self._key_class)]
        key_name_duplicate_list = self._find_duplicate_list(
            [key.getName().toString() for key in key_list if key.getName() is not None])
        if len(key_name_duplicate_list) == 0:
            return True, None
        else:
            return False, 'duplicate key name: {li}'.format(li=key_name_duplicate_list)

    def _validate_row_in_key(self, create):
        create_detail_list = create.getTableElementList()
        row_name_set = set()
        for detail in create_detail_list:
            if isinstance(detail, self._row_class):
                row_name_set.add(detail.getNameAsString())
        err_key_dict = {}
        for detail in create_detail_list:
            if isinstance(detail, self._key_class):
                key_name = detail.getName().toString() if detail.getName() is not None else 'PRIMARY KEY'
                relative_row_set = set([row.getExpr().toString() for row in detail.getColumns()])
                additional_row_set = relative_row_set - row_name_set
                if len(additional_row_set) > 0:
                    err_key_dict[key_name] = list(additional_row_set)
        if len(err_key_dict) == 0:
            return True, None
        else:
            return False, ', '.join(
                ['key {key} contains nonexistent row: {row_list}'.format(key=key, row_list=row_list) for key, row_list
                 in
                 err_key_dict.items()])

    def _validate(self, create):
        validate_list = [self._validate_row_duplicate, self._validate_key_duplicate, self._validate_row_in_key]
        success = True
        msg = []
        for validate in validate_list:
            tmp_success, tmp_msg = validate(create)
            success &= tmp_success
            if not tmp_success:
                msg.append(tmp_msg)
        return success, '{table} error: {msg}'.format(table=create.getName().toString(),
                                                      msg=' and '.join(msg)), create.getName().toString()
