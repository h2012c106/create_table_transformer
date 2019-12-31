# -*- coding: UTF-8 -*-
import transformer
import argparse
import reader
import codecs

HOST_PARAM_STR = 'host'
PORT_PARAM_STR = 'port'
USER_PARAM_STR = 'user'
PWD_PARAM_STR = 'pwd'
TABLES_PARAM_STR = 'tables'
CONFIG_PARAM_STR = 'config'
DB_PARAM_STR = 'database'
OUTPUT_PARAM_STR = 'output'


def get_param_dict():
    parser = argparse.ArgumentParser()
    parser.add_argument('--' + HOST_PARAM_STR, type=str)
    parser.add_argument('--' + PORT_PARAM_STR, type=str)
    parser.add_argument('--' + USER_PARAM_STR, type=str)
    parser.add_argument('--' + PWD_PARAM_STR, type=str)
    parser.add_argument('--' + TABLES_PARAM_STR, type=str, default=None)
    parser.add_argument('--' + CONFIG_PARAM_STR, type=str)
    parser.add_argument('--' + DB_PARAM_STR, type=str)
    parser.add_argument('--' + OUTPUT_PARAM_STR, type=str)
    args = parser.parse_args()
    return args.__dict__


if __name__ == '__main__':
    param_dict = get_param_dict()
    create_table_list = reader.DbReader(param_dict).get_create_table_list()
    tran = transformer.Transformer(param_dict.get(CONFIG_PARAM_STR))
    success_output = []
    error_output = []
    error_table = []
    for sql, validate_res in tran.main(create_table_list):
        success, msg, table_name = validate_res
        if success:
            success_output.append(sql)
        else:
            error_output.append(msg)
            error_table.append(table_name)
    with codecs.open(param_dict.get(OUTPUT_PARAM_STR, None), 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(success_output))
    with codecs.open('error.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(error_output) + '\n\n' + '\n'.join(error_table))
