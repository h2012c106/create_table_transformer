import os
import codecs
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

HOST_PARAM_STR = 'host'
PORT_PARAM_STR = 'port'
USER_PARAM_STR = 'user'
PWD_PARAM_STR = 'pwd'
TABLES_PARAM_STR = 'tables'
CONFIG_PARAM_STR = 'config'
DB_PARAM_STR = 'database'


def make_engine(**kwargs):
    if 'url' in kwargs:
        return create_engine(kwargs['url'], encoding='utf-8')
    else:
        return create_engine(
            'mysql+pymysql://{user}:{pwd}@{host}:{port}/{database}?charset=utf8'.format(user=kwargs['user'],
                                                                                        pwd=kwargs['pwd'],
                                                                                        host=kwargs['host'],
                                                                                        port=kwargs['port'],
                                                                                        database=kwargs['database']),
            encoding='utf-8', convert_unicode=True)


def get_session(engine):
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    return session


class DbReader:
    def __init__(self, param_dict):
        self._engine = make_engine(**param_dict)
        table_list_file = param_dict.get(TABLES_PARAM_STR, None)
        if table_list_file is None or not os.path.exists(table_list_file):
            self._table_list = self.__get_table_list_from_db()
        else:
            with codecs.open(table_list_file, 'r', 'utf-8') as f:
                self._table_list = list(filter(lambda table_name: len(table_name) > 0,
                                               map(lambda table_name: table_name.strip(), f.readlines())))
        self._table_list = set(self._table_list)

    def __get_table_list_from_db(self):
        session = get_session(self._engine)
        sql = '''SHOW TABLES;'''
        result = session.execute(text(sql)).fetchall()
        result = [r[0] for r in result]
        session.close()
        return result

    def get_create_table_list(self):
        result = []
        session = get_session(self._engine)
        for table_name in self._table_list:
            sql = '''SHOW CREATE TABLE {table_name};'''.format(table_name=table_name)
            tmp_result = session.execute(text(sql)).fetchall()
            tmp_result = tmp_result[0][1]
            result.append(tmp_result)
        session.close()
        return result
